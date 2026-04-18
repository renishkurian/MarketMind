from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Body, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from typing import List, Dict, Optional, Any
import logging
import os
from datetime import datetime, date
import json
import asyncio

from backend.data.db import (
    get_db, SessionLocal,
    StockMaster, SignalsCache, AIInsights, PriceHistory, FundamentalsCache, SyncLog, PortfolioTransaction
)
from backend.utils.market_hours import get_market_status, get_current_ist_time
from backend.utils.auth import verify_password, create_access_token, get_current_admin, get_password_hash
from backend.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── WebSocket Connection Manager ──────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WS client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WS client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                dead.append(conn)
        for d in dead:
            self.disconnect(d)

manager = ConnectionManager()

# ── Broadcast hook for scheduler ─────────────────────────────────────────────
async def broadcast_price_update(symbol: str, price: float, change_pct: float):
    await manager.broadcast({
        "type": "price_update",
        "data": {"symbol": symbol, "price": price, "change_pct": change_pct}
    })

async def broadcast_market_status(status: str):
    await manager.broadcast({"type": "market_status", "data": {"status": status}})

# ── App Lifespan (startup / shutdown) ────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: attach broadcast hook and start scheduler
    from backend import scheduler as sched_module
    sched_module.broadcast_price_update = broadcast_price_update
    sched_module.broadcast_market_status = broadcast_market_status

    from backend.scheduler import start_scheduler
    scheduler = start_scheduler()
    logger.info("APScheduler started.")

    # Kick off market status broadcaster (runs every 60s)
    async def status_loop():
        while True:
            try:
                status = get_market_status()
                await broadcast_market_status(status)
            except Exception as e:
                logger.error(f"Status broadcast error: {e}")
            await asyncio.sleep(60)

    status_task = asyncio.create_task(status_loop())

    yield

    # Shutdown
    status_task.cancel()
    scheduler.shutdown(wait=False)
    logger.info("APScheduler stopped.")


app = FastAPI(title="MarketMind API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── WebSocket endpoint ────────────────────────────────────────────────────────
@app.websocket("/ws/market")
async def websocket_endpoint(websocket: WebSocket, db: AsyncSession = Depends(get_db)):
    await manager.connect(websocket)
    try:
        # Send initial snapshot
        result = await db.execute(
            select(StockMaster, SignalsCache)
            .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
            .where(StockMaster.is_active == True)
        )
        rows = result.all()

        # Fetch latest close price from price_history as fallback (for closed-market periods)
        from sqlalchemy import func as sqlfunc
        ph_sub = (
            select(
                PriceHistory.symbol,
                sqlfunc.max(PriceHistory.date).label("max_date")
            )
            .group_by(PriceHistory.symbol)
            .subquery()
        )
        ph_result = await db.execute(
            select(PriceHistory.symbol, PriceHistory.close)
            .join(ph_sub, (PriceHistory.symbol == ph_sub.c.symbol) & (PriceHistory.date == ph_sub.c.max_date))
        )
        last_close_map = {row.symbol: float(row.close) for row in ph_result.all()}

        snapshot = []
        for stock, signal in rows:
            sig_data = _serialize_signal(signal)
            # If signal has no current_price, fall back to last known close from price_history
            if sig_data and not sig_data.get("current_price"):
                sig_data["current_price"] = last_close_map.get(stock.symbol)
            elif sig_data is None:
                fallback_price = last_close_map.get(stock.symbol)
                if fallback_price:
                    sig_data = {"current_price": fallback_price}
            item = {
                "symbol": stock.symbol,
                "company_name": stock.company_name,
                "scp_name": stock.scp_name,
                "sector": stock.sector,
                "type": stock.type,
                "quantity": float(stock.quantity) if stock.quantity else None,
                "avg_buy_price": float(stock.avg_buy_price) if stock.avg_buy_price else None,
                "buy_date": str(stock.buy_date) if stock.buy_date else None,
                "signal": sig_data
            }
            snapshot.append(item)

        await websocket.send_json({"type": "snapshot", "data": snapshot})

        # Send current market status
        status = get_market_status()
        await websocket.send_json({"type": "market_status", "data": {"status": status}})

        # Keep alive / handle client pings
        while True:
            data = await websocket.receive_text()
            # Handle ping-pong
            if data == "ping":
                await websocket.send_text("pong")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WS error: {e}")
        manager.disconnect(websocket)


# ── Auth ───────────────────────────────────────────────────────────────────
@app.post("/api/auth/login")
async def login(data: dict = Body(...)):
    username = data.get("username")
    password = data.get("password")
    
    # Check against settings.ADMIN_PASSWORD (handling both plain and hashed for convenience)
    is_valid = False
    if password == settings.ADMIN_PASSWORD:
        is_valid = True
    else:
        try:
            is_valid = verify_password(password, settings.ADMIN_PASSWORD)
        except:
            is_valid = False

    if not is_valid:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    access_token = create_access_token(data={"sub": "admin"})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/api/auth/verify")
async def verify(admin: str = Depends(get_current_admin)):
    return {"status": "authenticated", "user": admin}


# ── Settings ───────────────────────────────────────────────────────────────
@app.get("/api/settings")
async def get_app_settings(admin: str = Depends(get_current_admin)):
    return {
        "anthropic_api_key": settings.ANTHROPIC_API_KEY,
        "anthropic_model": settings.ANTHROPIC_MODEL,
        "openai_api_key": settings.OPENAI_API_KEY,
        "openai_model": settings.OPENAI_MODEL,
        "xai_api_key": settings.XAI_API_KEY,
        "xai_model": settings.XAI_MODEL,
        "ai_provider": settings.AI_PROVIDER,
        "env": settings.APP_ENV,
        "version": "1.1.0"
    }

@app.post("/api/settings")
async def update_app_settings(data: dict = Body(...), admin: str = Depends(get_current_admin)):
    # Fields to persist
    fields = [
        "anthropic_api_key", "anthropic_model",
        "openai_api_key", "openai_model",
        "xai_api_key", "xai_model",
        "ai_provider"
    ]
    updated = {}
    
    import dotenv
    # Robustly find .env in current file's directory
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    
    # If not found there, try the parent directory (project root)
    if not os.path.exists(env_path):
        root_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        if os.path.exists(root_env):
            env_path = root_env

    for f in fields:
        val = data.get(f)
        if val is not None:
            # Update in-memory (use upper case for settings object)
            attr_name = f.upper()
            if hasattr(settings, attr_name):
                setattr(settings, attr_name, str(val))
                # Update .env
                if os.path.exists(env_path):
                    dotenv.set_key(env_path, attr_name, str(val))
                updated[f] = val

    logger.info(f"Updated settings: {list(updated.keys())}")
    return {"message": "Settings updated", "count": len(updated)}


# ── Market Status ─────────────────────────────────────────────────────────────
@app.get("/api/market/status")
async def get_status():
    status = get_market_status()
    now = get_current_ist_time()
    return {"status": status, "last_update": now.isoformat()}


# ── Portfolio ─────────────────────────────────────────────────────────────────
@app.get("/api/portfolio")
async def get_portfolio(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(StockMaster, SignalsCache)
        .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
        .order_by(SignalsCache.confidence_pct.desc())
    )
    rows = result.all()
    return [
        {
            "symbol": stock.symbol,
            "company_name": stock.company_name,
            "sector": stock.sector,
            "market_cap_cat": stock.market_cap_cat,
            "signal": _serialize_signal(signal)
        }
        for stock, signal in rows
    ]


@app.post("/api/portfolio/import")
async def import_portfolio(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    admin: str = Depends(get_current_admin)
):
    """Import portfolio holdings from an XLSX file."""
    import pandas as pd
    from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

    try:
        # Read the uploaded file
        contents = await file.read()
        df = pd.read_excel(pd.io.common.BytesIO(contents))
        
        # We look for ISINs in the dataframe to match against our master list
        isin_to_symbol = {v["isin"]: k for k, v in PORTFOLIO_STOCKS.items()}
        
        imported_count = 0
        df_str = df.astype(str)
        
        # Scan every cell for anything looking like an ISIN from our mapper
        found_isins = set()
        for col in df_str.columns:
            for val in df_str[col]:
                if val in isin_to_symbol:
                    found_isins.add(val)
        
        for isin in found_isins:
            symbol = isin_to_symbol[isin]
            stock_data = PORTFOLIO_STOCKS[symbol]
            
            # Upsert into StockMaster
            stmt = mysql_insert(StockMaster).values(
                symbol=symbol,
                company_name=stock_data["name"],
                isin=isin,
                sector=stock_data["sector"],
                market_cap_cat=stock_data["mcap"],
                type="PORTFOLIO",
                added_date=date.today(),
                is_active=True
            ).on_duplicate_key_update(
                type="PORTFOLIO",
                is_active=True
            )
            await db.execute(stmt)
            imported_count += 1
            
        await db.commit()
        return {
            "message": f"Successfully imported {imported_count} stocks from portfolio file.",
            "imported_count": imported_count
        }
    except Exception as e:
        logger.error(f"Portfolio import failed: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to process portfolio file: {str(e)}")


@app.post("/api/portfolio/add")
async def add_to_portfolio(
    symbol: str = Body(...),
    company_name: str = Body(...),
    sector: str = Body(None),
    db: AsyncSession = Depends(get_db),
    admin: str = Depends(get_current_admin)
):
    symbol = symbol.upper().strip()
    existing = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol))
    stock = existing.scalars().first()

    if stock:
        if stock.type == "PORTFOLIO" and stock.is_active:
            raise HTTPException(status_code=409, detail=f"{symbol} already in portfolio")
        stock.type = "PORTFOLIO"
        stock.is_active = True
    else:
        stock = StockMaster(
            symbol=symbol,
            company_name=company_name,
            sector=sector,
            type="PORTFOLIO",
            added_date=date.today(),
            is_active=True,
        )
        db.add(stock)

    await db.commit()
    return {"message": f"{symbol} added to portfolio"}


@app.delete("/api/portfolio/{symbol}")
async def remove_from_portfolio(symbol: str, db: AsyncSession = Depends(get_db), admin: str = Depends(get_current_admin)):
    result = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol.upper()))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    stock.is_active = False
    await db.commit()
    return {"message": f"{symbol} removed"}


# ── Watchlist ─────────────────────────────────────────────────────────────────
@app.get("/api/watchlist")
async def get_watchlist(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(StockMaster, SignalsCache)
        .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.type == "WATCHLIST", StockMaster.is_active == True)
    )
    rows = result.all()
    return [
        {
            "symbol": stock.symbol,
            "company_name": stock.company_name,
            "sector": stock.sector,
            "signal": _serialize_signal(signal)
        }
        for stock, signal in rows
    ]


@app.post("/api/watchlist/add")
async def add_to_watchlist(
    symbol: str = Body(...),
    company_name: str = Body(default=""),
    db: AsyncSession = Depends(get_db),
    admin: str = Depends(get_current_admin)
):
    symbol = symbol.upper().strip()
    existing = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol))
    stock = existing.scalars().first()

    if stock:
        if stock.type == "WATCHLIST" and stock.is_active:
            raise HTTPException(status_code=409, detail=f"{symbol} already in watchlist")
        stock.type = "WATCHLIST"
        stock.is_active = True
    else:
        # Auto-resolve company name if not provided
        name = company_name or symbol
        stock = StockMaster(
            symbol=symbol,
            company_name=name,
            type="WATCHLIST",
            added_date=date.today(),
            is_active=True,
        )
        db.add(stock)

    await db.commit()

    # Trigger background fetch for new stock
    asyncio.create_task(_bootstrap_new_stock(symbol))

    return {"message": f"{symbol} added to watchlist"}


@app.delete("/api/watchlist/{symbol}")
async def remove_from_watchlist(symbol: str, db: AsyncSession = Depends(get_db), admin: str = Depends(get_current_admin)):
    result = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol.upper()))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    stock.is_active = False
    await db.commit()
    return {"message": f"{symbol} removed from watchlist"}


async def _bootstrap_new_stock(symbol: str):
    """Kick off initial data fetch for a newly added stock."""
    try:
        from backend.data.fetcher import fetch_live_prices
        prices = await fetch_live_prices([symbol])
        if symbol in prices:
            async with SessionLocal() as db:
                stmt = mysql_insert(SignalsCache).values(
                    symbol=symbol,
                    computed_at=datetime.now(),
                    market_session="LIVE",
                    current_price=prices[symbol]["close"],
                )
                stmt = stmt.on_duplicate_key_update(
                    current_price=stmt.inserted.current_price,
                    computed_at=stmt.inserted.computed_at
                )
                await db.execute(stmt)
                await db.commit()
    except Exception as e:
        logger.error(f"Bootstrap fetch failed for {symbol}: {e}")


# ── Opportunities ─────────────────────────────────────────────────────────────
@app.get("/api/opportunities")
async def get_opportunities(
    signal: str = None,
    min_confidence: float = 0,
    limit: int = 20,
    db: AsyncSession = Depends(get_db)
):
    query = (
        select(StockMaster, SignalsCache)
        .join(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.is_active == True)
        .where(SignalsCache.confidence_pct >= min_confidence)
    )
    if signal and signal in ("BUY", "HOLD", "SELL"):
        query = query.where(SignalsCache.st_signal == signal)

    query = query.order_by(SignalsCache.confidence_pct.desc()).limit(limit)
    result = await db.execute(query)
    rows = result.all()

    return [
        {
            "symbol": stock.symbol,
            "company_name": stock.company_name,
            "sector": stock.sector,
            "signal": _serialize_signal(signal_row)
        }
        for stock, signal_row in rows
    ]


# ── Stock Detail ──────────────────────────────────────────────────────────────
@app.get("/api/stock/{symbol}/history")
async def get_stock_history(symbol: str, days: int = 365, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.symbol == symbol.upper())
        .order_by(PriceHistory.date.asc())
    )
    history = result.scalars().all()

    records = [
        {
            "date": str(h.date),
            "open": float(h.open) if h.open else None,
            "high": float(h.high) if h.high else None,
            "low": float(h.low) if h.low else None,
            "close": float(h.close),
            "volume": int(h.volume) if h.volume else 0
        }
        for h in history
    ]
    # Slice to requested days
    return records[-days:] if days < len(records) else records


@app.get("/api/stock/{symbol}/signals")
async def get_stock_signals(symbol: str, db: AsyncSession = Depends(get_db)):
    """Return the full signal + indicator breakdown for a stock."""
    result = await db.execute(
        select(SignalsCache).where(SignalsCache.symbol == symbol.upper())
    )
    sig = result.scalars().first()
    if not sig:
        raise HTTPException(status_code=404, detail="No signals found for this symbol")

    return {
        **_serialize_signal(sig),
        "computed_at": str(sig.computed_at),
        "market_session": sig.market_session,
        "prev_close": float(sig.prev_close) if sig.prev_close else None,
        "flags": sig.flags,
        "indicator_breakdown": sig.indicator_breakdown,
    }


@app.get("/api/stock/{symbol}/fundamentals")
async def get_stock_fundamentals(symbol: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(FundamentalsCache).where(FundamentalsCache.symbol == symbol.upper())
    )
    fund = result.scalars().first()
    if not fund:
        raise HTTPException(status_code=404, detail="No fundamentals found")
    return {
        "fetched_at": str(fund.fetched_at),
        "pe_ratio": float(fund.pe_ratio) if fund.pe_ratio else None,
        "eps": float(fund.eps) if fund.eps else None,
        "roe": float(fund.roe) if fund.roe else None,
        "debt_equity": float(fund.debt_equity) if fund.debt_equity else None,
        "revenue_growth": float(fund.revenue_growth) if fund.revenue_growth else None,
        "market_cap": int(fund.market_cap) if fund.market_cap else None,
        "data_quality": fund.data_quality,
    }


@app.get("/api/stock/{symbol}/lots")
async def get_stock_lots(symbol: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(PortfolioTransaction)
        .where(PortfolioTransaction.symbol == symbol.upper(), PortfolioTransaction.status == 'OPEN')
        .order_by(PortfolioTransaction.buy_date.asc())
    )
    lots = result.scalars().all()
    return [
        {
            "id": lot.id,
            "quantity": float(lot.quantity),
            "buy_price": float(lot.buy_price),
            "buy_date": str(lot.buy_date)
        }
        for lot in lots
    ]


@app.get("/api/stock/{symbol}/insight")
async def get_stock_insight(symbol: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(AIInsights)
        .where(AIInsights.symbol == symbol.upper())
        .order_by(AIInsights.generated_at.desc())
        .limit(1)
    )
    insight = result.scalars().first()
    if not insight:
        raise HTTPException(status_code=404, detail="No insight found")

    return {
        "generated_at": str(insight.generated_at),
        "trigger_reason": insight.trigger_reason,
        "short_summary": insight.short_summary,
        "long_summary": insight.long_summary,
        "key_risks": insight.key_risks,
        "key_opportunities": insight.key_opportunities,
        "sentiment_score": float(insight.sentiment_score) if insight.sentiment_score else None,
    }


from pydantic import BaseModel
class BhavcopySyncRequest(BaseModel):
    from_date: str
    to_date: Optional[str] = None
    exchange: Optional[str] = 'NSE'

class InsightGenerateRequest(BaseModel):
    skill_id: Optional[str] = None

@app.post("/api/stock/{symbol}/insight/generate")
async def trigger_insight_generation(
    symbol: str, 
    req: Optional[InsightGenerateRequest] = None,
    db: AsyncSession = Depends(get_db), 
    admin: str = Depends(get_current_admin)
):
    """Manually trigger AI insight generation for a symbol."""
    # Validate symbol exists
    result = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol.upper()))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Symbol not found in master")

    skill_id = req.skill_id if req else None
    asyncio.create_task(_generate_and_save_insight(symbol.upper(), "MANUAL", skill_id))
    return {"message": f"AI insight generation queued for {symbol} (Skill: {skill_id or 'General'})"}


async def _generate_and_save_insight(symbol: str, trigger: str, skill_id: str = None):
    """Background task to generate and persist AI insight."""
    try:
        import pandas as pd
        from backend.engine.ai_engine import generate_insight
        from backend.data.db import AIInsights

        async with SessionLocal() as db:
            # Fetch price history
            ph_result = await db.execute(
                select(PriceHistory)
                .where(PriceHistory.symbol == symbol)
                .order_by(PriceHistory.date.asc())
            )
            history = ph_result.scalars().all()
            df = pd.DataFrame([
                {"date": h.date, "close": float(h.close), "open": float(h.open or 0),
                 "high": float(h.high or 0), "low": float(h.low or 0), "volume": int(h.volume or 0)}
                for h in history
            ])

            # Fetch signals
            sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
            sig = sig_result.scalars().first()
            signals = _serialize_signal(sig) if sig else {}

            # Fetch fundamentals
            fund_result = await db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == symbol))
            fund = fund_result.scalars().first()
            fundamentals = {
                "pe_ratio": float(fund.pe_ratio) if fund and fund.pe_ratio else None,
                "eps": float(fund.eps) if fund and fund.eps else None,
                "roe": float(fund.roe) if fund and fund.roe else None,
                "debt_equity": float(fund.debt_equity) if fund and fund.debt_equity else None,
                "revenue_growth": float(fund.revenue_growth) if fund and fund.revenue_growth else None,
            } if fund else {}

        insight_data = await generate_insight(symbol, df, signals, fundamentals, trigger, skill_id)

        async with SessionLocal() as db:
            new_insight = AIInsights(
                symbol=symbol,
                generated_at=datetime.now(),
                trigger_reason=trigger,
                short_summary=insight_data.get("short_summary"),
                long_summary=insight_data.get("long_summary"),
                key_risks=insight_data.get("key_risks", []),
                key_opportunities=insight_data.get("key_opportunities", []),
                sentiment_score=insight_data.get("sentiment_score"),
            )
            db.add(new_insight)
            await db.commit()
            logger.info(f"AI insight saved for {symbol}")

    except Exception as e:
        logger.error(f"AI insight generation failed for {symbol}: {e}")


# ── Bhavcopy Manual Sync ──────────────────────────────────────────────────────
@app.post("/api/market/bhavcopy/sync")
async def manual_bhavcopy_sync(req: BhavcopySyncRequest, admin: str = Depends(get_current_admin)):
    """Manually trigger Bhavcopy download for a single date or a range."""
    try:
        from datetime import datetime
        from backend.data.bhavcopy import load_bhavcopy_to_db, load_historical_bhavcopy
        import asyncio
        
        exchange = req.exchange.upper() if req.exchange else 'NSE'
        start_date = datetime.strptime(req.from_date, "%Y-%m-%d")
        
        if req.to_date:
            end_date = datetime.strptime(req.to_date, "%Y-%m-%d")
            # Trigger background range sync
            asyncio.create_task(load_historical_bhavcopy(start_date, end_date, exchange=exchange))
            return {"message": f"Bulk {exchange} sync initiated from {req.from_date} to {req.to_date}"}
        else:
            # Trigger background single day sync
            asyncio.create_task(load_bhavcopy_to_db(start_date, sync_type="MANUAL", exchange=exchange))
            return {"message": f"Manual {exchange} sync queued for {req.from_date}"}
            
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

@app.get("/api/market/bhavcopy/logs")
async def get_bhavcopy_logs(limit: int = 20, db: AsyncSession = Depends(get_db), admin: str = Depends(get_current_admin)):
    """Fetch recent Bhavcopy sync logs."""
    result = await db.execute(
        select(SyncLog).order_by(SyncLog.completed_at.desc()).limit(limit)
    )
    logs = result.scalars().all()
    return [
        {
            "id": log.id,
            "target_date": str(log.target_date),
            "exchange": log.exchange,
            "sync_type": log.sync_type,
            "status": log.status,
            "records_count": log.records_count,
            "error_message": log.error_message,
            "completed_at": log.completed_at.isoformat()
        } for log in logs
    ]

# ── System Configuration ───────────────────────────────────────────────────
class ConfigUpdateRequest(BaseModel):
    key: str
    value: str

@app.get("/api/settings/config")
async def get_system_config(db: AsyncSession = Depends(get_db), admin: str = Depends(get_current_admin)):
    """Fetch all system configurations."""
    result = await db.execute(select(SystemConfig))
    configs = result.scalars().all()
    return {c.key: {"value": c.value, "description": c.description} for c in configs}

@app.patch("/api/settings/config")
async def update_system_config(req: ConfigUpdateRequest, db: AsyncSession = Depends(get_db), admin: str = Depends(get_current_admin)):
    """Update a specific configuration value."""
    result = await db.execute(select(SystemConfig).where(SystemConfig.key == req.key))
    config = result.scalar_one_or_none()
    
    if not config:
        raise HTTPException(status_code=404, detail="Configuration key not found")
    
    config.value = req.value
    await db.commit()
    return {"message": f"Successfully updated {req.key}"}

# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "market": get_market_status(),
        "time": get_current_ist_time().isoformat(),
        "ws_clients": len(manager.active_connections)
    }


# ── Helper ────────────────────────────────────────────────────────────────────
def _serialize_signal(signal) -> dict:
    if not signal:
        return None
    return {
        "current_price": float(signal.current_price) if signal.current_price else None,
        "change_pct": float(signal.change_pct) if signal.change_pct else None,
        "st_signal": signal.st_signal,
        "lt_signal": signal.lt_signal,
        "st_score": float(signal.st_score) if signal.st_score else None,
        "lt_score": float(signal.lt_score) if signal.lt_score else None,
        "confidence_pct": float(signal.confidence_pct) if signal.confidence_pct else None,
        "data_quality": signal.data_quality,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
