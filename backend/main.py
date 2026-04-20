from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Body, UploadFile, File, BackgroundTasks
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
    StockMaster, SignalsCache, AIInsights, PriceHistory, FundamentalsCache, SyncLog,
    PortfolioTransaction, AICallLog, AllocationLog, SystemConfig, IntradayTicks
)
from backend.utils.market_hours import get_market_status, get_current_ist_time
from backend.utils.auth import verify_password, create_access_token, get_current_admin, get_password_hash
from backend.config import settings
from backend.api import analysis
from backend.engine.ai_engine import generate_portfolio_allocation, generate_chart_chat

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

app.include_router(analysis.router)

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
async def get_app_settings(admin: str = Depends(get_current_admin), db: AsyncSession = Depends(get_db)):
    # Load AI settings from DB (system_config), fallback to in-memory settings
    from backend.data.db import SystemConfig
    result = await db.execute(select(SystemConfig))
    db_config = {row.key: row.value for row in result.scalars().all()}

    return {
        "anthropic_api_key": db_config.get("ANTHROPIC_API_KEY", settings.ANTHROPIC_API_KEY),
        "anthropic_model": db_config.get("ANTHROPIC_MODEL", settings.ANTHROPIC_MODEL),
        "openai_api_key": db_config.get("OPENAI_API_KEY", settings.OPENAI_API_KEY),
        "openai_model": db_config.get("OPENAI_MODEL", settings.OPENAI_MODEL),
        "xai_api_key": db_config.get("XAI_API_KEY", settings.XAI_API_KEY),
        "xai_model": db_config.get("XAI_MODEL", settings.XAI_MODEL),
        "ai_provider": db_config.get("AI_PROVIDER", settings.AI_PROVIDER),
        "env": settings.APP_ENV,
        "version": "1.1.0"
    }

@app.post("/api/settings")
async def update_app_settings(data: dict = Body(...), admin: str = Depends(get_current_admin), db: AsyncSession = Depends(get_db)):
    """Save AI settings to system_config DB table + update in-memory."""
    from backend.data.db import SystemConfig
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    AI_FIELDS = {
        "anthropic_api_key": "ANTHROPIC_API_KEY",
        "anthropic_model": "ANTHROPIC_MODEL",
        "openai_api_key": "OPENAI_API_KEY",
        "openai_model": "OPENAI_MODEL",
        "xai_api_key": "XAI_API_KEY",
        "xai_model": "XAI_MODEL",
        "ai_provider": "AI_PROVIDER",
    }
    updated = []

    for field, db_key in AI_FIELDS.items():
        val = data.get(field)
        if val is not None:
            # 1. Upsert into system_config
            stmt = mysql_insert(SystemConfig).values(
                key=db_key, value=str(val), description=f"AI setting: {field}"
            )
            stmt = stmt.on_duplicate_key_update(value=str(val), updated_at=datetime.now())
            await db.execute(stmt)

            # 2. Update in-memory settings for the running process
            if hasattr(settings, db_key):
                setattr(settings, db_key, str(val))

            updated.append(field)

    await db.commit()
    logger.info(f"AI settings saved to DB: {updated}")
    return {"message": "Settings saved to database", "count": len(updated)}



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
    portfolio_data = []
    for stock, signal in rows:
        # Determine Accumulate/Buy More signal (price-agnostic)
        # Rule: Strong fundamentals/moat (composite > 60) AND positive short-term momentum (st_signal == 'BUY')
        is_accumulate_recommended = False
        if signal and signal.composite_score is not None and signal.st_signal == 'BUY':
            if signal.composite_score > 60:
                is_accumulate_recommended = True

        portfolio_data.append({
            "symbol": stock.symbol,
            "company_name": stock.company_name,
            "sector": stock.sector,
            "market_cap_cat": stock.market_cap_cat,
            "quantity": float(stock.quantity) if stock.quantity else 0.0,
            "avg_buy_price": float(stock.avg_buy_price) if stock.avg_buy_price else 0.0,
            "is_accumulate_recommended": is_accumulate_recommended,
            "signal": _serialize_signal(signal)
        })
    
    return portfolio_data


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


@app.post("/api/portfolio/allocate")
async def allocate_portfolio(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db)
):
    amount = float(payload.get("amount", 10000))
    use_ai = bool(payload.get("use_ai", False))
    limit = payload.get("limit")

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than 0")

    result = await db.execute(
        select(StockMaster, SignalsCache)
        .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
    )
    rows = result.all()
    if not rows:
        raise HTTPException(status_code=400, detail="No active portfolio stocks found")

    portfolio_data = []
    for stock, signal in rows:
        portfolio_data.append({
            "symbol": stock.symbol,
            "sector": stock.sector,
            "signal": _serialize_signal(signal) if signal else None
        })

    # Sort descending by composite score to find the strongest stocks
    portfolio_data.sort(
        key=lambda x: float(x["signal"]["composite_score"] or 0) if x["signal"] else 0.0, 
        reverse=True
    )

    # Apply top N limit if requested
    if limit is not None and str(limit).strip():
        try:
            limit_int = int(limit)
            if limit_int > 0:
                portfolio_data = portfolio_data[:limit_int]
        except ValueError:
            pass

    allocation_result = {}
    allocation_type = "AI_DRIVEN" if use_ai else "PROPORTIONAL"

    if use_ai:
        try:
            allocation_result = await generate_portfolio_allocation(amount, portfolio_data)
        except Exception as e:
            logger.error(f"AI Allocation failed, falling back to proportional: {e}")
            use_ai = False
            allocation_type = "PROPORTIONAL"

    if not use_ai:
        # Fallback proportional allocation based on composite scores
        total_score = sum([float(p["signal"]["composite_score"] or 50) for p in portfolio_data if p["signal"]])
        if total_score == 0:
            total_score = len(portfolio_data) * 50
        
        allocs = []
        for p in portfolio_data:
            score = float(p["signal"]["composite_score"] or 50) if p["signal"] else 50.0
            weight = score / total_score
            alloc_amt = amount * weight
            price = p["signal"]["current_price"] if p["signal"] and p["signal"]["current_price"] else 1.0
            
            allocs.append({
                "symbol": p["symbol"],
                "allocated_amount": round(alloc_amt, 2),
                "estimated_qty": round(alloc_amt / float(price), 2) if price else 0,
                "reason": f"Proportional allocation based on composite score of {score:.1f}"
            })
            
        allocs.sort(key=lambda x: x["allocated_amount"], reverse=True)
        allocation_result = {
            "rationale": "Mathematical proportional allocation based on AI composite scores.",
            "allocations": allocs
        }

    # Save to db
    log_entry = AllocationLog(
        total_amount=amount,
        allocation_type=allocation_type,
        allocations=allocation_result
    )
    db.add(log_entry)
    await db.commit()

    return allocation_result


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
    # Join or fetch StockMaster metadata too
    stock_res = await db.execute(select(StockMaster.yahoo_symbol).where(StockMaster.symbol == symbol.upper()))
    stock_row = stock_res.scalar_one_or_none()

    return {
        "fetched_at": str(fund.fetched_at),
        "pe_ratio": float(fund.pe_ratio) if fund.pe_ratio else None,
        "eps": float(fund.eps) if fund.eps else None,
        "roe": float(fund.roe) if fund.roe else None,
        "debt_equity": float(fund.debt_equity) if fund.debt_equity else None,
        "revenue_growth": float(fund.revenue_growth) if fund.revenue_growth else None,
        "market_cap": int(fund.market_cap) if fund.market_cap else None,
        
        # -- Institutional Upgrade --
        "peg_ratio": float(fund.peg_ratio) if fund.peg_ratio else None,
        "ps_ratio": float(fund.ps_ratio) if fund.ps_ratio else None,
        "pb_ratio": float(fund.pb_ratio) if fund.pb_ratio else None,
        "ev_ebitda": float(fund.ev_ebitda) if fund.ev_ebitda else None,
        "book_value": float(fund.book_value) if fund.book_value else None,
        "ebitda": fund.ebitda,
        "held_percent_institutions": float(fund.held_percent_institutions) if fund.held_percent_institutions else None,
        "shares_outstanding": fund.shares_outstanding,
        
        # -- Phase 3: Health & Sentiment --
        "analyst_rating": float(fund.analyst_rating) if fund.analyst_rating else None,
        "recommendation_key": fund.recommendation_key,
        "total_cash": fund.total_cash,
        "total_debt": fund.total_debt,
        "current_ratio": float(fund.current_ratio) if fund.current_ratio else None,
        
        "data_quality": fund.data_quality,
        "yahoo_symbol": stock_row,
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
            "buy_date": str(lot.buy_date),
            "status": lot.status
        }
        for lot in lots
    ]


@app.post("/api/stock/{symbol}/chart_chat")
async def handle_chart_chat(
    symbol: str,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db)
):
    """Handles an interactive chat about the chart, utilizing historical OHLC and Signals as context."""
    chat_history = payload.get("messages", [])
    symbol = symbol.upper()

    # Get signals context
    sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = sig_result.scalars().first()
    
    # Get last 90 bars of history
    hist_result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.desc())
        .limit(90)
    )
    history = hist_result.scalars().all()
    history.reverse() # chronological order

    # Get today's true intraday OHLCV from IntradayTicks (the authoritative live source)
    from sqlalchemy import func, cast, Date as SADate
    from datetime import date as dt_date
    today_date = dt_date.today()
    intraday_result = await db.execute(
        select(
            func.min(IntradayTicks.open).label("open"),
            func.max(IntradayTicks.high).label("high"),
            func.min(IntradayTicks.low).label("low"),
            IntradayTicks.close,
            func.sum(IntradayTicks.volume).label("volume"),
        )
        .where(
            IntradayTicks.symbol == symbol,
            func.date(IntradayTicks.timestamp) == today_date,
        )
        .order_by(IntradayTicks.timestamp.desc())
        .limit(1)
    )
    intraday_row = intraday_result.first()

    # Aggregate 90 bars into compact summary to minimize AI token cost
    closes = [float(h.close) for h in history]
    highs  = [float(h.high)  for h in history]
    lows   = [float(h.low)   for h in history]
    vols   = [int(h.volume)  for h in history]

    def sma(series, n):
        return round(sum(series[-n:]) / min(len(series), n), 2) if series else None

    # Build weekly buckets (5 bars ≈ 1 week)
    def weekly_aggregates(hist):
        buckets = []
        for i in range(0, len(hist), 5):
            chunk = hist[i:i+5]
            if not chunk: continue
            buckets.append({
                "week_start": str(chunk[0].date),
                "open":  round(float(chunk[0].open), 2),
                "high":  round(max(float(h.high) for h in chunk), 2),
                "low":   round(min(float(h.low)  for h in chunk), 2),
                "close": round(float(chunk[-1].close), 2),
                "avg_vol": round(sum(int(h.volume) for h in chunk) / len(chunk)),
            })
        return buckets

    price_summary = {
        "period_days": len(history),
        "period_start_close": round(closes[0], 2) if closes else None,
        "period_end_close":   round(closes[-1], 2) if closes else None,
        "period_change_pct":  round((closes[-1] - closes[0]) / closes[0] * 100, 2) if closes else None,
        "90d_high": round(max(highs), 2) if highs else None,
        "90d_low":  round(min(lows), 2) if lows else None,
        "avg_volume_90d": round(sum(vols) / len(vols)) if vols else None,
        "sma_20":  sma(closes, 20),
        "sma_50":  sma(closes, 50),
        "sma_90":  sma(closes, 90),
        "weekly_candles": weekly_aggregates(history),
        "recent_5_bars": [
            {
                "date": str(h.date), "open": round(float(h.open), 2),
                "high": round(float(h.high), 2), "low": round(float(h.low), 2),
                "close": round(float(h.close), 2), "volume": int(h.volume)
            } for h in history[-5:]
        ],
    }

    # Format the context tightly
    context_data = {
        "current_st_signal": sig.st_signal if sig else "HOLD",
        "current_lt_signal": sig.lt_signal if sig else "HOLD",
        "composite_score": float(sig.composite_score) if sig and sig.composite_score else None,
        "indicators": {
            "composite_score": float(sig.composite_score) if sig and sig.composite_score else None,
            "ta": sig.ta_breakdown or {},
            "fa": sig.fa_breakdown or {},
            "momentum": sig.momentum_breakdown or {},
        },
        # ── TODAY's live intraday snapshot — use these exact values, do NOT guess ──
        "today": {
            "date": str(today_date),
            "open": float(intraday_row.open) if intraday_row and intraday_row.open else None,
            "high": float(intraday_row.high) if intraday_row and intraday_row.high else None,
            "low": float(intraday_row.low) if intraday_row and intraday_row.low else None,
            "close": float(sig.current_price) if sig and sig.current_price else (float(intraday_row.close) if intraday_row and intraday_row.close else None),
            "prev_close": float(sig.prev_close) if sig and sig.prev_close else None,
            "change_pct": float(sig.change_pct) if sig and sig.change_pct else None,
            "volume": int(intraday_row.volume) if intraday_row and intraday_row.volume else None,
        },
        "price_summary": price_summary,  # aggregated — replaces raw 90-bar dump
    }

    try:
        response = await generate_chart_chat(symbol, chat_history, context_data)
        return response
    except Exception as e:
        logger.error(f"Chart Chat Error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/ai-logs")
async def get_ai_logs(
    limit: int = 100,
    symbol: Optional[str] = None,
    trigger: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Return all AI insight logs for the AI Logs page, newest first."""
    query = select(AIInsights).order_by(AIInsights.generated_at.desc()).limit(limit)
    if symbol:
        query = query.where(AIInsights.symbol == symbol.upper())
    if trigger:
        query = query.where(AIInsights.trigger_reason == trigger)
    result = await db.execute(query)
    insights = result.scalars().all()
    return [
        {
            "id": ins.id,
            "symbol": ins.symbol,
            "generated_at": str(ins.generated_at),
            "trigger_reason": ins.trigger_reason,
            "skill_id": ins.skill_id,
            "verdict": ins.verdict,
            "short_summary": ins.short_summary,
            "long_summary": ins.long_summary,
            "key_risks": ins.key_risks or [],
            "key_opportunities": ins.key_opportunities or [],
            "sentiment_score": float(ins.sentiment_score) if ins.sentiment_score else None,
        }
        for ins in insights
    ]

@app.get("/api/ai-logs/calls")
async def get_ai_call_logs(
    limit: int = 100,
    symbol: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Return raw AI API call logs with token usage, payloads, and timing."""
    query = select(AICallLog).order_by(AICallLog.called_at.desc()).limit(limit)
    if symbol:
        query = query.where(AICallLog.symbol == symbol.upper())
    result = await db.execute(query)
    logs = result.scalars().all()
    return [
        {
            "id": log.id,
            "insight_id": log.insight_id,
            "symbol": log.symbol,
            "skill_id": log.skill_id,
            "provider": log.provider,
            "model": log.model,
            "trigger_reason": log.trigger_reason,
            "prompt_tokens": log.prompt_tokens,
            "completion_tokens": log.completion_tokens,
            "total_tokens": log.total_tokens,
            "duration_ms": log.duration_ms,
            "status": log.status,
            "error_message": log.error_message,
            "request_payload": log.request_payload,
            "response_raw": log.response_raw,
            "called_at": str(log.called_at),
        }
        for log in logs
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
    skill_id: str

class FundamentalUpdateRequest(BaseModel):
    pe_ratio: Optional[float] = None
    eps: Optional[float] = None
    roe: Optional[float] = None
    debt_equity: Optional[float] = None
    revenue_growth: Optional[float] = None
    market_cap: Optional[float] = None
    yahoo_symbol: Optional[str] = None

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


@app.post("/api/stock/{symbol}/fundamentals/research")
async def trigger_fundamental_research(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    admin: str = Depends(get_current_admin),
    background_tasks: BackgroundTasks = None
):
    """Trigger AI to research and fill in missing fundamental data."""
    symbol = symbol.upper()
    result = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol))
    if not result.scalars().first():
         raise HTTPException(status_code=404, detail="Symbol not found")

    background_tasks.add_task(_research_and_save_fundamentals, symbol)
    return {"message": f"Historical research task started for {symbol}."}


@app.patch("/api/stock/{symbol}/fundamentals")
async def update_fundamentals(
    symbol: str, 
    update: FundamentalUpdateRequest, 
    db: AsyncSession = Depends(get_db),
    admin: dict = Depends(get_current_admin)
):
    """Manually override fundamental data for a stock."""
    from backend.scheduler import recompute_signals_for
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    symbol = symbol.upper()
    
    # 1. Update/Insert fundamentally verified data
    stmt = mysql_insert(FundamentalsCache).values(
        symbol=symbol,
        fetched_at=datetime.now(),
        pe_ratio=update.pe_ratio,
        eps=update.eps,
        roe=update.roe,
        debt_equity=update.debt_equity,
        revenue_growth=update.revenue_growth,
        market_cap=update.market_cap,
        data_quality="VERIFIED"
    )
    
    cols_to_update = {
        "fetched_at": stmt.inserted.fetched_at,
        "data_quality": stmt.inserted.data_quality
    }
    if update.pe_ratio is not None: cols_to_update["pe_ratio"] = stmt.inserted.pe_ratio
    if update.eps is not None: cols_to_update["eps"] = stmt.inserted.eps
    if update.roe is not None: cols_to_update["roe"] = stmt.inserted.roe
    if update.debt_equity is not None: cols_to_update["debt_equity"] = stmt.inserted.debt_equity
    if update.revenue_growth is not None: cols_to_update["revenue_growth"] = stmt.inserted.revenue_growth
    if update.market_cap is not None: cols_to_update["market_cap"] = stmt.inserted.market_cap
    
    stmt = stmt.on_duplicate_key_update(**cols_to_update)
    
    await db.execute(stmt)
    
    # 2. Update yahoo_symbol in StockMaster if provided
    if update.yahoo_symbol is not None:
        await db.execute(
            update(StockMaster)
            .where(StockMaster.symbol == symbol)
            .values(yahoo_symbol=update.yahoo_symbol)
        )
    
    await db.commit()
    
    # 3. Trigger signal recompute
    await recompute_signals_for(symbol)
    
    return {"message": f"Fundamentals updated and signals recomputed for {symbol}."}


@app.post("/api/stock/{symbol}/fundamentals/sync")
async def sync_fundamental_data(
    symbol: str, 
    db: AsyncSession = Depends(get_db),
    admin: dict = Depends(get_current_admin)
):
    """Trigger a fresh fetch of fundamentals from Yahoo Finance."""
    from backend.data.fetcher import fetch_fundamentals
    from backend.scheduler import recompute_signals_for
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    symbol = symbol.upper()
    
    # 1. Fetch from StockMaster for Yahoo symbol mapping
    stock_res = await db.execute(select(StockMaster.yahoo_symbol).where(StockMaster.symbol == symbol))
    yf_sym = stock_res.scalar_one_or_none()
    
    # 2. Fetch from Yahoo
    data = await fetch_fundamentals(symbol, yahoo_symbol=yf_sym)
    
    # 2. Persist
    stmt = mysql_insert(FundamentalsCache).values(
        symbol=symbol,
        fetched_at=datetime.now(),
        pe_ratio=data.get("pe_ratio"),
        eps=data.get("eps"),
        roe=data.get("roe"),
        debt_equity=data.get("debt_equity"),
        revenue_growth=data.get("revenue_growth"),
        market_cap=data.get("market_cap"),
        data_quality=data.get("data_quality", "FULL")
    )
    
    cols = ["fetched_at", "pe_ratio", "eps", "roe", "debt_equity", "revenue_growth", "market_cap", "data_quality"]
    stmt = stmt.on_duplicate_key_update(**{c: stmt.inserted[c] for c in cols})
    
    await db.execute(stmt)
    await db.commit()
    
    # 3. Always recompute signals when data changes
    await recompute_signals_for(symbol)
    
    return {"message": f"Fundamentals synced from Yahoo Finance for {symbol}.", "status": data.get("data_quality")}


@app.post("/api/stock/{symbol}/signals/recompute")
async def recompute_stock_signals(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    admin: dict = Depends(get_current_admin)
):
    """Manually trigger a re-computation of all signals for a symbol."""
    from backend.scheduler import recompute_signals_for, _fetch_all_sector_data
    symbol = symbol.upper()
    
    try:
        # Fetch sector peers for accurate V2 ranking
        vault = await _fetch_all_sector_data()
        await recompute_signals_for(symbol, sector_vault=vault)
        return {"message": f"Signals recomputed for {symbol}."}
    except Exception as e:
        logger.error(f"Manual recompute failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/portfolio/sync-signals")
async def sync_portfolio_signals(
    db: AsyncSession = Depends(get_db),
    admin: dict = Depends(get_current_admin)
):
    """Recompute signals for every active PORTFOLIO stock in one click."""
    from backend.scheduler import recompute_signals_for, _fetch_all_sector_data

    result = await db.execute(
        select(StockMaster)
        .where(StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
    )
    stocks = result.scalars().all()
    if not stocks:
        raise HTTPException(status_code=404, detail="No portfolio stocks found")

    symbols = [s.symbol for s in stocks]
    logger.info(f"Bulk portfolio signal sync started for {len(symbols)} stocks")

    # Fetch sector vault once for all stocks
    vault = await _fetch_all_sector_data()

    success, errors = 0, []
    for sym in symbols:
        try:
            await recompute_signals_for(sym, sector_vault=vault)
            success += 1
            logger.info(f"[BULK SYNC] {sym} — OK")
        except Exception as e:
            errors.append(sym)
            logger.error(f"[BULK SYNC] {sym} — FAILED: {e}")

    return {
        "message": f"Signal sync complete: {success}/{len(symbols)} succeeded.",
        "success_count": success,
        "error_count": len(errors),
        "failed_symbols": errors,
    }



async def _research_and_save_fundamentals(symbol: str):
    """Background task to research fundamentals via AI and recompute signals."""
    try:
        from backend.engine.ai_engine import generate_fundamentals
        from backend.scheduler import recompute_signals_for
        
        # 0. Fetch company name
        company_name = symbol
        async with SessionLocal() as session:
            stock_res = await session.execute(select(StockMaster).where(StockMaster.symbol == symbol))
            stock = stock_res.scalars().first()
            if stock:
                company_name = stock.company_name

        # 1. Generate via AI
        ai_data = await generate_fundamentals(symbol, company_name)
        
        # 2. Persist to DB
        async with SessionLocal() as session:
            stmt = mysql_insert(FundamentalsCache).values(
                symbol=symbol,
                fetched_at=datetime.now(),
                pe_ratio=ai_data.get("pe_ratio"),
                eps=ai_data.get("eps"),
                roe=ai_data.get("roe"),
                debt_equity=ai_data.get("debt_equity"),
                revenue_growth=ai_data.get("revenue_growth"),
                market_cap=ai_data.get("market_cap"),
                data_quality="AI_RESEARCHED"
            )
            FUND_COLS = ["fetched_at", "pe_ratio", "eps", "roe", "debt_equity",
                         "revenue_growth", "market_cap", "data_quality"]
            stmt = stmt.on_duplicate_key_update(
                **{c: stmt.inserted[c] for c in FUND_COLS}
            )
            await session.execute(stmt)
            await session.commit()
            
            # 3. Recompute signals (FA data has changed)
            await recompute_signals_for(symbol)
            logger.info(f"AI Fundamental research + Signal recompute complete for {symbol}.")

    except Exception as e:
        logger.error(f"AI Fund research background task failed for {symbol}: {e}")


async def _generate_and_save_insight(symbol: str, trigger: str, skill_id: str = None):
    """Background task to generate and persist AI insight."""
    try:
        import pandas as pd
        from backend.engine.ai_engine import generate_insight
        from backend.data.db import AIInsights

        async with SessionLocal() as db:
            # Fetch company details
            stock_result = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol))
            stock = stock_result.scalars().first()
            company_name = stock.company_name if stock else symbol

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
                "peg_ratio": float(fund.peg_ratio) if fund and fund.peg_ratio else None,
                "ps_ratio": float(fund.ps_ratio) if fund and fund.ps_ratio else None,
                "pb_ratio": float(fund.pb_ratio) if fund and fund.pb_ratio else None,
                "ev_ebitda": float(fund.ev_ebitda) if fund and fund.ev_ebitda else None,
                "held_percent_institutions": float(fund.held_percent_institutions) if fund and fund.held_percent_institutions else None,
            } if fund else {}

        insight_data = await generate_insight(symbol, df, signals, fundamentals, trigger, skill_id, company_name=company_name)

        if not insight_data:
            logger.error(f"generate_insight returned None for {symbol} — skipping save to AIInsights.")
            return

        async with SessionLocal() as db:
            new_insight = AIInsights(
                symbol=symbol,
                generated_at=datetime.now(),
                trigger_reason=trigger,
                skill_id=skill_id,
                verdict=insight_data.get("verdict"),
                short_summary=insight_data.get("short_summary"),
                long_summary=insight_data.get("long_summary"),
                key_risks=insight_data.get("key_risks", []),
                key_opportunities=insight_data.get("key_opportunities", []),
                sentiment_score=insight_data.get("sentiment_score"),
            )
            db.add(new_insight)
            await db.commit()
            logger.info(f"AI insight saved for {symbol} (skill: {skill_id}, verdict: {insight_data.get('verdict')})")

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

@app.post("/api/market/fundamentals/sync")
async def bulk_fundamental_sync(admin: str = Depends(get_current_admin)):
    """Trigger a full refresh of fundamentals for all active stocks."""
    from backend.scheduler import run_bulk_fundamental_sync
    import asyncio
    
    # Run in background as it takes time
    asyncio.create_task(run_bulk_fundamental_sync())
    
    return {"message": "Bulk fundamental sync initiated in the background."}

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
        "current_price": float(signal.current_price) if signal.current_price is not None else None,
        "change_pct": float(signal.change_pct) if signal.change_pct is not None else None,
        "st_signal": signal.st_signal,
        "lt_signal": signal.lt_signal,
        "st_score": float(signal.st_score) if signal.st_score is not None else None,
        "lt_score": float(signal.lt_score) if signal.lt_score is not None else None,
        "confidence_pct": float(signal.confidence_pct) if signal.confidence_pct is not None else None,
        "data_quality": signal.data_quality,
        "indicator_breakdown": signal.indicator_breakdown or {},
        
        # V2 Pillar Scores
        "composite_score": float(signal.composite_score) if signal.composite_score is not None else None,
        "fundamental_score": float(signal.fundamental_score) if signal.fundamental_score is not None else None,
        "technical_score": float(signal.technical_score) if signal.technical_score is not None else None,
        "momentum_score": float(signal.momentum_score) if signal.momentum_score is not None else None,
        "sector_rank_score": float(signal.sector_rank_score) if signal.sector_rank_score is not None else None,
        
        # Metadata & Percentiles
        "sector_percentile": float(signal.sector_percentile) if signal.sector_percentile is not None else None,
        "data_confidence": float(signal.data_confidence) if signal.data_confidence is not None else None,
        "score_profile": signal.score_profile,
        "promoter_pledge_warning": bool(signal.promoter_pledge_warning) if signal.promoter_pledge_warning is not None else False,
        
        # JSON Detailed Breakdowns
        "fa_breakdown": signal.fa_breakdown or {},
        "ta_breakdown": signal.ta_breakdown or {},
        "momentum_breakdown": signal.momentum_breakdown or {},

        # V2.1 Institutional & Audit Data
        "score_version": signal.score_version,
        "scored_at": signal.scored_at.isoformat() if signal.scored_at else None,
        "fa_coverage": float(signal.fa_coverage) if signal.fa_coverage else 0,
        "indicator_breakdown": signal.indicator_breakdown,
        "fa_breakdown": signal.fa_breakdown,
        
        # -- Phase 3: Price Action --
        "fifty_two_week_high": float(signal.fifty_two_week_high) if signal.fifty_two_week_high else None,
        "fifty_two_week_low": float(signal.fifty_two_week_low) if signal.fifty_two_week_low else None,
        "fifty_two_week_change": float(signal.fifty_two_week_change) if signal.fifty_two_week_change else None,
        "beta": float(signal.beta) if signal.beta else None,
        "momentum_coverage": float(signal.momentum_coverage) if signal.momentum_coverage is not None else 0,
        "sector_peer_count": int(signal.sector_peer_count) if signal.sector_peer_count is not None else 0,
        "backtest_cagr": float(signal.backtest_cagr) if signal.backtest_cagr is not None else None,
        "backtest_win_rate": float(signal.backtest_win_rate) if signal.backtest_win_rate is not None else None,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
