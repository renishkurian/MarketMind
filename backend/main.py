from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler
from backend.utils.limiter import limiter

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Body, UploadFile, File, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from typing import List, Dict, Optional, Any
import logging
import os
import json
from datetime import datetime, date, timedelta
import asyncio
import time

from backend.data.db import (
    get_db, SessionLocal, run_migrations,
    StockMaster, SignalsCache, AIInsights, PriceHistory, FundamentalsCache, SyncLog,
    PortfolioTransaction, AICallLog, AllocationLog, SystemConfig, IntradayTicks,
    User, MoveExplanation, PriceAlert, PerformanceCache, ScreenerCache,
    CorporateAction
)
from backend.utils.market_hours import get_market_status, get_current_ist_time
from backend.utils.auth import verify_password, create_access_token, get_current_user, get_current_admin, get_password_hash
from backend.config import settings
from backend.api import analysis
from backend.engine.ai_engine import generate_portfolio_allocation, generate_chart_chat, generate_pattern_recognition, generate_move_explanation, generate_alert_levels, generate_skill_chat_response, generate_yearly_risk_explainer
from backend.engine.allocation_engine import calculate_allocation
from backend.features.portfolio import performance_routes

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
    if settings.APP_ENV == "production":
        if settings.SECRET_KEY == "marketmind_secure_vault_key_2026_reloaded_for_institutional_grade_stability":
            raise RuntimeError("FATAL: SECRET_KEY must be changed from default before running in production.")
        if settings.ADMIN_PASSWORD == "admin":
            raise RuntimeError("FATAL: ADMIN_PASSWORD must be changed from default before running in production.")
    # Create any new tables (idempotent — skips existing)
    from backend.data.db import engine, Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("DB create_all done.")
    # Run safe schema migrations (ADD COLUMN IF NOT EXISTS)
    await run_migrations()
    logger.info("DB migrations applied.")

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
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analysis.router)

from backend.features.ml import ml_routes
app.include_router(ml_routes.router)

from backend.features.portfolio_opt import opt_routes
app.include_router(opt_routes.router)

from backend.features.oracle import oracle_routes
app.include_router(oracle_routes.router)

from backend.features.war_room import war_room_routes
app.include_router(war_room_routes.router)
app.include_router(performance_routes.router)

# ── WebSocket endpoint ────────────────────────────────────────────────────────
@app.websocket("/ws/market")
async def websocket_endpoint(websocket: WebSocket, db: AsyncSession = Depends(get_db)):
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return

    # Verify user
    from backend.utils.auth import ALGORITHM
    import jwt
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            await websocket.close(code=1008)
            return
        
        # Verify user exists
        res = await db.execute(select(User).where(User.email == email))
        user = result_user = res.scalars().first()
        if not result_user:
            await websocket.close(code=1008)
            return
        user = result_user
            
    except jwt.PyJWTError:
        await websocket.close(code=1008)
        return

    await manager.connect(websocket)
    try:
        # Send initial snapshot ISOLATED to this user using unified fetcher
        snapshot = await _fetch_user_portfolio_stats(db, user, filter_type=None)
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
@app.post("/api/auth/register")
async def register(data: dict = Body(...), db: AsyncSession = Depends(get_db)):
    email = data.get("email")
    password = data.get("password")
    full_name = data.get("full_name", "")
    
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")
    
    res = await db.execute(select(User).where(User.email == email))
    if res.scalars().first():
        raise HTTPException(status_code=400, detail="User already registered")
        
    hashed = get_password_hash(password)
    res = await db.execute(select(User))
    # SECURITY: Disable first-user-auto-admin for institutional stability. 
    # New users default to USER. Admins must be promoted manually via DB or specialized API.
    role = "USER"
    
    new_user = User(email=email, hashed_password=hashed, full_name=full_name, role=role)
    db.add(new_user)
    await db.commit()
    return {"message": "User created", "role": role}

@app.post("/api/auth/login")
async def login(data: dict = Body(...), db: AsyncSession = Depends(get_db)):
    email = data.get("username") # OAuth2 sends 'username'
    password = data.get("password")
    
    res = await db.execute(select(User).where(User.email == email))
    user = res.scalars().first()
    
    if user and verify_password(password, user.hashed_password):
        token = create_access_token(data={"sub": user.email})
        return {"access_token": token, "token_type": "bearer"}
        
    # Legacy Admin Fallback (Bootstrap)
    if email == "admin" and password == settings.ADMIN_PASSWORD:
        res = await db.execute(select(User).where(User.email == "admin@marketmind.ai"))
        admin_user = res.scalars().first()
        if not admin_user:
            admin_user = User(
                email="admin@marketmind.ai",
                hashed_password=get_password_hash(password),
                full_name="System Admin",
                role="ADMIN"
            )
            db.add(admin_user)
            await db.commit()
        token = create_access_token(data={"sub": admin_user.email})
        return {"access_token": token, "token_type": "bearer"}

    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/api/auth/verify")
async def verify(current_user: User = Depends(get_current_user)):
    return {
        "status": "authenticated", 
        "user": {
            "email": current_user.email,
            "full_name": current_user.full_name,
            "role": current_user.role
        }
    }


# ── Settings ───────────────────────────────────────────────────────────────
@app.get("/api/settings")
async def get_app_settings(admin_user: User = Depends(get_current_admin), db: AsyncSession = Depends(get_db)):
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
async def update_app_settings(data: dict = Body(...), admin_user: User = Depends(get_current_admin), db: AsyncSession = Depends(get_db)):
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


from nse import NSE
from cachetools import TTLCache

indices_cache = TTLCache(maxsize=1, ttl=60)

@app.get("/api/market/indices")
async def get_market_indices():
    if "indices" in indices_cache:
        return indices_cache["indices"]
    
    target_indices = [
        "NIFTY 50", "NIFTY BANK", "NIFTY IT", "NIFTY MIDCAP 100",
        "NIFTY SMALLCAP 100", "NIFTY FMCG", "NIFTY PHARMA",
        "NIFTY AUTO", "NIFTY REALTY", "NIFTY METAL", "INDIA VIX"
    ]
    
    try:
        def fetch_indices():
            with NSE('.') as nse:
                return nse.listIndices()
        
        # Run synchronous NSE call in a thread to avoid blocking the event loop
        data = await asyncio.to_thread(fetch_indices)
        
        if not data or 'data' not in data:
            return []
            
        result = []
        for item in data['data']:
            if item.get('index') in target_indices:
                change = float(item.get('variation', 0))
                direction = "flat"
                if change > 0:
                    direction = "up"
                elif change < 0:
                    direction = "down"
                    
                result.append({
                    "name": item.get('index'),
                    "last": float(item.get('last', 0)),
                    "change": change,
                    "pct_change": float(item.get('percentChange', 0)),
                    "direction": direction
                })
        
        # Sort according to the target_indices order
        result.sort(key=lambda x: target_indices.index(x['name']) if x['name'] in target_indices else 999)
        
        indices_cache["indices"] = result
        return result
    except Exception as e:
        logger.error(f"Failed to fetch NSE indices: {e}")
        return []


# ── Portfolio ─────────────────────────────────────────────────────────────────
async def _fetch_user_portfolio_stats(db: AsyncSession, user: User, filter_type: str = "PORTFOLIO") -> list:
    """Unified helper to fetch portfolio data with PriceHistory fallback logic."""
    from sqlalchemy import func as sqlfunc
    
    query = select(StockMaster, SignalsCache).outerjoin(
        SignalsCache, StockMaster.symbol == SignalsCache.symbol
    ).where(StockMaster.user_id == user.id, StockMaster.is_active == True)
    
    if filter_type:
        query = query.where(StockMaster.type == filter_type)
        
    result = await db.execute(query)
    rows = result.all()

    # Bulk fetch latest close price from price_history as fallback
    ph_sub = (
        select(PriceHistory.symbol, sqlfunc.max(PriceHistory.date).label("max_date"))
        .group_by(PriceHistory.symbol)
        .subquery()
    )
    ph_result = await db.execute(
        select(PriceHistory.symbol, PriceHistory.close)
        .join(ph_sub, (PriceHistory.symbol == ph_sub.c.symbol) & (PriceHistory.date == ph_sub.c.max_date))
    )
    last_close_map = {row.symbol: float(row.close) for row in ph_result.all()}

    portfolio_data = []
    for stock, signal in rows:
        sig_data = _serialize_signal(signal)
        
        # Price Fallback Logic
        if sig_data and not sig_data.get("current_price"):
            sig_data["current_price"] = last_close_map.get(stock.symbol)
        elif sig_data is None:
            fb_price = last_close_map.get(stock.symbol)
            if fb_price:
                sig_data = {"current_price": fb_price}

        # Sentiment & Recommendation
        is_accumulate = False
        if sig_data and sig_data.get("st_signal") == 'BUY' and (sig_data.get("composite_score") or 0) > 60:
            is_accumulate = True

        portfolio_data.append({
            "symbol": stock.symbol,
            "company_name": stock.company_name,
            "scp_name": stock.scp_name,
            "sector": stock.sector,
            "type": stock.type,
            "market_cap_cat": stock.market_cap_cat,
            "quantity": float(stock.quantity) if stock.quantity else 0.0,
            "avg_buy_price": float(stock.avg_buy_price) if stock.avg_buy_price else 0.0,
            "buy_date": str(stock.buy_date) if stock.buy_date else None,
            "added_date": str(stock.added_date) if stock.added_date else None,
            "is_accumulate_recommended": is_accumulate,
            "signal": sig_data
        })
    return portfolio_data

@app.get("/api/portfolio")
async def get_portfolio(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user), 
    db: AsyncSession = Depends(get_db)
):
    portfolio_data = await _fetch_user_portfolio_stats(db, current_user)
    
    # Check if we should trigger a background refresh
    from backend.utils.market_hours import is_market_open
    if is_market_open() and portfolio_data:
        needs_refresh = False
        now = datetime.now()
        for item in portfolio_data:
            sig = item.get("signal")
            # We use computed_at in the frontend serialized as 'computed_at' or 'scored_at'
            # Here we check the serialized dictionary. _serialize_signal puts computed_at
            if not sig or not sig.get("computed_at"):
                needs_refresh = True
                break
        
        if needs_refresh:
            from backend.scheduler import intraday_fetch
            background_tasks.add_task(intraday_fetch)
            logger.info("Triggered background intraday_fetch via portfolio access.")
    
    return portfolio_data

@app.post("/api/portfolio/import")
async def import_portfolio(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
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
                user_id=current_user.id, # Isolation
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
    current_user: User = Depends(get_current_user)
):
    symbol = symbol.upper().strip()
    existing = await db.execute(select(StockMaster).where(
        StockMaster.symbol == symbol, 
        StockMaster.user_id == current_user.id
    ))
    stock = existing.scalars().first()

    if stock:
        if stock.type == "PORTFOLIO" and stock.is_active:
            raise HTTPException(status_code=409, detail=f"{symbol} already in portfolio")
        stock.type = "PORTFOLIO"
        stock.is_active = True
    else:
        stock = StockMaster(
            user_id=current_user.id, # Isolation
            symbol=symbol,
            company_name=company_name,
            sector=sector,
            type="PORTFOLIO",
            added_date=date.today(),
            buy_date=date.today(),
            is_active=True,
        )
        db.add(stock)

    await db.commit()
    return {"message": f"{symbol} added to portfolio"}


@app.delete("/api/portfolio/{symbol}")
async def remove_from_portfolio(symbol: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    result = await db.execute(select(StockMaster).where(
        StockMaster.symbol == symbol.upper(),
        StockMaster.user_id == current_user.id
    ))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    stock.is_active = False
    await db.commit()
    return {"message": f"{symbol} removed"}


@app.post("/api/portfolio/allocate")
async def allocate_portfolio(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    amount = float(payload.get("amount", 10000))
    limit = payload.get("limit")
    strategy = payload.get("strategy", "AI_PULSE").upper()
    # #9: Target-based strategies — read slider values from payload
    target_volatility = float(payload["target_volatility"]) / 100 if payload.get("target_volatility") else None
    target_return = float(payload["target_return"]) / 100 if payload.get("target_return") else None

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than 0")

    # 1. Fetch active portfolio stocks
    result = await db.execute(
        select(StockMaster, SignalsCache)
        .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.user_id == current_user.id)
        .where(StockMaster.type == "PORTFOLIO", StockMaster.is_active == True)
    )
    rows = result.all()
    if not rows:
        raise HTTPException(status_code=400, detail="No active portfolio stocks found")

    # 2. Extract symbols & scores for basic filtering
    portfolio_data = []
    for stock, signal in rows:
        portfolio_data.append({
            "symbol": stock.symbol,
            "sector": stock.sector,
            "composite_score": float(signal.composite_score) if signal and signal.composite_score else 50.0,
            "current_price": float(signal.current_price) if signal and signal.current_price else 1.0,
            "signal": _serialize_signal(signal) if signal else None
        })

    # Sort by score and applying limit
    portfolio_data.sort(key=lambda x: x["composite_score"], reverse=True)
    if limit is not None and str(limit).strip():
        try:
            limit_int = int(limit)
            if limit_int > 0:
                portfolio_data = portfolio_data[:limit_int]
        except ValueError: pass

    symbols = [p["symbol"] for p in portfolio_data]
    current_prices = {p["symbol"]: p["current_price"] for p in portfolio_data}
    ai_scores = {p["symbol"]: p["composite_score"] for p in portfolio_data}
    sector_map = {p["symbol"]: (p["sector"] or "Unknown") for p in portfolio_data}

    # 3a. Fetch market caps from FundamentalsCache for true Black-Litterman prior
    market_caps = {}
    fund_result = await db.execute(
        select(FundamentalsCache.symbol, FundamentalsCache.market_cap)
        .where(FundamentalsCache.symbol.in_(symbols))
    )
    for row in fund_result.all():
        if row.market_cap:
            market_caps[row.symbol] = float(row.market_cap)

    # 3b. Fetch current holdings weights for rebalancing transaction cost penalty
    prev_weights = {}
    tx_result = await db.execute(
        select(PortfolioTransaction)
        .where(
            PortfolioTransaction.user_id == current_user.id,
            PortfolioTransaction.symbol.in_(symbols),
            PortfolioTransaction.status == "OPEN"
        )
    )
    tx_rows = tx_result.scalars().all()
    if tx_rows:
        total_tx_value = sum(
            float(t.quantity) * current_prices.get(t.symbol, 1.0)
            for t in tx_rows
        )
        if total_tx_value > 0:
            for t in tx_rows:
                sym_value = float(t.quantity) * current_prices.get(t.symbol, 1.0)
                prev_weights[t.symbol] = sym_value / total_tx_value

    # 4. Preparation for Math Strategies: Fetch 10-year returns matrix if needed
    returns_df = pd.DataFrame()
    lookback_days = 0
    if strategy != "AI_PULSE":
        lookback_days = 365 * 10
        start_date = date.today() - timedelta(days=lookback_days)

        history_result = await db.execute(
            select(PriceHistory.symbol, PriceHistory.date, PriceHistory.close)
            .where(PriceHistory.symbol.in_(symbols))
            .where(PriceHistory.date >= start_date)
            .order_by(PriceHistory.date.asc())
        )
        history_rows = history_result.all()
        if history_rows:
            raw_df = pd.DataFrame(history_rows, columns=["symbol", "date", "close"])
            raw_df["close"] = raw_df["close"].astype(float)
            pivot_df = raw_df.pivot(index="date", columns="symbol", values="close").ffill()
            returns_df = pivot_df.pct_change().dropna(how="all")

    # 4b. Fetch Nifty50 prices for CAPM-based BL prior (#13)
    nifty_prices = pd.Series(dtype=float)
    if strategy in ("BLACK_LITTERMAN",) and not returns_df.empty:
        nifty_result = await db.execute(
            select(PriceHistory.date, PriceHistory.close)
            .where(PriceHistory.symbol == "NIFTY50")
            .where(PriceHistory.date >= (date.today() - timedelta(days=lookback_days)))
            .order_by(PriceHistory.date.asc())
        )
        nifty_rows = nifty_result.all()
        if nifty_rows:
            nifty_prices = pd.Series(
                {row.date: float(row.close) for row in nifty_rows}
            )

    # 5. Invoke Allocation Engine with full context
    allocation_result = calculate_allocation(
        strategy=strategy,
        amount=amount,
        returns_df=returns_df,
        ai_scores=ai_scores,
        current_prices=current_prices,
        market_caps=market_caps,
        sector_map=sector_map,
        prev_weights=prev_weights,
        nifty_prices=nifty_prices,
        target_volatility=target_volatility,
        target_return=target_return,
    )

    # 6. Save results to db
    metrics = allocation_result.get("metrics", {})
    log_entry = AllocationLog(
        user_id=current_user.id,
        total_amount=amount,
        allocation_type=allocation_result.get("strategy", strategy),
        allocations=allocation_result.get("allocations", []),
        lookback_days=lookback_days if not returns_df.empty else 0,
        expected_return=metrics.get("expected_return"),
        expected_volatility=metrics.get("expected_volatility"),
        expected_sharpe=metrics.get("expected_sharpe")
    )
    db.add(log_entry)
    await db.commit()

    return allocation_result


# ── Watchlist ─────────────────────────────────────────────────────────────────
@app.get("/api/watchlist")
async def get_watchlist(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(StockMaster, SignalsCache)
        .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.user_id == current_user.id) # Isolation
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
    current_user: User = Depends(get_current_user)
):
    symbol = symbol.upper().strip()
    existing = await db.execute(select(StockMaster).where(
        StockMaster.symbol == symbol,
        StockMaster.user_id == current_user.id
    ))
    stock = existing.scalars().first()

    if stock:
        if stock.type == "WATCHLIST" and stock.is_active:
            raise HTTPException(status_code=409, detail=f"{symbol} already in watchlist")
        stock.type = "WATCHLIST"
        stock.is_active = True
    else:
        name = company_name or symbol
        stock = StockMaster(
            user_id=current_user.id, # Isolation
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
async def remove_from_watchlist(symbol: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    result = await db.execute(select(StockMaster).where(
        StockMaster.symbol == symbol.upper(),
        StockMaster.user_id == current_user.id
    ))
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


@app.get("/api/symbols")
async def get_all_symbols(db: AsyncSession = Depends(get_db)):
    """Fetch all unique NSE symbols available in the price history table."""
    result = await db.execute(
        select(PriceHistory.symbol)
        .where(PriceHistory.exchange == 'NSE')
        .distinct()
        .order_by(PriceHistory.symbol)
    )
    return result.scalars().all()


# ── Opportunities ─────────────────────────────────────────────────────────────
@app.get("/api/opportunities")
async def get_opportunities(
    signal: str = None,
    min_confidence: float = 0,
    limit: int = 20,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    query = (
        select(StockMaster, SignalsCache)
        .join(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.user_id == current_user.id)
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
async def get_stock_history(
    symbol: str, 
    days: int = 365, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
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
    
    # ── Dynamically append LIVE candles from IntradayTicks ──
    from sqlalchemy import func
    from itertools import groupby
    
    last_hist_date = history[-1].date if history else None
    if last_hist_date:
        from datetime import datetime, time
        # Start from the beginning of the next day after last_hist_date
        next_day_start = datetime.combine(last_hist_date + timedelta(days=1), time.min)
        intra_res = await db.execute(
            select(IntradayTicks)
            .where(IntradayTicks.symbol == symbol.upper())
            .where(IntradayTicks.timestamp >= next_day_start)
            .order_by(IntradayTicks.timestamp.asc())
        )
        intra_ticks = intra_res.scalars().all()
        
        if intra_ticks:
            # Group ticks by day to handle multiple missing days (e.g. over weekend/holidays)
            for t_date, ticks_iter in groupby(intra_ticks, key=lambda t: t.timestamp.date()):
                day_ticks = list(ticks_iter)
                records.append({
                    "date": str(t_date),
                    "open": float(day_ticks[0].open) if day_ticks[0].open is not None else float(day_ticks[0].close),
                    "high": float(max((t.high if t.high is not None else t.close) for t in day_ticks)),
                    "low": float(min((t.low if t.low is not None else t.close) for t in day_ticks)),
                    "close": float(day_ticks[-1].close),
                    "volume": sum((t.volume or 0) for t in day_ticks)
                })

    # Slice to requested days
    return records[-days:] if days < len(records) else records


@app.get("/api/stock/{symbol}/intraday")
async def get_stock_intraday(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Return 5-minute OHLCV candles from IntradayTicks.
    Uses today's ticks if available, otherwise falls back to the most recent date that has ticks."""
    from datetime import date as dt_date, datetime, timedelta

    def day_bounds(d):
        """Naive datetime bounds for a date — matches DB storage (IST naive)."""
        return datetime(d.year, d.month, d.day, 0, 0, 0), datetime(d.year, d.month, d.day, 23, 59, 59)

    today = dt_date.today()
    today_start, today_end = day_bounds(today)

    result = await db.execute(
        select(IntradayTicks)
        .where(IntradayTicks.symbol == symbol.upper())
        .where(IntradayTicks.timestamp >= today_start)
        .where(IntradayTicks.timestamp <= today_end)
        .order_by(IntradayTicks.timestamp.asc())
    )
    ticks = result.scalars().all()

    # Fall back to the most recent date that actually has ticks
    if not ticks:
        latest_result = await db.execute(
            select(IntradayTicks)
            .where(IntradayTicks.symbol == symbol.upper())
            .order_by(IntradayTicks.timestamp.desc())
            .limit(1)
        )
        latest_tick = latest_result.scalars().first()
        if not latest_tick:
            return []
        latest_ts = latest_tick.timestamp
        # Strip tzinfo if present so .date() is consistent
        if hasattr(latest_ts, 'tzinfo') and latest_ts.tzinfo is not None:
            import pytz
            latest_ts = latest_ts.astimezone(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None)
        latest_date = latest_ts.date()
        lb_start, lb_end = day_bounds(latest_date)
        result2 = await db.execute(
            select(IntradayTicks)
            .where(IntradayTicks.symbol == symbol.upper())
            .where(IntradayTicks.timestamp >= lb_start)
            .where(IntradayTicks.timestamp <= lb_end)
            .order_by(IntradayTicks.timestamp.asc())
        )
        ticks = result2.scalars().all()
        if not ticks:
            return []

    # Aggregate into 5-minute buckets
    candles = []
    bucket_start = None
    bucket = []

    def to_ist_naive(ts):
        """DB stores naive IST datetimes — return as-is; strip tzinfo if somehow present."""
        if ts.tzinfo is None:
            return ts
        import pytz
        return ts.astimezone(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None)

    def flush_bucket(b_start, b_ticks):
        return {
            "time": b_start.strftime("%Y-%m-%dT%H:%M:%S"),
            "open":   float(b_ticks[0].open  or b_ticks[0].close),
            "high":   float(max((float(t.high or t.close)) for t in b_ticks)),
            "low":    float(min((float(t.low  or t.close)) for t in b_ticks)),
            "close":  float(b_ticks[-1].close),
            "volume": sum((t.volume or 0) for t in b_ticks),
        }

    from datetime import timedelta
    for tick in ticks:
        ts = to_ist_naive(tick.timestamp)
        # Floor to 5-min boundary
        floored = ts.replace(second=0, microsecond=0)
        floored = floored - timedelta(minutes=floored.minute % 5)
        if bucket_start is None:
            bucket_start = floored
        if floored != bucket_start:
            candles.append(flush_bucket(bucket_start, bucket))
            bucket_start = floored
            bucket = []
        bucket.append(tick)

    if bucket:
        candles.append(flush_bucket(bucket_start, bucket))

    return candles


@app.get("/api/stock/{symbol}/signals")
async def get_stock_signals(
    symbol: str, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
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
async def get_stock_fundamentals(
    symbol: str, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    result = await db.execute(
        select(FundamentalsCache).where(FundamentalsCache.symbol == symbol.upper())
    )
    fund = result.scalars().first()
    
    stock_res = await db.execute(select(StockMaster.yahoo_symbol, StockMaster.screener_symbol).where(StockMaster.symbol == symbol.upper()))
    stock_row = stock_res.first()
    yahoo_sym = stock_row[0] if stock_row else None
    screener_sym = stock_row[1] if stock_row else None

    # We now pull NSE enrichment directly from the DB cache (populated during manual sync)
    nse_trade_info = fund.nse_data if fund else None

    return {
        "fetched_at": str(fund.fetched_at) if fund else str(datetime.now()),
        "pe_ratio": float(fund.pe_ratio) if fund and fund.pe_ratio else None,
        "eps": float(fund.eps) if fund and fund.eps else None,
        "roe": float(fund.roe) if fund and fund.roe else None,
        "debt_equity": float(fund.debt_equity) if fund and fund.debt_equity else None,
        "revenue_growth": float(fund.revenue_growth) if fund and fund.revenue_growth else None,
        "market_cap": int(fund.market_cap) if fund and fund.market_cap else None,
        
        # -- Institutional Upgrade --
        "peg_ratio": float(fund.peg_ratio) if fund and fund.peg_ratio else None,
        "ps_ratio": float(fund.ps_ratio) if fund and fund.ps_ratio else None,
        "pb_ratio": float(fund.pb_ratio) if fund and fund.pb_ratio else None,
        "ev_ebitda": float(fund.ev_ebitda) if fund and fund.ev_ebitda else None,
        "book_value": float(fund.book_value) if fund and fund.book_value else None,
        "ebitda": fund.ebitda if fund else None,
        "held_percent_institutions": float(fund.held_percent_institutions) if fund and fund.held_percent_institutions else None,
        "shares_outstanding": fund.shares_outstanding if fund else None,
        
        # -- Phase 3: Health & Sentiment --
        "analyst_rating": float(fund.analyst_rating) if fund and fund.analyst_rating else None,
        "recommendation_key": fund.recommendation_key if fund else None,
        "total_cash": fund.total_cash if fund else None,
        "total_debt": fund.total_debt if fund else None,
        "current_ratio": float(fund.current_ratio) if fund and fund.current_ratio else None,
        
        "data_quality": fund.data_quality if fund else "UNAVAILABLE",
        "yahoo_symbol": yahoo_sym,
        "screener_symbol": screener_sym,
        "nse_trade_info": nse_trade_info
    }


@app.get("/api/stock/{symbol}/lots")
async def get_stock_lots(symbol: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    result = await db.execute(
        select(PortfolioTransaction)
        .where(
            PortfolioTransaction.symbol == symbol.upper(), 
            PortfolioTransaction.status == 'OPEN',
            PortfolioTransaction.user_id == current_user.id
        )
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

@app.post("/api/stock/{symbol}/lots")
async def add_stock_lot(
    symbol: str, 
    data: dict = Body(...), 
    db: AsyncSession = Depends(get_db), 
    current_user: User = Depends(get_current_user)
):
    try:
        qty = float(data.get("quantity", 0))
        buy_price = float(data.get("buy_price", 0))
        buy_date_str = data.get("buy_date")
        
        if qty <= 0 or buy_price <= 0:
            raise ValueError("Quantity and Buy Price must be greater than 0")

        if not buy_date_str:
            buy_date_val = date.today()
        else:
            from datetime import datetime
            buy_date_val = datetime.strptime(buy_date_str, "%Y-%m-%d").date()

        new_lot = PortfolioTransaction(
            user_id=current_user.id,
            symbol=symbol.upper(),
            quantity=qty,
            buy_price=buy_price,
            buy_date=buy_date_val,
            status="OPEN"
        )
        db.add(new_lot)
        
        # Ensure stock exists in StockMaster so it shows in portfolio views
        existing_stock = await db.execute(select(StockMaster).where(
            StockMaster.symbol == symbol.upper(), 
            StockMaster.user_id == current_user.id
        ))
        if not existing_stock.scalars().first():
            new_stock = StockMaster(
                user_id=current_user.id,
                symbol=symbol.upper(),
                company_name=symbol.upper(),
                is_active=True
            )
            db.add(new_stock)

        await db.commit()
        return {"status": "success", "id": new_lot.id}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))



from backend.utils.limiter import limiter

@app.post("/api/stock/{symbol}/chart_chat")
@limiter.limit("10/minute")
async def handle_chart_chat(
    symbol: str,
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Handles an interactive chat about the chart, utilizing historical OHLC and Signals as context."""
    chat_history = payload.get("messages", [])
    chart_range  = payload.get("range", "3M")  # frontend passes selected timeframe
    symbol = symbol.upper()

    # Map range to bar count for context window
    range_bars = {"5M": 0, "1W": 7, "1M": 21, "3M": 63, "6M": 126, "1Y": 252, "ALL": 500}
    context_bars = range_bars.get(chart_range, 63)

    # Get signals context
    sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = sig_result.scalars().first()
    
    # Get history scoped to selected range (min 30 bars for S/R accuracy)
    fetch_bars = max(context_bars, 30) if context_bars > 0 else 90
    hist_result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.desc())
        .limit(fetch_bars)
    )
    history = hist_result.scalars().all()
    history.reverse() # chronological order

    # Get today's true intraday OHLCV from IntradayTicks (the authoritative live source)
    from sqlalchemy import func, cast, Date as SADate
    from datetime import date as dt_date
    today_date = dt_date.today()

    # Step 1: latest close price from most recent tick today
    latest_tick_result = await db.execute(
        select(IntradayTicks.close, IntradayTicks.timestamp)
        .where(
            IntradayTicks.symbol == symbol,
            func.date(IntradayTicks.timestamp) == today_date,
        )
        .order_by(IntradayTicks.timestamp.desc())
        .limit(1)
    )
    latest_tick = latest_tick_result.first()

    # Step 2: aggregate OHLV for today
    agg_result = await db.execute(
        select(
            func.min(IntradayTicks.open).label("open"),
            func.max(IntradayTicks.high).label("high"),
            func.min(IntradayTicks.low).label("low"),
            func.sum(IntradayTicks.volume).label("volume"),
        )
        .where(
            IntradayTicks.symbol == symbol,
            func.date(IntradayTicks.timestamp) == today_date,
        )
    )
    agg_row = agg_result.first()

    # Build intraday snapshot: live close from latest tick, OHLV from aggregates
    live_close = float(latest_tick.close) if latest_tick and latest_tick.close else None
    intraday_open   = float(agg_row.open)   if agg_row and agg_row.open   else None
    intraday_high   = float(agg_row.high)   if agg_row and agg_row.high   else None
    intraday_low    = float(agg_row.low)    if agg_row and agg_row.low    else None
    intraday_volume = int(agg_row.volume)   if agg_row and agg_row.volume else None

    # Aggregate 90 bars into compact summary to minimize AI token cost
    closes = [float(h.close) for h in history]
    highs  = [float(h.high)  for h in history]
    lows   = [float(h.low)   for h in history]
    vols   = [int(h.volume)  for h in history]

    def sma(series, n):
        return round(sum(series[-n:]) / min(len(series), n), 2) if series else None

    def calc_live_rsi(prices: list[float], period: int = 14) -> float | None:
        """Wilder smoothed RSI matching frontend RSIChart.jsx exactly."""
        if len(prices) < period + 1:
            return None
        gains, losses = 0.0, 0.0
        for i in range(1, period + 1):
            diff = prices[i] - prices[i - 1]
            gains  += max(diff, 0)
            losses += max(-diff, 0)
        avg_gain = gains / period
        avg_loss = losses / period
        for i in range(period + 1, len(prices)):
            diff = prices[i] - prices[i - 1]
            avg_gain = (avg_gain * (period - 1) + max(diff, 0))  / period
            avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

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
        "chart_range": chart_range,
        "period_days": len(history),
        "period_start_close": round(closes[0], 2) if closes else None,
        "period_end_close":   round(closes[-1], 2) if closes else None,
        "period_change_pct":  round((closes[-1] - closes[0]) / closes[0] * 100, 2) if closes else None,
        "range_high": round(max(highs), 2) if highs else None,
        "range_low":  round(min(lows), 2) if lows else None,
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
            "ta": {**(sig.ta_breakdown or {}), "rsi": calc_live_rsi(closes)},  # rsi overridden with live-computed value matching chart
            "fa": sig.fa_breakdown or {},
            "momentum": sig.momentum_breakdown or {},
        },
        # ── TODAY's live intraday snapshot — use these exact values, do NOT guess ──
        "today": {
            "date": str(today_date),
            "open":      intraday_open,
            "high":      intraday_high,
            "low":       intraday_low,
            "close":     live_close or (float(sig.current_price) if sig and sig.current_price else None),
            "prev_close": float(sig.prev_close) if sig and sig.prev_close else None,
            "change_pct": (
                round((live_close - float(sig.prev_close)) / float(sig.prev_close) * 100, 2)
                if live_close and sig and sig.prev_close and float(sig.prev_close) > 0
                else (float(sig.change_pct) if sig and sig.change_pct else None)
            ),
            "volume":    intraday_volume,
        },
        "price_summary": price_summary,  # aggregated — replaces raw 90-bar dump
        "backtest": {
            "cagr": float(sig.backtest_cagr) if sig and sig.backtest_cagr else None,
            "win_rate": float(sig.backtest_win_rate) if sig and sig.backtest_win_rate else None,
            "sharpe": float(sig.backtest_sharpe) if sig and sig.backtest_sharpe else None,
            "max_drawdown": float(sig.backtest_max_drawdown) if sig and sig.backtest_max_drawdown else None,
            "avg_return": float(sig.backtest_avg_return) if sig and sig.backtest_avg_return else None,
        },
    }

    try:
        response = await generate_chart_chat(symbol, chat_history, context_data, user_id=current_user.id)
        return response
    except Exception as e:
        logger.error(f"Chart Chat Error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{symbol}/patterns")
@limiter.limit("6/minute")
async def handle_pattern_recognition(
    symbol: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Silently called on stock page load.
    Returns active chart patterns detected by AI from last 90 bars.
    Results are cached in SignalsCache.pattern_data (JSON) for 4 hours.
    """
    symbol = symbol.upper()

    # 4-hour cache check via SignalsCache.pattern_data
    sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = sig_result.scalars().first()

    if sig and sig.pattern_data:
        try:
            cached = json.loads(sig.pattern_data) if isinstance(sig.pattern_data, str) else sig.pattern_data
            cached_at = cached.get("cached_at")
            if cached_at:
                age = (datetime.utcnow() - datetime.fromisoformat(cached_at)).total_seconds()
                if age < 14400:
                    # Validate cached double bottom / double top patterns against actual price data
                    # to reject false positives where troughs/peaks differ by more than 3%
                    _invalidate = False
                    _cached_patterns = cached.get("patterns", [])
                    for _p in _cached_patterns:
                        _pname = (_p.get("name") or "").lower()
                        if "double bottom" in _pname or "double top" in _pname:
                            _tls = _p.get("trend_lines") or []
                            _level_prices = [tl.get("start_price") for tl in _tls if tl.get("start_price")]
                            _level_prices += [tl.get("end_price") for tl in _tls if tl.get("end_price")]
                            if len(_level_prices) >= 2:
                                _lo = min(_level_prices)
                                _hi = max(_level_prices)
                                # If the recorded levels span >3% apart, treat as false positive
                                if _lo > 0 and (_hi - _lo) / _lo > 0.03:
                                    _invalidate = True
                                    break
                            else:
                                # No trend lines to validate — fetch raw lows and check
                                # If 90-bar low and the median low differ by >3%, likely one trough
                                _quick_hist = await db.execute(
                                    select(PriceHistory)
                                    .where(PriceHistory.symbol == symbol)
                                    .order_by(PriceHistory.date.desc())
                                    .limit(90)
                                )
                                _qh = list(reversed(_quick_hist.scalars().all()))
                                if _qh:
                                    _all_lows = sorted([float(h.low) for h in _qh])
                                    _min_low = _all_lows[0]
                                    _p10_low = _all_lows[len(_all_lows) // 10]
                                    if _min_low > 0 and (_p10_low - _min_low) / _min_low > 0.03:
                                        _invalidate = True
                    if not _invalidate:
                        return cached
                    # Invalidate and fall through to fresh AI scan
                    sig.pattern_data = None
                    await db.commit()
                    logger.info(f"Pattern cache invalidated for {symbol}: double bottom/top failed price validation")
        except Exception:
            pass

    # Build same context as chart_chat
    hist_result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.desc())
        .limit(90)
    )
    history = hist_result.scalars().all()
    history.reverse()

    if len(history) < 20:
        return {"patterns": [], "summary": "Insufficient data for pattern detection."}

    closes = [float(h.close) for h in history]
    highs  = [float(h.high)  for h in history]
    lows   = [float(h.low)   for h in history]

    def sma(series, n):
        return round(sum(series[-n:]) / min(len(series), n), 2) if series else None

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

    from datetime import date as dt_date
    today_date = dt_date.today()

    context_data = {
        "current_st_signal": sig.st_signal if sig else "HOLD",
        "current_lt_signal": sig.lt_signal if sig else "HOLD",
        "composite_score":   float(sig.composite_score) if sig and sig.composite_score else None,
        "today": {
            "date":  str(today_date),
            "close": float(sig.current_price) if sig and sig.current_price else closes[-1],
        },
        "price_summary": {
            "period_change_pct": round((closes[-1] - closes[0]) / closes[0] * 100, 2) if closes else None,
            "90d_high": round(max(highs), 2),
            "90d_low":  round(min(lows),  2),
            "sma_20":   sma(closes, 20),
            "sma_50":   sma(closes, 50),
            "sma_90":   sma(closes, 90),
            "bb_upper": round((lambda c20, mean: mean + 2*(sum((x-mean)**2 for x in c20)/20)**0.5)(closes[-20:], sum(closes[-20:])/20), 2) if len(closes) >= 20 else None,
            "bb_lower": round((lambda c20, mean: mean - 2*(sum((x-mean)**2 for x in c20)/20)**0.5)(closes[-20:], sum(closes[-20:])/20), 2) if len(closes) >= 20 else None,
            "weekly_candles":  weekly_aggregates(history),
            "recent_5_bars": [
                {
                    "date": str(h.date), "open": round(float(h.open), 2),
                    "high": round(float(h.high), 2), "low": round(float(h.low), 2),
                    "close": round(float(h.close), 2), "volume": int(h.volume)
                } for h in history[-5:]
            ],
            "daily_lows_90d": [round(float(h.low), 2) for h in history],
            "daily_dates_90d": [str(h.date) for h in history],
        },
    }

    try:
        result = await generate_pattern_recognition(symbol, context_data, user_id=current_user.id)
        result["cached_at"] = datetime.utcnow().isoformat()

        # Store in SignalsCache.pattern_data
        if sig:
            sig.pattern_data = json.dumps(result)
        # If no sig row yet, log but don't crash — cache miss is acceptable
        else:
            logger.warning(f"Pattern cache: no SignalsCache row for {symbol}, result not persisted")
        await db.commit()

        return result
    except Exception as e:
        logger.error(f"Pattern recognition endpoint error for {symbol}: {e}")
        return {"patterns": [], "summary": "Pattern detection unavailable."}


@app.delete("/api/stock/{symbol}/patterns/cache")
async def clear_pattern_cache(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Clear the pattern cache for a symbol so the next GET triggers a fresh AI scan."""
    symbol = symbol.upper()
    sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = sig_result.scalars().first()
    if sig:
        sig.pattern_data = None
        await db.commit()
    return {"cleared": True, "symbol": symbol}


@app.get("/api/stock/{symbol}/move-explanation")
@limiter.limit("10/minute")
async def handle_move_explanation(
    symbol: str,
    period: str,          # query param: week | month | year | ytd
    gain_pct: float,      # query param: the actual gain % from frontend
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Returns a cached AI explanation for why symbol moved gain_pct% in period.
    Cache TTL: 4 hours. Force-refreshes if gain_pct has changed by >2% from cached value.
    """
    symbol = symbol.upper()
    period = period.lower()

    # Check cache
    cache_result = await db.execute(
        select(MoveExplanation).where(
            and_(MoveExplanation.symbol == symbol, MoveExplanation.period == period)
        )
    )
    cached = cache_result.scalar_one_or_none()

    if cached:
        age = (datetime.utcnow() - cached.updated_at).total_seconds()
        gain_drift = abs((cached.gain_pct or 0) - gain_pct)
        if age < 14400 and gain_drift < 2.0:
            return {
                "symbol":      symbol,
                "period":      period,
                "gain_pct":    cached.gain_pct,
                "headline":    cached.headline,
                "explanation": cached.explanation,
                "catalysts":   cached.catalysts,
                "sentiment":   cached.sentiment,
                "should_act":  cached.should_act,
                "cached":      True,
            }

    # Build context — reuse same pattern as chart_chat
    sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = sig_result.scalars().first()

    hist_result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.desc())
        .limit(90)
    )
    history = hist_result.scalars().all()
    history.reverse()

    closes = [float(h.close) for h in history]
    highs  = [float(h.high)  for h in history]
    lows   = [float(h.low)   for h in history]

    def sma(series, n):
        return round(sum(series[-n:]) / min(len(series), n), 2) if series else None

    def calc_live_rsi(prices: list[float], period: int = 14) -> float | None:
        """Wilder smoothed RSI matching frontend RSIChart.jsx exactly."""
        if len(prices) < period + 1:
            return None
        gains, losses = 0.0, 0.0
        for i in range(1, period + 1):
            diff = prices[i] - prices[i - 1]
            gains  += max(diff, 0)
            losses += max(-diff, 0)
        avg_gain = gains / period
        avg_loss = losses / period
        for i in range(period + 1, len(prices)):
            diff = prices[i] - prices[i - 1]
            avg_gain = (avg_gain * (period - 1) + max(diff, 0))  / period
            avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    from datetime import date as dt_date
    context_data = {
        "current_st_signal": sig.st_signal if sig else "HOLD",
        "current_lt_signal": sig.lt_signal if sig else "HOLD",
        "composite_score":   float(sig.composite_score) if sig and sig.composite_score else None,
        "today": {"close": float(sig.current_price) if sig and sig.current_price else (closes[-1] if closes else None)},
        "price_summary": {
            "period_change_pct": round((closes[-1] - closes[0]) / closes[0] * 100, 2) if len(closes) >= 2 else None,
            "90d_high": round(max(highs), 2) if highs else None,
            "90d_low":  round(min(lows),  2) if lows else None,
            "sma_20":   sma(closes, 20),
            "sma_50":   sma(closes, 50),
            "sma_90":   sma(closes, 90),
        },
        "indicators": {
            "ta": {**(sig.ta_breakdown or {} if sig else {}), "rsi": calc_live_rsi(closes)},  # rsi overridden with live-computed value matching chart
            "fa": sig.fa_breakdown or {} if sig else {},
        },
    }

    try:
        result = await generate_move_explanation(
            symbol=symbol,
            period=period,
            gain_pct=gain_pct,
            context_data=context_data,
            user_id=current_user.id
        )

        # Upsert into cache
        if cached:
            cached.gain_pct    = gain_pct
            cached.headline    = result.get("headline")
            cached.explanation = result.get("explanation")
            cached.catalysts   = result.get("catalysts")
            cached.sentiment   = result.get("sentiment")
            cached.should_act  = result.get("should_act")
            cached.updated_at  = datetime.utcnow()
        else:
            db.add(MoveExplanation(
                symbol=symbol,
                period=period,
                gain_pct=gain_pct,
                headline=result.get("headline"),
                explanation=result.get("explanation"),
                catalysts=result.get("catalysts"),
                sentiment=result.get("sentiment"),
                should_act=result.get("should_act"),
            ))
        await db.commit()

        return {
            "symbol":      symbol,
            "period":      period,
            "gain_pct":    gain_pct,
            "headline":    result.get("headline"),
            "explanation": result.get("explanation"),
            "catalysts":   result.get("catalysts"),
            "sentiment":   result.get("sentiment"),
            "should_act":  result.get("should_act"),
            "cached":      False,
        }

    except Exception as e:
        logger.error(f"Move explanation endpoint error {symbol}/{period}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stock/{symbol}/alerts/ai")
@limiter.limit("10/minute")
async def create_ai_alert(
    symbol: str,
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Parses user natural language request and creates AI-generated price alerts.
    Payload: { message: str, context_data: dict }
    """
    symbol  = symbol.upper()
    message = payload.get("message", "")
    context = payload.get("context_data", {})

    if not message:
        raise HTTPException(status_code=400, detail="message is required")

    try:
        result = await generate_alert_levels(
            symbol=symbol,
            user_message=message,
            context_data=context,
            user_id=current_user.id
        )

        created_alerts = []
        for a in (result.get("alerts") or []):
            price_level = a.get("price_level")
            direction   = a.get("direction", "BELOW")
            if not price_level:
                continue

            alert = PriceAlert(
                user_id     = current_user.id,
                symbol      = symbol,
                alert_type  = a.get("alert_type", "CUSTOM"),
                direction   = direction,
                price_level = float(price_level),
                label       = a.get("label"),
                source      = "AI",
                ai_rationale= a.get("rationale"),
                is_active   = True,
                is_triggered= False,
            )
            db.add(alert)
            created_alerts.append({
                "alert_type":  alert.alert_type,
                "direction":   alert.direction,
                "price_level": alert.price_level,
                "label":       alert.label,
                "rationale":   alert.ai_rationale,
            })

        await db.commit()

        return {
            "reply":   result.get("reply", f"{len(created_alerts)} alert(s) set for {symbol}."),
            "alerts":  created_alerts,
            "count":   len(created_alerts),
        }

    except Exception as e:
        logger.error(f"AI alert creation error {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alerts")
@limiter.limit("20/minute")
async def get_user_alerts(
    request: Request,
    active_only: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Returns all alerts for the current user."""
    stmt = select(PriceAlert).where(PriceAlert.user_id == current_user.id)
    if active_only:
        stmt = stmt.where(PriceAlert.is_active == True)
    stmt = stmt.order_by(PriceAlert.created_at.desc())
    result = await db.execute(stmt)
    alerts = result.scalars().all()

    return [{
        "id":            a.id,
        "symbol":        a.symbol,
        "alert_type":    a.alert_type,
        "direction":     a.direction,
        "price_level":   a.price_level,
        "label":         a.label,
        "source":        a.source,
        "ai_rationale":  a.ai_rationale,
        "is_active":     a.is_active,
        "is_triggered":  a.is_triggered,
        "triggered_at":  a.triggered_at.isoformat() if a.triggered_at else None,
        "triggered_price": a.triggered_price,
        "created_at":    a.created_at.isoformat(),
    } for a in alerts]


@app.delete("/api/alerts/{alert_id}")
@limiter.limit("20/minute")
async def delete_alert(
    alert_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deletes (deactivates) an alert. Only owner can delete."""
    result = await db.execute(
        select(PriceAlert).where(
            and_(PriceAlert.id == alert_id, PriceAlert.user_id == current_user.id)
        )
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.is_active = False
    await db.commit()
    return {"deleted": True, "id": alert_id}


@app.post("/api/stock/{symbol}/skill_chat")
@limiter.limit("8/minute")
async def handle_skill_chat(
    symbol: str,
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Chat through a specific investment skill persona.
    Payload: { skill_id: str, messages: list }
    Same context pipeline as chart_chat — full OHLC + signals + news.
    """
    symbol   = symbol.upper()
    skill_id = payload.get("skill_id", "")
    chat_history = payload.get("messages", [])
    chart_range  = payload.get("range", "3M")

    if not skill_id:
        raise HTTPException(status_code=400, detail="skill_id is required")

    # Map range to bar count
    range_bars = {"5M": 0, "1W": 7, "1M": 21, "3M": 63, "6M": 126, "1Y": 252, "ALL": 500}
    context_bars = range_bars.get(chart_range, 63)
    fetch_bars = max(context_bars, 30) if context_bars > 0 else 90

    # Build full context — identical pipeline to chart_chat
    sig_result = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = sig_result.scalars().first()

    hist_result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.symbol == symbol)
        .order_by(PriceHistory.date.desc())
        .limit(fetch_bars)
    )
    history = hist_result.scalars().all()
    history.reverse()

    closes = [float(h.close) for h in history]
    highs  = [float(h.high)  for h in history]
    lows   = [float(h.low)   for h in history]
    vols   = [int(h.volume)  for h in history]

    def sma(series, n):
        return round(sum(series[-n:]) / min(len(series), n), 2) if series else None

    def calc_live_rsi(prices: list[float], period: int = 14) -> float | None:
        """Wilder smoothed RSI matching frontend RSIChart.jsx exactly."""
        if len(prices) < period + 1:
            return None
        gains, losses = 0.0, 0.0
        for i in range(1, period + 1):
            diff = prices[i] - prices[i - 1]
            gains  += max(diff, 0)
            losses += max(-diff, 0)
        avg_gain = gains / period
        avg_loss = losses / period
        for i in range(period + 1, len(prices)):
            diff = prices[i] - prices[i - 1]
            avg_gain = (avg_gain * (period - 1) + max(diff, 0))  / period
            avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

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

    from datetime import date as dt_date
    today_date = dt_date.today()

    # Fetch fundamentals for skill context (especially important for Buffett/SEBI skills)
    fund_result = await db.execute(
        select(FundamentalsCache).where(FundamentalsCache.symbol == symbol)
    )
    fund = fund_result.scalar_one_or_none()

    fa_data = {}
    if fund:
        fa_data = {
            "pe_ratio":           float(fund.pe_ratio)           if fund.pe_ratio           else None,
            "pe_5yr_avg":         float(fund.pe_5yr_avg)         if fund.pe_5yr_avg         else None,
            "roe":                float(fund.roe)                if fund.roe                else None,
            "roe_3yr_avg":        float(fund.roe_3yr_avg)        if fund.roe_3yr_avg        else None,
            "debt_equity":        float(fund.debt_equity)        if fund.debt_equity        else None,
            "revenue_growth_3yr": float(fund.revenue_growth_3yr) if fund.revenue_growth_3yr else None,
            "pat_growth_3yr":     float(fund.pat_growth_3yr)     if fund.pat_growth_3yr     else None,
            "operating_margin":   float(fund.operating_margin)   if fund.operating_margin   else None,
            "promoter_holding":   float(fund.promoter_holding)   if fund.promoter_holding   else None,
            "promoter_pledge_pct":float(fund.promoter_pledge_pct)if fund.promoter_pledge_pct else None,
        }

    context_data = {
        "current_st_signal": sig.st_signal if sig else "HOLD",
        "current_lt_signal": sig.lt_signal if sig else "HOLD",
        "composite_score":   float(sig.composite_score) if sig and sig.composite_score else None,
        "today": {
            "date":       str(today_date),
            "close":      float(sig.current_price) if sig and sig.current_price else (closes[-1] if closes else None),
            "change_pct": float(sig.change_pct)    if sig and sig.change_pct    else None,
        },
        "price_summary": {
            "chart_range": chart_range,
            "period_change_pct": round((closes[-1] - closes[0]) / closes[0] * 100, 2) if len(closes) >= 2 else None,
            "range_high": round(max(highs), 2) if highs else None,
            "range_low":  round(min(lows),  2) if lows  else None,
            "90d_high":    round(max(highs), 2) if highs else None,
            "90d_low":     round(min(lows),  2) if lows  else None,
            "avg_volume_90d": round(sum(vols) / len(vols)) if vols else None,
            "sma_20":  sma(closes, 20),
            "sma_50":  sma(closes, 50),
            "sma_90":  sma(closes, 90),
            "weekly_candles": weekly_aggregates(history),
            "recent_5_bars": [
                {
                    "date":   str(h.date),
                    "open":   round(float(h.open),  2),
                    "high":   round(float(h.high),  2),
                    "low":    round(float(h.low),   2),
                    "close":  round(float(h.close), 2),
                    "volume": int(h.volume)
                } for h in history[-5:]
            ],
        },
        "indicators": {
            "composite_score": float(sig.composite_score) if sig and sig.composite_score else None,
            "ta":       {**(sig.ta_breakdown or {} if sig else {}), "rsi": calc_live_rsi(closes)},  # rsi overridden with live-computed value matching chart
            "fa":       fa_data,
            "momentum": sig.momentum_breakdown or {} if sig else {},
        },
        "backtest": {
            "cagr":         float(sig.backtest_cagr)         if sig and sig.backtest_cagr         else None,
            "win_rate":     float(sig.backtest_win_rate)     if sig and sig.backtest_win_rate     else None,
            "sharpe":       float(sig.backtest_sharpe)       if sig and sig.backtest_sharpe       else None,
            "max_drawdown": float(sig.backtest_max_drawdown) if sig and sig.backtest_max_drawdown else None,
        },
    }

    try:
        response = await generate_skill_chat_response(
            symbol=symbol,
            skill_id=skill_id,
            user_message=chat_history[-1]["content"] if chat_history else "",
            chat_history=chat_history,
            context_data=context_data,
            user_id=current_user.id,
        )
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Skill chat error {symbol}/{skill_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portfolio-performance/yearly-explainer")
@limiter.limit("6/minute")
async def handle_yearly_explainer(
    year: int,
    portfolio_return: float,
    nifty_return: float,
    alpha: float,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Generates AI explanation for a specific year's portfolio performance.
    Pulls top gainers/losers for that year from PriceHistory + StockMaster.
    Cached in PerformanceCache (cache_type=YEARLY_EXPLAINER, cache_key=str(year)) — 12h TTL.
    """

    # Cache check — 12h TTL (yearly data changes slowly)
    cache_key = f"YEARLY_EXPLAINER_{year}_{current_user.id}"
    c_result = await db.execute(
        select(PerformanceCache).where(
            and_(
                PerformanceCache.user_id   == current_user.id,
                PerformanceCache.cache_type == "YEARLY_EXPLAINER",
                PerformanceCache.cache_key  == str(year),
            )
        )
    )
    cached = c_result.scalar_one_or_none()
    if cached:
        age = (datetime.utcnow() - cached.updated_at).total_seconds()
        if age < 43200:  # 12 hours
            return cached.data

    # Build holdings context — fetch portfolio symbols
    stocks_result = await db.execute(
        select(StockMaster).where(
            and_(
                StockMaster.user_id   == current_user.id,
                StockMaster.type      == "PORTFOLIO",
                StockMaster.is_active == True,
            )
        )
    )
    stocks = stocks_result.scalars().all()
    symbols = [s.symbol for s in stocks]

    # For each holding, get year start/end price to compute that year's return
    holdings_context = []
    if symbols:
        from sqlalchemy import extract
        for stock in stocks[:15]:  # cap at 15 to keep prompt manageable
            try:
                yr_result = await db.execute(
                    select(PriceHistory.date, PriceHistory.close)
                    .where(
                        and_(
                            PriceHistory.symbol == stock.symbol,
                            extract("year", PriceHistory.date) == year,
                        )
                    )
                    .order_by(PriceHistory.date.asc())
                )
                yr_rows = yr_result.all()
                if len(yr_rows) >= 2:
                    yr_start = float(yr_rows[0].close)
                    yr_end   = float(yr_rows[-1].close)
                    yr_ret   = round((yr_end - yr_start) / yr_start * 100, 2)
                    holdings_context.append({
                        "symbol":  stock.symbol,
                        "return":  yr_ret,
                        "sector":  stock.sector or "Unknown",
                    })
            except Exception:
                continue

    # Sort by return descending so AI sees best/worst clearly
    holdings_context.sort(key=lambda x: x["return"], reverse=True)

    # Sector concentration summary for macro context
    sector_counts = {}
    for s in stocks:
        sec = s.sector or "Unknown"
        sector_counts[sec] = sector_counts.get(sec, 0) + 1

    macro_context = {
        "portfolio_size":      len(symbols),
        "sector_distribution": sector_counts,
        "top_3_winners":       holdings_context[:3]  if holdings_context else [],
        "top_3_losers":        holdings_context[-3:] if len(holdings_context) >= 3 else [],
    }

    try:
        result = await generate_yearly_risk_explainer(
            year=year,
            portfolio_return=portfolio_return,
            nifty_return=nifty_return,
            alpha=alpha,
            holdings_context=holdings_context,
            macro_context=macro_context,
            user_id=current_user.id,
        )

        result["year"]             = year
        result["portfolio_return"] = portfolio_return
        result["nifty_return"]     = nifty_return
        result["alpha"]            = alpha

        # Upsert into PerformanceCache
        if cached:
            cached.data       = result
            cached.updated_at = datetime.utcnow()
        else:
            db.add(PerformanceCache(
                user_id    = current_user.id,
                cache_type = "YEARLY_EXPLAINER",
                cache_key  = str(year),
                data       = result,
                updated_at = datetime.utcnow(),
            ))
        await db.commit()

        return result

    except Exception as e:
        logger.error(f"Yearly explainer error {year}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ai-logs")
async def get_ai_logs(
    limit: int = 100,
    symbol: Optional[str] = None,
    trigger: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Return all AI insight logs for the AI Logs page, newest first."""
    query = (
        select(AIInsights)
        .where(AIInsights.user_id == current_user.id) # Isolation
        .order_by(AIInsights.generated_at.desc())
        .limit(limit)
    )
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
            "generated_at": ins.generated_at.isoformat() + "Z" if ins.generated_at else None,
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
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Return raw AI API call logs with token usage, payloads, and timing."""
    query = (
        select(AICallLog)
        .where(AICallLog.user_id == current_user.id) # Isolation
        .order_by(AICallLog.called_at.desc())
        .limit(limit)
    )
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
            "called_at": log.called_at.isoformat() + "Z" if log.called_at else None,
        }
        for log in logs
    ]


@app.get("/api/stock/{symbol}/insight")
async def get_stock_insight(
    symbol: str, 
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(AIInsights)
        .where(
            AIInsights.symbol == symbol.upper(),
            AIInsights.user_id == current_user.id # Isolation
        )
        .order_by(AIInsights.generated_at.desc())
        .limit(1)
    )
    insight = result.scalars().first()
    if not insight:
        raise HTTPException(status_code=404, detail="No insight found")

    return {
        "generated_at": insight.generated_at.isoformat() + "Z" if insight.generated_at else None,
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
    screener_symbol: Optional[str] = None

@app.post("/api/stock/{symbol}/insight/generate")
@limiter.limit("5/minute")
async def trigger_insight_generation(
    symbol: str, 
    request: Request,
    req: Optional[InsightGenerateRequest] = None,
    db: AsyncSession = Depends(get_db), 
    current_user: User = Depends(get_current_user)
):
    """Manually trigger AI insight generation for a symbol."""
    # Validate symbol exists
    result = await db.execute(select(StockMaster).where(StockMaster.symbol == symbol.upper()))
    stock = result.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail="Symbol not found in master")

    skill_id = req.skill_id if req else None
    asyncio.create_task(_generate_and_save_insight(symbol.upper(), "MANUAL", skill_id, current_user.id))
    return {"message": f"AI insight generation queued for {symbol} (Skill: {skill_id or 'General'})"}


@app.post("/api/stock/{symbol}/fundamentals/research")
@limiter.limit("3/minute")
async def trigger_fundamental_research(
    symbol: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
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
    
    # 2. Update yahoo_symbol / screener_symbol in StockMaster if provided
    if update.yahoo_symbol is not None:
        await db.execute(
            update(StockMaster)
            .where(StockMaster.symbol == symbol)
            .values(yahoo_symbol=update.yahoo_symbol)
        )
    if update.screener_symbol is not None:
        await db.execute(
            update(StockMaster)
            .where(StockMaster.symbol == symbol)
            .values(screener_symbol=update.screener_symbol)
        )
    
    await db.commit()
    
    # 3. Trigger signal recompute
    await recompute_signals_for(symbol)
    
    return {"message": f"Fundamentals updated and signals recomputed for {symbol}."}


@app.post("/api/stock/{symbol}/fundamentals/sync")
async def sync_fundamental_data(
    symbol: str, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Trigger a fresh fetch of fundamentals from Yahoo Finance."""
    from backend.data.fetcher import fetch_fundamentals
    from backend.scheduler import recompute_signals_for
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    symbol = symbol.upper()
    
    # 1. Fetch from StockMaster for Yahoo + Screener symbol mappings
    stock_res = await db.execute(select(StockMaster.yahoo_symbol, StockMaster.screener_symbol).where(StockMaster.symbol == symbol))
    stock_row = stock_res.first()
    yf_sym = stock_row[0] if stock_row else None
    screener_slug = stock_row[1] if stock_row else None
    
    # 2. Fetch from Yahoo
    from backend.data.fetcher import fetch_screener_fundamentals
    data = await fetch_fundamentals(symbol, yahoo_symbol=yf_sym)

    # 2b. Fill missing fields from Screener.in
    screener_fields = ["pe_ratio", "roe", "debt_equity", "revenue_growth_3yr", "pat_growth_3yr",
                       "operating_margin", "pe_5yr_avg", "roe_3yr_avg", "pb_ratio", "ev_ebitda",
                       "promoter_holding", "promoter_pledge_pct", "current_ratio"]
    missing = [f for f in screener_fields if not data.get(f)]
    if missing:
        screener_data = await fetch_screener_fundamentals(symbol, screener_symbol=screener_slug or None)
        for field in missing:
            if screener_data.get(field) is not None:
                data[field] = screener_data[field]
        if screener_data:
            logger.info(f"Screener.in filled {len(screener_data)} missing fields for {symbol}")

    # 2c. Fetch Enrichment from NSE (Deep dive enrichment)
    nse_trade_info = None
    try:
        from nse import NSE
        import os
        nse_sym = yf_sym or f"{symbol}.NS"
        if nse_sym.endswith(".NS"): nse_sym = nse_sym[:-3]
        
        nse_cache_dir = os.path.join(os.getcwd(), ".nse_cache")
        os.makedirs(nse_cache_dir, exist_ok=True)
        
        def _fetch():
            with NSE(download_folder=nse_cache_dir) as nse:
                q_main = nse.quote(nse_sym)
                q_trade = nse.quote(nse_sym, section='trade_info')
                meta = q_main.get('metadata', {}); pinfo = q_main.get('priceInfo', {}); sinfo = q_main.get('securityInfo', {})
                trade = q_trade.get('marketDeptOrderBook', {}).get('tradeInfo', {})
                dp = q_trade.get('securityWiseDP', {})
                return {
                    "delivery_pct": dp.get("deliveryToTradedQuantity"),
                    "total_market_cap": trade.get("totalMarketCap"),
                    "ff_market_cap": trade.get("ffmc"),
                    "high_52w": pinfo.get("weekHighLow", {}).get("max"),
                    "low_52w": pinfo.get("weekHighLow", {}).get("min"),
                    "upper_circuit": pinfo.get("upperCP"),
                    "lower_circuit": pinfo.get("lowerCP"),
                    "is_fno": sinfo.get("isFNOSec"),
                    "listing_date": meta.get("listingDate"),
                    "industry": meta.get("industry"),
                    "isin": meta.get("isin"),
                    "delivery_qty": dp.get("deliveryQuantity"),
                    "qty_traded": dp.get("quantityTraded"),
                    "series": meta.get("series")
                }
        nse_trade_info = await asyncio.to_thread(_fetch)
        logger.info(f"NSE Enrichment sync'd for {symbol}")
    except Exception as e:
        logger.warning(f"NSE sync failed for {symbol}: {e}")

    # Recompute quality after merge
    required_keys = ["pe_ratio", "eps", "roe", "debt_equity", "revenue_growth"]
    missing_count = sum(1 for k in required_keys if not data.get(k))
    if missing_count == 0:
        data["data_quality"] = "FULL"
    elif missing_count < len(required_keys):
        data["data_quality"] = "PARTIAL"

    # 3. Persist
    save_data = {
        "symbol": symbol,
        "fetched_at": datetime.now(),
        "pe_ratio": data.get("pe_ratio"),
        "eps": data.get("eps"),
        "roe": data.get("roe"),
        "debt_equity": data.get("debt_equity"),
        "revenue_growth": data.get("revenue_growth"),
        "market_cap": data.get("market_cap"),
        "revenue_growth_3yr": data.get("revenue_growth_3yr"),
        "pat_growth_3yr": data.get("pat_growth_3yr"),
        "operating_margin": data.get("operating_margin"),
        "pe_5yr_avg": data.get("pe_5yr_avg"),
        "roe_3yr_avg": data.get("roe_3yr_avg"),
        "pb_ratio": data.get("pb_ratio"),
        "ev_ebitda": data.get("ev_ebitda"),
        "peg_ratio": data.get("peg_ratio"),
        "ps_ratio": data.get("ps_ratio"),
        "book_value": data.get("book_value"),
        "ebitda": data.get("ebitda"),
        "held_percent_institutions": data.get("held_percent_institutions"),
        "shares_outstanding": data.get("shares_outstanding"),
        "analyst_rating": data.get("analyst_rating"),
        "recommendation_key": data.get("recommendation_key"),
        "total_cash": data.get("total_cash"),
        "total_debt": data.get("total_debt"),
        "current_ratio": data.get("current_ratio"),
        "promoter_holding": data.get("promoter_holding"),
        "promoter_pledge_pct": data.get("promoter_pledge_pct"),
        "data_quality": data.get("data_quality", "FULL"),
        "nse_data": nse_trade_info
    }

    stmt = mysql_insert(FundamentalsCache).values(**save_data)
    update_cols = [c for c in save_data.keys() if c != "symbol"]
    stmt = stmt.on_duplicate_key_update(**{c: stmt.inserted[c] for c in update_cols})

    await db.execute(stmt)
    await db.commit()

    # 4. Always recompute signals when data changes
    await recompute_signals_for(symbol)

    sources = "Yahoo Finance"
    if missing and screener_data:
        sources += " + Screener.in"
    return {"message": f"Fundamentals synced from {sources} for {symbol}.", "status": data.get("data_quality")}


@app.get("/api/stock/{symbol}/screener")
async def get_screener_data(symbol: str, db: AsyncSession = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Return cached Screener.in rich data for a symbol."""
    symbol = symbol.upper()
    res = await db.execute(select(ScreenerCache).where(ScreenerCache.symbol == symbol))
    row = res.scalars().first()
    if not row:
        return {"available": False}
    return {
        "available": True,
        "fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
        "roce": float(row.roce) if row.roce else None,
        "dividend_yield": float(row.dividend_yield) if row.dividend_yield else None,
        "dividend_payout_pct": float(row.dividend_payout_pct) if row.dividend_payout_pct else None,
        "face_value": float(row.face_value) if row.face_value else None,
        "book_value": float(row.book_value) if row.book_value else None,
        "market_cap_cr": float(row.market_cap_cr) if row.market_cap_cr else None,
        "promoter_holding": float(row.promoter_holding) if row.promoter_holding else None,
        "fii_holding": float(row.fii_holding) if row.fii_holding else None,
        "dii_holding": float(row.dii_holding) if row.dii_holding else None,
        "public_holding": float(row.public_holding) if row.public_holding else None,
        "promoter_pledge_pct": float(row.promoter_pledge_pct) if row.promoter_pledge_pct else None,
        "debtor_days": float(row.debtor_days) if row.debtor_days else None,
        "inventory_days": float(row.inventory_days) if row.inventory_days else None,
        "days_payable": float(row.days_payable) if row.days_payable else None,
        "cash_conversion_cycle": float(row.cash_conversion_cycle) if row.cash_conversion_cycle else None,
        "working_capital_days": float(row.working_capital_days) if row.working_capital_days else None,
        "revenue_cagr_3yr": float(row.revenue_cagr_3yr) if row.revenue_cagr_3yr else None,
        "revenue_cagr_5yr": float(row.revenue_cagr_5yr) if row.revenue_cagr_5yr else None,
        "revenue_cagr_10yr": float(row.revenue_cagr_10yr) if row.revenue_cagr_10yr else None,
        "profit_cagr_3yr": float(row.profit_cagr_3yr) if row.profit_cagr_3yr else None,
        "profit_cagr_5yr": float(row.profit_cagr_5yr) if row.profit_cagr_5yr else None,
        "profit_cagr_10yr": float(row.profit_cagr_10yr) if row.profit_cagr_10yr else None,
        "price_cagr_1yr": float(row.price_cagr_1yr) if row.price_cagr_1yr else None,
        "price_cagr_3yr": float(row.price_cagr_3yr) if row.price_cagr_3yr else None,
        "price_cagr_5yr": float(row.price_cagr_5yr) if row.price_cagr_5yr else None,
        "price_cagr_10yr": float(row.price_cagr_10yr) if row.price_cagr_10yr else None,
        "roe_avg_3yr": float(row.roe_avg_3yr) if row.roe_avg_3yr else None,
        "roe_avg_5yr": float(row.roe_avg_5yr) if row.roe_avg_5yr else None,
        "roe_avg_10yr": float(row.roe_avg_10yr) if row.roe_avg_10yr else None,
        "quarterly_results": row.quarterly_results or [],
        "annual_pnl": row.annual_pnl or [],
        "annual_balance_sheet": row.annual_balance_sheet or [],
        "annual_cashflows": row.annual_cashflows or [],
        "annual_ratios": row.annual_ratios or [],
        "shareholding_history": row.shareholding_history or [],
        "screener_pros": row.screener_pros or [],
        "screener_cons": row.screener_cons or [],
        "about_text": row.about_text,
        "sector": row.sector,
        "industry": row.industry,
    }


@app.post("/api/stock/{symbol}/fundamentals/sync-screener")
async def sync_screener_data(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Fetch fundamentals + full rich data from Screener.in.
    Merges scalars into FundamentalsCache and stores historical data in ScreenerCache."""
    from backend.data.fetcher import fetch_screener_fundamentals
    from backend.scheduler import recompute_signals_for
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    symbol = symbol.upper()

    # Resolve screener slug: use master override if set, else fall back to auto-derive
    master_res = await db.execute(select(StockMaster.screener_symbol).where(StockMaster.symbol == symbol).limit(1))
    screener_slug = master_res.scalars().first()

    screener_data = await fetch_screener_fundamentals(symbol, screener_symbol=screener_slug or None)
    if not screener_data:
        raise HTTPException(status_code=404, detail=f"Screener.in returned no data for {symbol} (slug: '{screener_slug or symbol.lower()}'). The symbol may not exist on Screener.in.")

    # ── 1. Merge scalars into FundamentalsCache ───────────────────────────
    existing = await db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == symbol))
    row = existing.scalars().first()
    fillable = ["pe_ratio", "roe", "debt_equity", "revenue_growth_3yr", "pat_growth_3yr",
                "operating_margin", "pe_5yr_avg", "roe_3yr_avg", "pb_ratio", "ev_ebitda",
                "promoter_holding", "promoter_pledge_pct", "current_ratio", "book_value",
                "peg_ratio", "ps_ratio", "ebitda", "total_cash", "total_debt"]
    updates = {}
    for field in fillable:
        current_val = getattr(row, field, None) if row else None
        if current_val is None and screener_data.get(field) is not None:
            updates[field] = screener_data[field]
    if updates:
        if row:
            for k, v in updates.items():
                setattr(row, k, v)
            required = ["pe_ratio", "roe", "debt_equity"]
            if all(getattr(row, k, None) for k in required):
                row.data_quality = "FULL"
            await db.commit()
        else:
            stmt = mysql_insert(FundamentalsCache).values(symbol=symbol, fetched_at=datetime.now(), **updates)
            stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in updates})
            await db.execute(stmt)
            await db.commit()

    # ── 2. Save rich data into ScreenerCache ─────────────────────────────
    # screener_data is a flat dict — screener_full key is not used
    sd = screener_data
    sc_res = await db.execute(select(ScreenerCache).where(ScreenerCache.symbol == symbol))
    sc_row = sc_res.scalars().first()
    sc_vals = {
        "symbol": symbol, "fetched_at": datetime.now(),
        "roce": sd.get("roce"), "dividend_yield": sd.get("dividend_yield"),
        "dividend_payout_pct": sd.get("dividend_payout_pct") or sd.get("dividend_payout"), 
        "face_value": sd.get("face_value"),
        "book_value": sd.get("book_value"),
        "market_cap_cr": sd.get("market_cap_cr") or (sd.get("market_cap") / 1e7 if sd.get("market_cap") else None),
        "promoter_holding": sd.get("promoter_holding"),
        "fii_holding": sd.get("fii_holding"), "dii_holding": sd.get("dii_holding"),
        "public_holding": sd.get("public_holding"),
        "promoter_pledge_pct": sd.get("promoter_pledge_pct"),
        "debtor_days": sd.get("debtor_days"), "inventory_days": sd.get("inventory_days"),
        "days_payable": sd.get("days_payable"), "cash_conversion_cycle": sd.get("cash_conversion_cycle"),
        "working_capital_days": sd.get("working_capital_days"),
        "revenue_cagr_3yr": sd.get("revenue_cagr_3yr") or sd.get("revenue_growth_3yr"),
        "revenue_cagr_5yr": sd.get("revenue_cagr_5yr"),
        "revenue_cagr_10yr": sd.get("revenue_cagr_10yr"),
        "profit_cagr_3yr": sd.get("profit_cagr_3yr") or sd.get("pat_growth_3yr"),
        "profit_cagr_5yr": sd.get("profit_cagr_5yr"), "profit_cagr_10yr": sd.get("profit_cagr_10yr"),
        "price_cagr_1yr": sd.get("price_cagr_1yr"), "price_cagr_3yr": sd.get("price_cagr_3yr"),
        "price_cagr_5yr": sd.get("price_cagr_5yr"), "price_cagr_10yr": sd.get("price_cagr_10yr"),
        "roe_avg_3yr": sd.get("roe_avg_3yr") or sd.get("roe_3yr_avg"),
        "roe_avg_5yr": sd.get("roe_avg_5yr"), "roe_avg_10yr": sd.get("roe_avg_10yr"),
        "quarterly_results": sd.get("quarterly_results"), "annual_pnl": sd.get("annual_pnl"),
        "annual_balance_sheet": sd.get("annual_balance_sheet"), "annual_cashflows": sd.get("annual_cashflows"),
        "annual_ratios": sd.get("annual_ratios"), "shareholding_history": sd.get("shareholding_history"),
        "screener_pros": sd.get("screener_pros"), "screener_cons": sd.get("screener_cons"),
        "about_text": sd.get("about_text"), "sector": sd.get("sector"), "industry": sd.get("industry"),
    }
    sc_clean = {k: v for k, v in sc_vals.items() if v is not None}
    if sc_row:
        for k, v in sc_clean.items():
            if k != "symbol":
                setattr(sc_row, k, v)
        await db.commit()
    else:
        stmt = mysql_insert(ScreenerCache).values(**sc_clean)
        stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in sc_clean if k != "symbol"})
        await db.execute(stmt)
        await db.commit()

    await recompute_signals_for(symbol)
    return {
        "message": f"Screener sync complete for {symbol}.",
        "fundamentals_filled": len(updates), "fundamentals_fields": list(updates.keys()),
        "rich_data_stored": bool(screener_data),
        "has_quarterly": bool(screener_data.get("quarterly_results")),
        "has_annual_pnl": bool(screener_data.get("annual_pnl")),
        "has_shareholding": bool(screener_data.get("shareholding_history")),
    }


@app.post("/api/stock/{symbol}/signals/recompute")
async def recompute_stock_signals(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
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
            # Map AI confidence to data_quality column
            confidence = ai_data.get("data_confidence", "LOW")
            # If LOW, keep as AI_RESEARCHED. If HIGH/MEDIUM, we consider it FULL coverage.
            data_quality = "AI_RESEARCHED" if confidence == "LOW" else "FULL"

            stmt = mysql_insert(FundamentalsCache).values(
                symbol=symbol,
                fetched_at=datetime.now(),
                pe_ratio=ai_data.get("pe_ratio"),
                eps=ai_data.get("eps"),
                roe=ai_data.get("roe"),
                debt_equity=ai_data.get("debt_equity"),
                revenue_growth=ai_data.get("revenue_growth"),
                market_cap=ai_data.get("market_cap"),
                # -- v2.1 Expansion --
                revenue_growth_3yr=ai_data.get("revenue_growth_3yr"),
                pat_growth_3yr=ai_data.get("pat_growth_3yr"),
                operating_margin=ai_data.get("operating_margin"),
                pe_5yr_avg=ai_data.get("pe_5yr_avg"),
                roe_3yr_avg=ai_data.get("roe_3yr_avg"),
                peg_ratio=ai_data.get("peg_ratio"),
                pb_ratio=ai_data.get("pb_ratio"),
                ev_ebitda=ai_data.get("ev_ebitda"),
                held_percent_institutions=ai_data.get("held_percent_institutions"),
                promoter_holding=ai_data.get("promoter_holding"),
                promoter_pledge_pct=ai_data.get("promoter_pledge_pct"),
                analyst_rating=ai_data.get("analyst_rating"),
                recommendation_key=ai_data.get("recommendation_key"),
                total_cash=ai_data.get("total_cash"),
                total_debt=ai_data.get("total_debt"),
                current_ratio=ai_data.get("current_ratio"),
                data_quality=data_quality
            )
            FUND_COLS = [
                "fetched_at", "pe_ratio", "eps", "roe", "debt_equity",
                "revenue_growth", "market_cap", "revenue_growth_3yr",
                "pat_growth_3yr", "operating_margin", "pe_5yr_avg",
                "roe_3yr_avg", "peg_ratio", "pb_ratio", "ev_ebitda",
                "held_percent_institutions", "promoter_holding",
                "promoter_pledge_pct", "analyst_rating", "recommendation_key",
                "total_cash", "total_debt", "current_ratio", "data_quality"
            ]
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


async def _generate_and_save_insight(symbol: str, trigger: str, skill_id: str = None, user_id: int = None):
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
                # Extended fields required by skill templates via StockMeta
                "pe_5yr_avg": float(fund.pe_5yr_avg) if fund and fund.pe_5yr_avg else None,
                "roe_3yr_avg": float(fund.roe_3yr_avg) if fund and fund.roe_3yr_avg else None,
                "revenue_growth_3yr": float(fund.revenue_growth_3yr) if fund and fund.revenue_growth_3yr else None,
                "pat_growth_3yr": float(fund.pat_growth_3yr) if fund and fund.pat_growth_3yr else None,
                "operating_margin": float(fund.operating_margin) if fund and fund.operating_margin else None,
                "promoter_holding": float(fund.promoter_holding) if fund and fund.promoter_holding else None,
                "promoter_pledge_pct": float(fund.promoter_pledge_pct) if fund and fund.promoter_pledge_pct else None,
                "market_cap": int(fund.market_cap) if fund and fund.market_cap else None,
            } if fund else {}

        insight_data = await generate_insight(symbol, df, signals, fundamentals, trigger, skill_id, company_name=company_name)

        if not insight_data:
            logger.error(f"generate_insight returned None for {symbol} — skipping save to AIInsights.")
            return

        async with SessionLocal() as db:
            new_insight = AIInsights(
                user_id=user_id, # Isolation
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
async def manual_bhavcopy_sync(req: BhavcopySyncRequest, admin_user: User = Depends(get_current_admin)):
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
async def bulk_fundamental_sync(admin_user: User = Depends(get_current_admin)):
    """Trigger a full refresh of fundamentals for all active stocks."""
    from backend.scheduler import run_bulk_fundamental_sync
    import asyncio
    
    # Run in background as it takes time
    asyncio.create_task(run_bulk_fundamental_sync())
    
    return {"message": "Bulk fundamental sync initiated in the background."}

@app.get("/api/market/bhavcopy/logs")
async def get_bhavcopy_logs(limit: int = 20, db: AsyncSession = Depends(get_db), admin_user: User = Depends(get_current_admin)):
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

@app.get("/api/admin/users")
async def list_users(db: AsyncSession = Depends(get_db), admin: User = Depends(get_current_admin)):
    result = await db.execute(select(User).order_by(User.id))
    users = result.scalars().all()
    return [{
        "id": u.id,
        "email": u.email,
        "full_name": u.full_name,
        "role": u.role,
        "is_active": u.is_active,
        "created_at": u.created_at.isoformat() if u.created_at else None
    } for u in users]

@app.patch("/api/admin/users/{user_id}")
async def update_user(user_id: int, data: dict = Body(...), db: AsyncSession = Depends(get_db), admin: User = Depends(get_current_admin)):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Prevent admin from deactivating themselves for safety
    if user.id == admin.id and "is_active" in data and not data["is_active"]:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own administrative account")

    if "role" in data:
        user.role = data["role"]
    if "is_active" in data:
        user.is_active = data["is_active"]
        
    await db.commit()
    return {"message": "User updated successfully"}

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
        "computed_at": signal.computed_at.isoformat() if signal.computed_at else None,
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
        "prev_close": float(signal.prev_close) if signal.prev_close is not None else None,
        "backtest_sharpe": float(signal.backtest_sharpe) if signal.backtest_sharpe is not None else None,
        
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

@app.get("/api/stock/{symbol}/corporate-actions")
async def get_corporate_actions(symbol: str, db: AsyncSession = Depends(get_db), current_user=Depends(get_current_user)):
    """
    Corporate actions endpoint — layered data strategy:
      Layer 0: NSE India live API — ground truth for Indian equities
                 - dividends, bonus, splits, buybacks, rights issues
                 - 6h in-memory cached to handle NSE bot-protection
      Layer 1: yf.Ticker().calendar  — official upcoming ex-date/amount if Yahoo has it
      Layer 2: yf.Ticker().actions / .dividends / .splits — full history
      Layer 3: Algorithmic prediction engine — uses history to predict next dividend
                 - detects payout frequency (annual / semi-annual / quarterly)
                 - computes avg amount per cycle, trends over last 3 cycles
                 - projects next ex-date + estimated range
                 - computes dividend CAGR, yield, consistency score
      Layer 4: info fields — exDividendDate, lastDividendValue as cross-check
    """
    from backend.data.fetcher import fetch_nse_corporate_actions
    import yfinance as yf
    import numpy as np
    from datetime import date as date_cls

    # ── Layer 0: NSE India live corporate actions ─────────────────────────────
    nse_layer0 = None
    try:
        nse_layer0 = await fetch_nse_corporate_actions(symbol)
        if nse_layer0:
            logger.info(f"NSE Layer 0: found data for {symbol} — "
                        f"div={bool(nse_layer0.get('upcoming_dividend'))}, "
                        f"bonus={bool(nse_layer0.get('upcoming_bonus'))}, "
                        f"split={bool(nse_layer0.get('upcoming_split'))}, "
                        f"buyback={bool(nse_layer0.get('upcoming_buyback'))}")
    except Exception as _nse_exc:
        logger.warning(f"NSE Layer 0 failed for {symbol}: {_nse_exc!r}")

    stock_res = await db.execute(select(StockMaster.yahoo_symbol).where(StockMaster.symbol == symbol.upper()))
    row = stock_res.first()
    yf_sym = (row.yahoo_symbol if row and row.yahoo_symbol else f"{symbol.upper()}.NS")

    def _safe(v):
        if v is None:
            return None
        try:
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                return None
        except Exception:
            pass
        if hasattr(v, 'item'):
            return v.item()
        return v

    def _to_date(v):
        """Normalize various date representations to date string."""
        if v is None:
            return None
        if isinstance(v, (int, float)):
            # Unix timestamp
            try:
                return date_cls.fromtimestamp(int(v)).isoformat()
            except Exception:
                return None
        try:
            return str(pd.Timestamp(v).date())
        except Exception:
            return str(v)

    # ── Layer 1+2: pull raw yfinance data ────────────────────────────────────
    dividend_history = []
    split_history    = []
    upcoming_dividend = None
    upcoming_split   = None
    info_ex_date     = None
    info_last_div    = None

    try:
        ticker = yf.Ticker(yf_sym)

        # Calendar — Layer 1
        try:
            cal = ticker.calendar
            if cal:
                cal_d = cal if isinstance(cal, dict) else (cal.to_dict() if hasattr(cal, 'to_dict') else {})
                # flatten one level if values are dicts
                flat = {}
                for k, v in cal_d.items():
                    flat[str(k).lower()] = list(v.values())[0] if isinstance(v, dict) and v else v
                ex_date_raw = flat.get("ex-dividend date") or flat.get("dividend date") or flat.get("exdividenddate")
                div_amt_raw = flat.get("dividends") or flat.get("dividend amount") or flat.get("lastdividendvalue")
                if ex_date_raw:
                    upcoming_dividend = {
                        "ex_date": _to_date(ex_date_raw),
                        "amount": _safe(div_amt_raw),
                        "source": "yf_calendar",
                        "confirmed": True,
                    }
                split_date_raw  = flat.get("split date") or flat.get("splitdate")
                split_ratio_raw = flat.get("split factor") or flat.get("split ratio") or flat.get("splitratio")
                if split_date_raw:
                    upcoming_split = {
                        "date": _to_date(split_date_raw),
                        "ratio": str(_safe(split_ratio_raw)) if split_ratio_raw else None,
                        "source": "yf_calendar",
                        "confirmed": True,
                    }
        except Exception:
            pass

        # Info — Layer 4 (cross-check / fallback)
        try:
            info = ticker.info or {}
            ex_raw = info.get("exDividendDate")
            if ex_raw:
                info_ex_date = _to_date(ex_raw)
            info_last_div = _safe(info.get("lastDividendValue") or info.get("dividendRate"))
            # If calendar came back empty but info has ex-date, use it
            if not upcoming_dividend and info_ex_date:
                upcoming_dividend = {
                    "ex_date": info_ex_date,
                    "amount": info_last_div,
                    "source": "yf_info",
                    "confirmed": True,
                }
        except Exception:
            pass

        # History — Layer 2
        try:
            actions = ticker.actions
            if actions is not None and not actions.empty:
                if "Dividends" in actions.columns:
                    divs = actions[actions["Dividends"] > 0]["Dividends"].sort_index(ascending=False).head(30)
                    for idx, val in divs.items():
                        dividend_history.append({"date": str(idx.date()), "amount": round(float(_safe(val)), 4)})
                if "Stock Splits" in actions.columns:
                    splits = actions[actions["Stock Splits"] > 0]["Stock Splits"].sort_index(ascending=False).head(10)
                    for idx, val in splits.items():
                        split_history.append({"date": str(idx.date()), "ratio": str(_safe(val))})
        except Exception:
            pass

        if not dividend_history:
            try:
                divs = ticker.dividends
                if divs is not None and not divs.empty:
                    for idx, val in divs.sort_index(ascending=False).head(30).items():
                        v = _safe(val)
                        if v and float(v) > 0:
                            dividend_history.append({"date": str(idx.date()), "amount": round(float(v), 4)})
            except Exception:
                pass

        if not split_history:
            try:
                splits = ticker.splits
                if splits is not None and not splits.empty:
                    for idx, val in splits.sort_index(ascending=False).head(10).items():
                        v = _safe(val)
                        if v:
                            split_history.append({"date": str(idx.date()), "ratio": str(v)})
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Corporate actions yf fetch error for {symbol}: {e}")

    # ── Layer 3: Algorithmic Prediction Engine ────────────────────────────────
    prediction = None
    analytics  = None

    try:
        if len(dividend_history) >= 2:
            # Parse dates + amounts in chronological order
            parsed = []
            for d in dividend_history:
                try:
                    parsed.append((pd.Timestamp(d["date"]), float(d["amount"])))
                except Exception:
                    pass
            parsed.sort(key=lambda x: x[0])  # oldest first

            dates   = [p[0] for p in parsed]
            amounts = [p[1] for p in parsed]

            # ── Detect payout frequency ──────────────────────────────────────
            if len(dates) >= 2:
                gaps_days = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
                median_gap = float(np.median(gaps_days))

                if median_gap < 50:
                    frequency = "monthly"
                    freq_days = 30
                elif median_gap < 120:
                    frequency = "quarterly"
                    freq_days = 91
                elif median_gap < 240:
                    frequency = "semi-annual"
                    freq_days = 182
                else:
                    frequency = "annual"
                    freq_days = 365
            else:
                frequency = "annual"
                freq_days = 365
                median_gap = 365.0

            last_date   = dates[-1]
            last_amount = amounts[-1]
            today       = pd.Timestamp.now(tz=last_date.tzinfo)

            # ── Compute analytics ────────────────────────────────────────────
            recent_3 = amounts[-3:]
            recent_5 = amounts[-5:] if len(amounts) >= 5 else amounts

            avg_amount      = round(float(np.mean(amounts)), 4)
            avg_recent      = round(float(np.mean(recent_3)), 4)
            trend_pct       = round((recent_3[-1] / recent_3[0] - 1) * 100, 2) if len(recent_3) >= 2 and recent_3[0] > 0 else 0.0

            # CAGR of dividend amount over full history
            div_cagr = None
            if len(amounts) >= 3 and amounts[0] > 0:
                years = (dates[-1] - dates[0]).days / 365.25
                if years > 0.5:
                    div_cagr = round((amounts[-1] / amounts[0]) ** (1 / years) - 1, 4) * 100

            # Consistency score: % of expected periods that had a dividend
            expected_periods = max(1, round((dates[-1] - dates[0]).days / freq_days))
            consistency_pct  = round(min(len(dates) / expected_periods * 100, 100), 1)

            # Payout gaps (was there ever a skipped year?)
            skipped = sum(1 for g in gaps_days if g > freq_days * 1.8) if len(dates) > 1 else 0

            analytics = {
                "frequency": frequency,
                "median_gap_days": round(median_gap, 1),
                "total_dividends_paid": len(amounts),
                "avg_dividend": avg_amount,
                "avg_recent_3": avg_recent,
                "last_dividend": round(last_amount, 4),
                "last_date": str(last_date.date()),
                "trend_pct": trend_pct,
                "dividend_cagr_pct": round(div_cagr, 2) if div_cagr is not None else None,
                "consistency_score": consistency_pct,
                "skipped_cycles": skipped,
                "years_of_history": round((dates[-1] - dates[0]).days / 365.25, 1) if len(dates) > 1 else 0,
            }

            # ── Predict next dividend ────────────────────────────────────────
            days_since_last = (today - last_date).days
            days_until_next = max(0, freq_days - days_since_last)
            next_date_est   = (today + pd.Timedelta(days=days_until_next)).date()

            # Estimated amount: weighted avg (recent 3 weighted 2x, trend-adjusted)
            if len(amounts) >= 3:
                weights    = [1, 1.5, 2]
                w_amounts  = amounts[-3:]
                est_amount = sum(w * a for w, a in zip(weights, w_amounts)) / sum(weights)
            else:
                est_amount = avg_amount

            # Apply trend adjustment (cap at ±20% to avoid wild extrapolation)
            if trend_pct and abs(trend_pct) < 20:
                est_amount = est_amount * (1 + trend_pct / 100 * 0.5)

            est_amount = round(est_amount, 4)
            est_low    = round(est_amount * 0.80, 4)
            est_high   = round(est_amount * 1.20, 4)

            # Confidence: higher if consistent, history > 3yrs, overdue
            confidence = 50
            if consistency_pct >= 90:
                confidence += 20
            elif consistency_pct >= 70:
                confidence += 10
            if analytics["years_of_history"] >= 5:
                confidence += 15
            elif analytics["years_of_history"] >= 3:
                confidence += 8
            if days_since_last >= freq_days * 0.85:
                confidence += 10
            if skipped > 0:
                confidence -= 15
            confidence = max(10, min(confidence, 95))

            # Only show prediction if we're not already past the predicted date
            # and if calendar doesn't already have a confirmed upcoming date
            already_confirmed = (
                upcoming_dividend is not None
                and upcoming_dividend.get("ex_date")
                and pd.Timestamp(upcoming_dividend["ex_date"]) > today
            )

            if not already_confirmed and consistency_pct >= 50:
                prediction = {
                    "next_ex_date_est": str(next_date_est),
                    "days_until_est": int(days_until_next),
                    "amount_est": est_amount,
                    "amount_range_low": est_low,
                    "amount_range_high": est_high,
                    "confidence_pct": confidence,
                    "basis": f"Based on {len(amounts)} historical dividends ({frequency} pattern)",
                    "overdue": days_since_last > freq_days * 1.1,
                }

    except Exception as e:
        logger.warning(f"Corporate actions prediction engine error for {symbol}: {e}")

    # ── Layer 0 merge: NSE overrides yfinance when more authoritative ────────
    nse_actions_payload = None
    if nse_layer0:
        # Override upcoming_dividend — NSE is ground truth for Indian equities
        if nse_layer0.get("upcoming_dividend"):
            upcoming_dividend = nse_layer0["upcoming_dividend"]
        # Override upcoming_split from NSE if present
        if nse_layer0.get("upcoming_split") and not upcoming_split:
            upcoming_split = nse_layer0["upcoming_split"]
        elif nse_layer0.get("upcoming_split"):
            # NSE is more authoritative — prefer it
            upcoming_split = nse_layer0["upcoming_split"]

        nse_actions_payload = {
            "upcoming_bonus":    nse_layer0.get("upcoming_bonus"),
            "upcoming_buyback":  nse_layer0.get("upcoming_buyback"),
            "recent_actions":    nse_layer0.get("recent_actions", []),
        }

    return {
        "symbol": symbol.upper(),
        "yahoo_symbol": yf_sym,
        "upcoming_dividend": upcoming_dividend,
        "upcoming_split": upcoming_split,
        "prediction": prediction,
        "analytics": analytics,
        "dividend_history": dividend_history,
        "split_history": split_history,
        "nse_actions": nse_actions_payload,
    }


@app.get("/api/stocks/symbols")
async def get_all_symbols(db: AsyncSession = Depends(get_db), current_user=Depends(get_current_user)):
    """Return a list of unique symbols from the StockMaster table for autocomplete."""
    result = await db.execute(
        select(StockMaster.symbol, StockMaster.company_name)
        .where(StockMaster.user_id == current_user.id)
        .distinct()
    )
    return [{"symbol": row.symbol, "name": row.company_name} for row in result]


@app.get("/api/corporate-actions")
async def get_market_corporate_actions(
    from_date: str,
    to_date: str,
    symbol: Optional[str] = None,
    force_sync: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    Fetch market-wide corporate actions. 
    By default, it reads from the local DB. If force_sync=true, it fetches from NSE and updates DB.
    """
    from datetime import datetime
    try:
        d_from = datetime.strptime(from_date, "%d-%m-%Y").date()
        d_to = datetime.strptime(to_date, "%d-%m-%Y").date()

        if force_sync:
            await sync_corporate_actions_from_nse(db, d_from, d_to, symbol)

        # Query from DB
        query = select(CorporateAction).where(
            CorporateAction.ex_date >= d_from,
            CorporateAction.ex_date <= d_to
        )
        if symbol:
            query = query.where(CorporateAction.symbol == symbol.upper().strip())
        
        query = query.order_by(CorporateAction.ex_date.asc())
        result = await db.execute(query)
        actions = result.scalars().all()
        
        # Map to expected frontend format (matches nse library keys for compatibility)
        return [{
            "symbol": a.symbol,
            "series": a.series,
            "ind": a.ind,
            "faceVal": a.face_val,
            "subject": a.subject,
            "exDate": a.ex_date.strftime("%d-%b-%Y") if a.ex_date else None,
            "recDate": a.rec_date.strftime("%d-%b-%Y") if a.rec_date else None,
            "bcStartDate": a.bc_start_date.strftime("%d-%b-%Y") if a.bc_start_date else None,
            "bcEndDate": a.bc_end_date.strftime("%d-%b-%Y") if a.bc_end_date else None,
            "ndStartDate": a.nd_start_date.strftime("%d-%b-%Y") if a.nd_start_date else None,
            "ndEndDate": a.nd_end_date.strftime("%d-%b-%Y") if a.nd_end_date else None,
            "comp": a.comp,
            "isin": a.isin,
            "caBroadcastDate": a.ca_broadcast_date.strftime("%d-%b-%Y") if a.ca_broadcast_date else None,
            "paymentDate": a.payment_date.strftime("%d-%b-%Y") if a.payment_date else None,
            "action_type": a.action_type
        } for a in actions]

    except Exception as e:
        logger.error(f"Error fetching market corporate actions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/corporate-actions/sync")
async def trigger_corporate_actions_sync(
    from_date: str,
    to_date: str,
    symbol: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_admin) # Admin only for global sync
):
    """Trigger manual sync from NSE to DB."""
    from datetime import datetime
    try:
        d_from = datetime.strptime(from_date, "%d-%m-%Y").date()
        d_to = datetime.strptime(to_date, "%d-%m-%Y").date()
        
        count = await sync_corporate_actions_from_nse(db, d_from, d_to, symbol)
        return {"status": "success", "synced_count": count}
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def sync_corporate_actions_from_nse(db: AsyncSession, from_date, to_date, symbol=None):
    from nse import NSE
    import os
    from datetime import datetime

    def _parse_nse_date(d_str):
        if not d_str or d_str == "-": return None
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try: return datetime.strptime(d_str, fmt).date()
            except: continue
        return None

    def _classify(purpose):
        p = purpose.upper()
        if "DIVIDEND" in p: return "DIVIDEND"
        if "BONUS" in p: return "BONUS"
        if "SPLIT" in p: return "SPLIT"
        if "BUYBACK" in p: return "BUYBACK"
        if "RIGHTS" in p: return "RIGHTS"
        return "OTHER"

    nse_cache_dir = os.path.join(os.getcwd(), ".nse_cache")
    os.makedirs(nse_cache_dir, exist_ok=True)

    with NSE(download_folder=nse_cache_dir) as nse:
        raw_actions = nse.actions(
            segment='equities',
            symbol=symbol if symbol and symbol.strip() else None,
            from_date=from_date,
            to_date=to_date
        )

    if not raw_actions:
        return 0

    count = 0
    for a in raw_actions:
        ex_d = _parse_nse_date(a.get("exDate"))
        if not ex_d: continue
        
        sub = a.get("subject") or ""
        
        stmt = mysql_insert(CorporateAction).values(
            symbol=a.get("symbol"),
            series=a.get("series"),
            ind=a.get("ind"),
            face_val=str(a.get("faceVal")) if a.get("faceVal") is not None else None,
            subject=sub,
            subject_short=sub[:200],
            ex_date=ex_d,
            rec_date=_parse_nse_date(a.get("recDate")),
            bc_start_date=_parse_nse_date(a.get("bcStartDate")),
            bc_end_date=_parse_nse_date(a.get("bcEndDate")),
            nd_start_date=_parse_nse_date(a.get("ndStartDate")),
            nd_end_date=_parse_nse_date(a.get("ndEndDate")),
            comp=a.get("comp"),
            isin=a.get("isin"),
            ca_broadcast_date=_parse_nse_date(a.get("caBroadcastDate")),
            payment_date=_parse_nse_date(a.get("paymentDate")),
            action_type=_classify(sub)
        )
        
        stmt = stmt.on_duplicate_key_update(
            series=stmt.inserted.series,
            ind=stmt.inserted.ind,
            face_val=stmt.inserted.face_val,
            subject=stmt.inserted.subject,
            rec_date=stmt.inserted.rec_date,
            bc_start_date=stmt.inserted.bc_start_date,
            bc_end_date=stmt.inserted.bc_end_date,
            nd_start_date=stmt.inserted.nd_start_date,
            nd_end_date=stmt.inserted.nd_end_date,
            comp=stmt.inserted.comp,
            isin=stmt.inserted.isin,
            ca_broadcast_date=stmt.inserted.ca_broadcast_date,
            payment_date=stmt.inserted.payment_date,
            action_type=stmt.inserted.action_type
        )
        await db.execute(stmt)
        count += 1
    
    await db.commit()
    return count



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
