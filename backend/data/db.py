import os
import sys
import datetime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, Numeric, BigInteger, JSON, Enum, UniqueConstraint, SmallInteger, ForeignKey, Float, Text, text
from sqlalchemy.orm import relationship

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.config import settings

# Sized for Raspberry Pi with max 5 connections
engine = create_async_engine(
    settings.async_database_url,
    pool_size=5,
    max_overflow=2,
    pool_recycle=3600
)

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    email = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(100))
    role = Column(Enum('USER', 'ADMIN'), default='USER')
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Relationships
    stocks = relationship("StockMaster", back_populates="user")
    transactions = relationship("PortfolioTransaction", back_populates="user")
    insights = relationship("AIInsights", back_populates="user")

class StockMaster(Base):
    __tablename__ = "stocks_master"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True) # Multi-user
    symbol = Column(String(20), index=True, nullable=False)
    exchange = Column(Enum('NSE', 'BSE'), default='NSE', nullable=False, index=True)
    company_name = Column(String(100), nullable=False)
    scp_name = Column(String(100))
    isin = Column(String(12), index=True)
    sector = Column(String(50))
    market_cap_cat = Column(Enum('LARGE','MID','SMALL','UNKNOWN'))
    type = Column(Enum('PORTFOLIO','WATCHLIST'), nullable=False, index=True)
    added_date = Column(Date, nullable=False)
    is_active = Column(Boolean, default=True)
    yahoo_symbol = Column(String(20), index=True)
    
    # Portfolio specific fields
    quantity = Column(Numeric(14,4))
    avg_buy_price = Column(Numeric(18,6))
    buy_date = Column(Date)

    # Relationships
    user = relationship("User", back_populates="stocks")

    __table_args__ = (
        UniqueConstraint('user_id', 'symbol', 'type', name='uix_user_symbol_type'),
    )

class PortfolioTransaction(Base):
    __tablename__ = "portfolio_transactions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True) # Multi-user
    symbol = Column(String(20), index=True, nullable=False)
    isin = Column(String(12), index=True)
    quantity = Column(Numeric(14,4), nullable=False)
    buy_price = Column(Numeric(18,6), nullable=False)
    buy_date = Column(Date, nullable=False)
    status = Column(Enum('OPEN', 'CLOSED'), default='OPEN', nullable=False)

    # Relationships
    user = relationship("User", back_populates="transactions")

class PriceHistory(Base):
    __tablename__ = "price_history"
    
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(20), index=True, nullable=False)
    exchange = Column(Enum('NSE', 'BSE'), default='NSE', nullable=False, index=True)
    isin = Column(String(12), index=True)
    date = Column(Date, index=True, nullable=False)
    open = Column(Numeric(10,2))
    high = Column(Numeric(10,2))
    low = Column(Numeric(10,2))
    close = Column(Numeric(10,2), nullable=False)
    volume = Column(BigInteger)
    no_of_trades = Column(Integer)
    source = Column(Enum('bhavcopy','yfinance_fallback','eod_computed','historical_import'))
    
    __table_args__ = (
        UniqueConstraint('symbol', 'date', 'exchange', name='uix_symbol_date_exchange'),
    )

class PerformanceCache(Base):
    __tablename__ = "performance_cache"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, index=True, nullable=False)
    cache_type = Column(String(50), index=True, nullable=False) # 'benchmark' or 'yearly'
    cache_key = Column(String(100), index=True, nullable=False)
    data = Column(JSON, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('user_id', 'cache_type', 'cache_key', name='uix_user_type_key'),
    )

class IntradayTicks(Base):
    __tablename__ = "intraday_ticks"
    
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(20), index=True, nullable=False)
    exchange = Column(Enum('NSE', 'BSE'), default='NSE', nullable=False, index=True)
    timestamp = Column(DateTime, index=True, nullable=False)
    open = Column(Numeric(10,2))
    high = Column(Numeric(10,2))
    low = Column(Numeric(10,2))
    close = Column(Numeric(10,2), nullable=False, index=True)
    volume = Column(BigInteger)

class SignalsCache(Base):
    __tablename__ = "signals_cache"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(20), index=True, nullable=False)
    exchange = Column(Enum('NSE', 'BSE'), default='NSE', nullable=False, index=True)
    computed_at = Column(DateTime, nullable=False)
    market_session = Column(Enum('LIVE','EOD','CLOSED'), nullable=False)
    current_price = Column(Numeric(10,2))
    prev_close = Column(Numeric(10,2))
    change_pct = Column(Numeric(8,4))
    st_signal = Column(Enum('BUY','HOLD','SELL'), index=True)
    st_score = Column(Numeric(5,2))
    lt_signal = Column(Enum('BUY','HOLD','SELL'), index=True)
    lt_score = Column(Numeric(5,2))
    confidence_pct = Column(Numeric(5,2))
    data_quality = Column(Enum('FULL', 'PARTIAL', 'TECHNICALS_ONLY'), default='FULL')
    flags = Column(JSON)
    indicator_breakdown = Column(JSON)
    
    # -- Price Action V3 --
    fifty_two_week_high = Column(Numeric(10,2))
    fifty_two_week_low = Column(Numeric(10,2))
    fifty_two_week_change = Column(Numeric(10,4))
    beta = Column(Numeric(6,3))
    
    # Analysis V2 Columns
    composite_score = Column(Numeric(5,2))
    fundamental_score = Column(Numeric(5,2))
    technical_score = Column(Numeric(5,2))
    momentum_score = Column(Numeric(5,2))
    sector_rank_score = Column(Numeric(5,2))
    sector_percentile = Column(Numeric(5,2))
    data_confidence = Column(Numeric(4,3))
    promoter_pledge_warning = Column(Boolean, default=False)
    score_profile = Column(String(50))
    fa_breakdown = Column(JSON)
    ta_breakdown = Column(JSON)
    momentum_breakdown = Column(JSON)
    pattern_data = Column(JSON, nullable=True) # AI Pattern Auto-detection Cache
    
    # ── Scoring engine v2.1 columns ──────────────
    score_version = Column(String(10), nullable=True)
    scored_at = Column(DateTime, nullable=True)
    fa_coverage = Column(Numeric(4, 3), nullable=True)
    ta_coverage = Column(Numeric(4, 3), nullable=True)
    momentum_coverage = Column(Numeric(4, 3), nullable=True)
    sector_peer_count = Column(SmallInteger, nullable=True)
    backtest_cagr = Column(Numeric(6, 2), nullable=True)
    backtest_win_rate = Column(Numeric(5, 2), nullable=True)
    backtest_sharpe = Column(Numeric(5, 2), nullable=True)
    backtest_max_drawdown = Column(Numeric(6, 2), nullable=True)
    backtest_avg_return = Column(Numeric(6, 2), nullable=True)
    backtest_trades = Column(SmallInteger, nullable=True)

class FundamentalsCache(Base):
    __tablename__ = "fundamentals_cache"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(20), unique=True, index=True, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    pe_ratio = Column(Numeric(10,2))
    eps = Column(Numeric(10,2))
    roe = Column(Numeric(6,2))
    debt_equity = Column(Numeric(6,2))
    revenue_growth = Column(Numeric(6,2)) # Current YoY
    revenue_growth_3yr = Column(Numeric(6,2)) # 3-year CAGR
    pat_growth_3yr = Column(Numeric(6,2)) # 3-year CAGR
    operating_margin = Column(Numeric(6,2))
    pe_5yr_avg = Column(Numeric(10,2))
    roe_3yr_avg = Column(Numeric(6,2))
    sector_pe = Column(Numeric(10,2))
    market_cap = Column(BigInteger)
    
    # -- Institutional Valuation Upgrade --
    peg_ratio = Column(Numeric(6,2))
    ps_ratio = Column(Numeric(10,2))
    pb_ratio = Column(Numeric(10,2))
    ev_ebitda = Column(Numeric(10,2))
    book_value = Column(Numeric(10,2))
    ebitda = Column(BigInteger)

    held_percent_institutions = Column(Numeric(6,2))
    shares_outstanding = Column(BigInteger)
    
    # -- Phase 3: Health & Sentiment --
    analyst_rating = Column(Numeric(4,2))
    recommendation_key = Column(String(50))
    total_cash = Column(BigInteger)
    total_debt = Column(BigInteger)
    current_ratio = Column(Numeric(10,2))
    
    promoter_holding = Column(Numeric(6,2))
    promoter_pledge_pct = Column(Numeric(6,2))
    data_quality = Column(Enum('FULL', 'PARTIAL', 'MISSING', 'AI_RESEARCHED', 'VERIFIED'), default='FULL')

class MLSnapshot(Base):
    __tablename__ = "ml_snapshots"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    data = Column(JSON) # Array of predictions
    summary = Column(JSON) # Overall desk stats (avg return, etc)

    user = relationship("User")

class SyncLog(Base):
    __tablename__ = "sync_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    target_date = Column(Date, nullable=False)
    exchange = Column(Enum('NSE', 'BSE'), default='NSE', nullable=False, index=True)
    sync_type = Column(Enum('MANUAL','SCHEDULED'), nullable=False)
    status = Column(Enum('SUCCESS','FAILED','PARTIAL'), nullable=False)
    records_count = Column(Integer, default=0)
    error_message = Column(String(500))
    completed_at = Column(DateTime, default=datetime.datetime.utcnow)

class AIInsights(Base):
    __tablename__ = "ai_insights"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True) # Multi-user
    symbol = Column(String(20), index=True, nullable=False)
    generated_at = Column(DateTime, index=True, nullable=False)
    trigger_reason = Column(Enum('WEEKLY','PRICE_SPIKE','MANUAL'), nullable=False)
    short_summary = Column(String(1000))
    long_summary = Column(String(5000))
    key_risks = Column(JSON)
    key_opportunities = Column(JSON)
    sentiment_score = Column(Numeric(4,2))
    skill_id = Column(String(50))
    verdict = Column(String(20))
    
    # ── v2.1 consensus fields ─────────────
    consensus_score = Column(Numeric(4, 1), nullable=True)
    bull_count = Column(Integer, default=0, nullable=True)
    bear_count = Column(Integer, default=0, nullable=True)
    neutral_count = Column(Integer, default=0, nullable=True)
    forensic_veto = Column(Boolean, default=False, nullable=True)
    all_verdicts = Column(JSON, nullable=True)
    composite_score_snapshot = Column(Numeric(5, 2), nullable=True)
    score_version_snapshot = Column(String(10), nullable=True)

    # Relationships
    user = relationship("User", back_populates="insights")

class AICallLog(Base):
    __tablename__ = "ai_call_logs"

    id                = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id           = Column(Integer, ForeignKey("users.id"), index=True) # Multi-user
    insight_id        = Column(Integer, nullable=True)           # FK to ai_insights (nullable)
    symbol            = Column(String(20), index=True, nullable=False)
    skill_id          = Column(String(50))
    provider          = Column(String(20), nullable=False)
    model             = Column(String(60), nullable=False)
    trigger_reason    = Column(String(30))
    prompt_tokens     = Column(Integer, default=0)
    completion_tokens = Column(Integer, default=0)
    total_tokens      = Column(Integer, default=0)
    duration_ms       = Column(Integer)
    status            = Column(Enum('SUCCESS', 'ERROR'), default='SUCCESS')
    error_message     = Column(String(500))
    request_payload   = Column(JSON)  # The full prompt/messages sent
    response_raw      = Column(JSON)  # The raw structured response
    called_at         = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)


class AllocationLog(Base):
    __tablename__ = "allocation_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True) # Multi-user
    allocated_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    total_amount = Column(Numeric(14,2), nullable=False)
    allocation_type = Column(String(30), nullable=False) # e.g. MVO, HRP, CVAR
    allocations = Column(JSON, nullable=False)  # Array of {symbol, amount, units, reason}
    ai_call_id = Column(Integer, nullable=True)  # FK to ai_call_logs if used
    
    # Metadata for Smart Allocation
    lookback_days = Column(Integer, nullable=True)
    expected_return = Column(Numeric(8,4), nullable=True)
    expected_volatility = Column(Numeric(8,4), nullable=True)
    expected_sharpe = Column(Numeric(6,3), nullable=True)

class WarRoomSnapshot(Base):
    __tablename__ = "war_room_snapshots"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    symbol = Column(String(20), index=True, nullable=False)
    intel_score = Column(Numeric(5,1))
    snapshot_data = Column(JSON, nullable=False) # Stores the full research object
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

class SystemConfig(Base):
    __tablename__ = "system_config"
    
    key = Column(String(50), primary_key=True)
    value = Column(String(500), nullable=False)
    description = Column(String(200))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

class MoveExplanation(Base):
    """
    Caches AI-generated plain-English explanation for why a stock moved.
    Keyed by symbol + period (week / month / year / ytd).
    TTL: 4 hours — refreshed on demand or by scheduler.
    """
    __tablename__ = "move_explanations"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    symbol     = Column(String(20), nullable=False, index=True)
    period     = Column(String(10), nullable=False)          # 'week' | 'month' | 'year' | 'ytd'
    gain_pct   = Column(Float, nullable=True)                # e.g. 49.82
    headline   = Column(String(200), nullable=True)          # one-line summary
    explanation= Column(Text, nullable=True)                 # 2-3 sentence explanation
    catalysts  = Column(JSON, nullable=True)                 # ["Q3 earnings beat", "FII buying"]
    sentiment  = Column(String(10), nullable=True)           # 'Bullish' | 'Bearish' | 'Neutral'
    should_act = Column(String(30), nullable=True)           # 'Consider Entry' | 'Consider Exit' | 'Hold' | 'Watch'
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "period", name="uq_move_explanation_symbol_period"),
    )

class PriceAlert(Base):
    """
    AI-generated or user-confirmed price alerts.
    Checked EOD by scheduler against PriceHistory latest close.
    """
    __tablename__ = "price_alerts"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    user_id        = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    symbol         = Column(String(20), nullable=False, index=True)
    alert_type     = Column(String(20), nullable=False)   # 'SUPPORT' | 'RESISTANCE' | 'TARGET' | 'STOP_LOSS' | 'CUSTOM'
    direction      = Column(String(10), nullable=False)   # 'ABOVE' | 'BELOW'
    price_level    = Column(Float, nullable=False)
    label          = Column(String(100), nullable=True)   # e.g. "AI Support" "Cup & Handle target"
    source         = Column(String(20), default="AI")     # 'AI' | 'USER'
    ai_rationale   = Column(Text, nullable=True)          # why AI set this level
    is_active      = Column(Boolean, default=True)
    is_triggered   = Column(Boolean, default=False)
    triggered_at   = Column(DateTime, nullable=True)
    triggered_price= Column(Float, nullable=True)
    created_at     = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at     = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

# ── Schema Migrations (safe, idempotent — ADD COLUMN IF NOT EXISTS) ──────────
async def run_migrations():
    """
    Apply any missing columns that were added to models after the table was first
    created.  Uses IF NOT EXISTS so every statement is safe to re-run on restart.
    """
    migrations = [
        # Feature 3: move_explanations.should_act added after initial table creation
        "ALTER TABLE move_explanations ADD COLUMN IF NOT EXISTS should_act VARCHAR(30) NULL",
        # Feature 4: price_alerts — ensure table exists (create_all handles new tables,
        #   but if the table was never created we surface a cleaner error via create_all)
    ]
    async with engine.begin() as conn:
        for stmt in migrations:
            try:
                await conn.execute(text(stmt))
            except Exception as e:
                # Log but don't crash — worst case the column already exists
                import logging
                logging.getLogger(__name__).warning(f"Migration skipped ({e}): {stmt}")


# Database Dependency
async def get_db():
    async with SessionLocal() as session:
        yield session
