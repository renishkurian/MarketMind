import os
import sys
import datetime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, Numeric, BigInteger, JSON, Enum, UniqueConstraint, SmallInteger

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

class StockMaster(Base):
    __tablename__ = "stocks_master"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
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
    
    # Portfolio specific fields
    quantity = Column(Numeric(14,4))
    avg_buy_price = Column(Numeric(18,6))
    buy_date = Column(Date)

class PortfolioTransaction(Base):
    __tablename__ = "portfolio_transactions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(20), index=True, nullable=False)
    isin = Column(String(12), index=True)
    quantity = Column(Numeric(14,4), nullable=False)
    buy_price = Column(Numeric(18,6), nullable=False)
    buy_date = Column(Date, nullable=False)
    status = Column(Enum('OPEN', 'CLOSED'), default='OPEN', nullable=False)

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
    change_pct = Column(Numeric(6,2))
    st_signal = Column(Enum('BUY','HOLD','SELL'), index=True)
    st_score = Column(Numeric(5,2))
    lt_signal = Column(Enum('BUY','HOLD','SELL'), index=True)
    lt_score = Column(Numeric(5,2))
    confidence_pct = Column(Numeric(5,2))
    data_quality = Column(Enum('FULL','TECHNICALS_ONLY'), default='FULL')
    flags = Column(JSON)
    indicator_breakdown = Column(JSON)
    
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
    promoter_holding = Column(Numeric(6,2))
    promoter_pledge_pct = Column(Numeric(6,2))
    data_quality = Column(Enum('FULL', 'PARTIAL', 'MISSING', 'AI_RESEARCHED', 'VERIFIED'), default='FULL')

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

class AICallLog(Base):
    __tablename__ = "ai_call_logs"

    id                = Column(Integer, primary_key=True, index=True, autoincrement=True)
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


class SystemConfig(Base):
    __tablename__ = "system_config"
    
    key = Column(String(50), primary_key=True)
    value = Column(String(500), nullable=False)
    description = Column(String(200))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

# Database Dependency
async def get_db():
    async with SessionLocal() as session:
        yield session
