import os
import sys
import datetime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, Numeric, BigInteger, JSON, Enum

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

class FundamentalsCache(Base):
    __tablename__ = "fundamentals_cache"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(20), unique=True, index=True, nullable=False)
    fetched_at = Column(DateTime, nullable=False)
    pe_ratio = Column(Numeric(10,2))
    eps = Column(Numeric(10,2))
    roe = Column(Numeric(6,2))
    debt_equity = Column(Numeric(6,2))
    revenue_growth = Column(Numeric(6,2))
    sector_pe = Column(Numeric(10,2))
    market_cap = Column(BigInteger)
    data_quality = Column(Enum('FULL','PARTIAL','MISSING'), default='FULL')

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
