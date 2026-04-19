from __future__ import annotations
import logging
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
import pandas as pd
import re

from backend.data.db import (
    get_db, StockMaster, SignalsCache, PriceHistory, FundamentalsCache, AIInsights, SessionLocal
)
from backend.engine.scoring.composite_score import (
    CompositeScorer, ScoreConfig, SectorData, result_to_cache_dict
)
from backend.engine.scoring.mapper import (
    build_fa_from_db, build_momentum_from_df, build_ta_from_indicators
)
from backend.engine.backtest.backtest_engine import (
    BacktestEngine, PriceBar, SignalEvent
)
from backend.engine.consensus.skill_loader import SkillLoader, StockMeta
from backend.engine.consensus.consensus_engine import ConsensusEngine, SkillAnalysis
from backend.engine.indicators import compute_short_term_indicators, compute_long_term_indicators

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/analysis", tags=["Analysis"])

# ── Analysis Infrastructure ───────────────────────────────────────────────────

# Scorer and consensus are stateless or cache-heavy, safe to globalize
consensus_engine = ConsensusEngine()
skill_loader = SkillLoader(skills_dir="backend/engine/skills")

# ── API Endpoints ─────────────────────────────────────────────────────────────

@router.get("/{isin}/full")
async def get_full_analysis(
    isin: str,
    skills: List[str] = Query(default=["sebi_forensic", "warren_buffett_quality", "rj_india_growth"]),
    profile: str = "long_term_compounding",
    db: AsyncSession = Depends(get_db)
):
    """
    Runs a full institutional-grade analysis pipeline:
    1. Recomputes Composite Score (FA+TA+Momentum+Sector)
    2. Runs walk-forward Backtest for the signal
    3. Executes AI Skills via Consensus Engine
    4. Handles Forensic Veto
    """
    
    # 1. Fetch Master Data
    master_res = await db.execute(
        select(StockMaster).where(StockMaster.isin == isin, StockMaster.is_active == True)
    )
    stock = master_res.scalars().first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"Stock with ISIN {isin} not found in master.")
    
    symbol = stock.symbol

    # 2. Fetch History & Cache Objects
    df, fund_cache, sig_cache = await _fetch_required_data(db, symbol, isin)
    
    # 3. Build Engine Data Containers
    fa_data = build_fa_from_db(fund_cache)
    
    # Use fresh indicators from history for the V2 scores (includes trades_shock)
    st_indicators = compute_short_term_indicators(df)
    lt_indicators = compute_long_term_indicators(df)
    ta_data = build_ta_from_indicators(st_indicators, lt_indicators)
    
    mom_data = build_momentum_from_df(df)
    sector_data = await _build_sector_data(db, stock.sector)
    
    # 4. Run Scoring Engine
    config = ScoreConfig(profile=profile)
    scorer = CompositeScorer(config)
    score_result = scorer.score(symbol, isin, fa_data, ta_data, mom_data, sector_data)
    
    # 5. Run Backtest Engine (Parallel with AI once we add async)
    backtest_engine = BacktestEngine(
        price_fetcher=lambda i, s, e: _price_fetcher(db, i, s, e),
        signal_fetcher=lambda i, t, s, e: _signal_fetcher(db, i, t, s, e)
    )
    bt_metrics = await backtest_engine.run(
        symbol=symbol, isin=isin, 
        signal_type="composite_score", 
        score_threshold=65.0, # Default institutional threshold
        start_date=date(2016, 1, 1)
    )

    # 6. Consensus Engine & AI Skills
    # SkillLoader formatting
    stock_meta = StockMeta(
        symbol=symbol, isin=isin, exchange=stock.exchange,
        sector=stock.sector or "Unknown",
        market_cap_cr=(float(fund_cache.market_cap) / 10000000) if fund_cache and fund_cache.market_cap else 0,
        current_price=float(sig_cache.current_price) if sig_cache else 0,
        pe_ratio=float(fund_cache.pe_ratio) if fund_cache else None,
        roe=float(fund_cache.roe) if fund_cache else None,
        debt_equity=float(fund_cache.debt_equity) if fund_cache else None,
        revenue_growth_3yr=float(fund_cache.revenue_growth) if fund_cache else None,
        promoter_holding=float(fund_cache.promoter_holding) if fund_cache else None,
        promoter_pledge_pct=float(fund_cache.promoter_pledge_pct) if fund_cache else None
    )

    # In a real app, we'd call an LLM here. For now, we simulate or use existing summaries 
    # to demonstrate the bridge.
    analyses = []
    for skill_name in skills:
        # Check if we already have a recent insight for this skill
        existing_insight = await _get_recent_insight(db, symbol, skill_name)
        
        if existing_insight:
            verdict = consensus_engine.extract_verdict(existing_insight.long_summary)
            analyses.append(SkillAnalysis(
                skill_name=skill_name,
                display_name=skill_name.replace("_", " ").title(),
                verdict=verdict,
                narrative=existing_insight.long_summary
            ))
        else:
            # Future: Call generate_expert_analysis(skill_id, prompt)
            analyses.append(SkillAnalysis(
                skill_name=skill_name,
                display_name=skill_name.replace("_", " ").title(),
                verdict="WATCH",
                narrative=f"Pending AI generation for {skill_name}. Please trigger manual generation."
            ))

    consensus_result = consensus_engine.compute_consensus(symbol, isin, analyses)

    # 7. Persist Results to Cache
    await _update_signals_cache(db, isin, score_result)
    
    return {
        "score": score_result,
        "backtest": bt_metrics,
        "consensus": consensus_result.to_dashboard_dict(),
        "meta": stock_meta
    }

# ── Private Helpers ──────────────────────────────────────────────────────────

async def _fetch_required_data(db: AsyncSession, symbol: str, isin: str):
    h_res = await db.execute(
        select(PriceHistory).where(PriceHistory.isin == isin).order_by(PriceHistory.date.asc())
    )
    history = h_res.scalars().all()
    df = pd.DataFrame([{
        "date": h.date, 
        "close": float(h.close), 
        "open": float(h.open or 0),
        "high": float(h.high or 0),
        "low": float(h.low or 0),
        "volume": int(h.volume or 0),
        "no_of_trades": h.no_of_trades
    } for h in history])
    
    # Fundamentals
    f_res = await db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == symbol))
    fund = f_res.scalars().first()
    
    # Signals
    s_res = await db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
    sig = s_res.scalars().first()
    
    return df, fund, sig

# Removing duplicate _build_fa and _build_momentum (now in mapper.py)

def _build_ta(sig: Optional[SignalsCache]) -> TechnicalData:
    # We keep this helper here as it handles the legacy "P:1179.8" string parsing
    # which is specific to viewing cached results.
    from backend.engine.scoring.composite_score import TechnicalData
    if not sig or not sig.indicator_breakdown: return TechnicalData()
    ib = sig.indicator_breakdown
    st = ib.get("short_term", {})
    lt = ib.get("long_term", {})
    
    # Helper to parse "P:1179.8, S20:1107.44"
    def parse_sma(s, key):
        if not s: return None
        match = re.search(rf"{key}:([\d.]+)", s)
        return float(match.group(1)) if match else None

    sma_str = st.get("SMA", {}).get("value", "")
    cross_str = lt.get("Cross", {}).get("value", "")
    price = parse_sma(sma_str, "P")
    
    s20 = parse_sma(sma_str, "S20")
    s50 = parse_sma(sma_str, "S50")
    s200 = parse_sma(cross_str, "200")

    return TechnicalData(
        rsi_14=st.get("RSI", {}).get("value"),
        macd_signal=st.get("MACD", {}).get("value"),
        price_vs_sma20=((price - s20) / s20 * 100) if price and s20 else None,
        price_vs_sma50=((price - s50) / s50 * 100) if price and s50 else None,
        price_vs_sma200=((price - s200) / s200 * 100) if price and s200 else None,
        adx=lt.get("ADX", {}).get("value")
    )

# Removing duplicate _build_momentum (now in mapper.py)

async def _build_sector_data(db: AsyncSession, sector: str) -> SectorData:
    if not sector: return SectorData()
    
    # Fetch all peers in sector
    res = await db.execute(
        select(FundamentalsCache.roe, FundamentalsCache.revenue_growth, SignalsCache.change_pct)
        .join(StockMaster, StockMaster.symbol == FundamentalsCache.symbol)
        .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
        .where(StockMaster.sector == sector)
    )
    rows = res.all()
    
    return SectorData(
        sector=sector,
        sector_roe_list=[float(r[0]) for r in rows if r[0]],
        sector_revenue_growth_list=[float(r[1]) for r in rows if r[1]],
        sector_momentum_list=[float(r[2]) for r in rows if r[2]]
    )

async def _price_fetcher(db: AsyncSession, isin: str, start: date, end: date) -> List[PriceBar]:
    res = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.isin == isin, PriceHistory.date >= start, PriceHistory.date <= end)
        .order_by(PriceHistory.date.asc())
    )
    return [
        PriceBar(date=h.date, open=float(h.open or 0), high=float(h.high or 0), 
                 low=float(h.low or 0), close=float(h.close), volume=int(h.volume or 0))
        for h in res.scalars().all()
    ]

async def _signal_fetcher(db: AsyncSession, isin: str, sig_type: str, start: date, end: date) -> List[SignalEvent]:
    # In V1, we don't store snapshots of SignalsCache.
    # For backtesting, we simulate the score at each historical point.
    # This is a bit heavy, so we'll return a sparse list based on existing history.
    # Future enhancement: Snapshot table for SignalsCache.
    return []

async def _get_recent_insight(db: AsyncSession, symbol: str, skill_id: str):
    res = await db.execute(
        select(AIInsights)
        .where(AIInsights.symbol == symbol, AIInsights.skill_id == skill_id)
        .order_by(AIInsights.generated_at.desc())
        .limit(1)
    )
    return res.scalars().first()

async def _update_signals_cache(db: AsyncSession, isin: str, r):
    # Map back to DB columns
    update_data = result_to_cache_dict(r)
    from sqlalchemy import update
    await db.execute(
        update(SignalsCache).where(SignalsCache.symbol == r.symbol).values(**update_data)
    )
    await db.commit()
