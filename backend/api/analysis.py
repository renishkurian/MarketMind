from __future__ import annotations
import logging
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func
import pandas as pd
import re

from backend.data.db import (
    get_db, StockMaster, SignalsCache, PriceHistory, FundamentalsCache, AIInsights, SessionLocal
)
from backend.engine.scoring.composite_score import (
    CompositeScorer, ScoreConfig, SectorData, result_to_cache_dict
)
from backend.engine.scoring.mapper import (
    build_fa_from_db, build_momentum_from_df, build_ta_from_indicators,
    build_signals_from_indicators,
)
from backend.engine.backtest.backtest_engine import (
    BacktestEngine, PriceBar, SignalEvent
)
from backend.engine.consensus.skill_loader import SkillLoader, StockMeta
from backend.engine.consensus.consensus_engine import ConsensusEngine, SkillAnalysis
from backend.engine.indicators import compute_short_term_indicators, compute_long_term_indicators

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/analysis", tags=["Analysis"])

# Nifty symbol in your PriceHistory — adjust if stored differently
NIFTY_SYMBOL = "NIFTY50"

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
    
    # 3. Fetch Nifty history for relative strength
    nifty_df = await _fetch_nifty_df(db)

    # 4. Build Engine Data Containers
    fa_data = build_fa_from_db(fund_cache)
    
    # Use fresh indicators from history
    st_indicators = compute_short_term_indicators(df)
    lt_indicators = compute_long_term_indicators(df)
    ta_data = build_ta_from_indicators(st_indicators, lt_indicators)
    
    mom_data = build_momentum_from_df(df, nifty_df=nifty_df)
    sector_data = await _build_sector_data(db, stock.sector)
    
    # Derived signals from indicators (BUY/SELL)
    signal_fields = build_signals_from_indicators(st_indicators, lt_indicators)

    # 5. Run Scoring Engine
    config = ScoreConfig(profile=profile)
    scorer = CompositeScorer(config)
    score_result = scorer.score(symbol, isin, fa_data, ta_data, mom_data, sector_data)
    
    # 6. Run Backtest Engine
    backtest_engine = BacktestEngine(
        price_fetcher=lambda i, s, e: _price_fetcher(db, i, s, e),
        signal_fetcher=lambda i, t, s, e: _signal_fetcher(db, i, t, s, e)
    )
    bt_metrics = await backtest_engine.run(
        symbol=symbol, isin=isin, 
        signal_type="composite_score", 
        score_threshold=65.0, 
        start_date=date(2016, 1, 1)
    )

    # 7. Consensus Engine & AI Skills
    current_price = float(sig_cache.current_price) if sig_cache and sig_cache.current_price else 0.0

    stock_meta = StockMeta(
        symbol=symbol, isin=isin, exchange=stock.exchange,
        sector=stock.sector or "Unknown",
        market_cap_cr=(float(fund_cache.market_cap) / 10_000_000) if fund_cache and fund_cache.market_cap else 0,
        current_price=current_price,
        pe_ratio=float(fund_cache.pe_ratio) if fund_cache and fund_cache.pe_ratio else None,
        pe_5yr_avg=float(fund_cache.pe_5yr_avg) if fund_cache and fund_cache.pe_5yr_avg else None,
        roe=float(fund_cache.roe) if fund_cache and fund_cache.roe else None,
        roe_3yr_avg=float(fund_cache.roe_3yr_avg) if fund_cache and fund_cache.roe_3yr_avg else None,
        debt_equity=float(fund_cache.debt_equity) if fund_cache and fund_cache.debt_equity else None,
        revenue_growth_3yr=float(fund_cache.revenue_growth_3yr) if fund_cache and fund_cache.revenue_growth_3yr else None,
        pat_growth_3yr=float(fund_cache.pat_growth_3yr) if fund_cache and fund_cache.pat_growth_3yr else None,
        operating_margin=float(fund_cache.operating_margin) if fund_cache and fund_cache.operating_margin else None,
        promoter_holding=float(fund_cache.promoter_holding) if fund_cache and fund_cache.promoter_holding else None,
        promoter_pledge_pct=float(fund_cache.promoter_pledge_pct) if fund_cache and fund_cache.promoter_pledge_pct else None,
        roc_252=mom_data.roc_252,
        roc_60=mom_data.roc_60,
        volume_ratio_20_90=mom_data.volume_ratio_20_90,
    )

    analyses = []
    for skill_name in skills:
        existing_insight = await _get_recent_insight(db, symbol, skill_id=skill_name)
        if existing_insight:
            verdict = consensus_engine.extract_verdict(existing_insight.long_summary)
            analyses.append(SkillAnalysis(
                skill_name=skill_name,
                display_name=skill_name.replace("_", " ").title(),
                verdict=verdict,
                narrative=existing_insight.long_summary
            ))
        else:
            analyses.append(SkillAnalysis(
                skill_name=skill_name,
                display_name=skill_name.replace("_", " ").title(),
                verdict="WATCH",
                narrative=f"Pending AI generation for {skill_name}."
            ))

    consensus_result = consensus_engine.compute_consensus(symbol, isin, analyses)

    # 8. Persist Results — Safe upsert + signal fields
    await _update_signals_cache(db, isin, symbol, stock.exchange, score_result, signal_fields, current_price)
    
    return {
        "score": score_result,
        "backtest": bt_metrics,
        "consensus": consensus_result.to_dashboard_dict(),
        "meta": stock_meta,
        "signals": signal_fields,
        "st_indicators": {
            "overall_trend": st_indicators.get("overall_trend"),
            "ema_crossover": st_indicators.get("ema_crossover"),
            "macd_crossover": st_indicators.get("macd_crossover"),
        },
        "lt_indicators": {
            "lt_recommendation": lt_indicators.get("lt_recommendation"),
        },
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
    
    f_res = await db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == symbol))
    fund = f_res.scalars().first()
    
    s_res = await db.execute(
        select(SignalsCache)
        .where(SignalsCache.symbol == symbol)
        .order_by(SignalsCache.computed_at.desc())
        .limit(1)
    )
    sig = s_res.scalars().first()
    
    return df, fund, sig

async def _fetch_nifty_df(db: AsyncSession) -> pd.DataFrame | None:
    """Fetch Nifty 50 price history for relative strength calculation."""
    try:
        res = await db.execute(
            select(PriceHistory)
            .where(PriceHistory.symbol == NIFTY_SYMBOL)
            .order_by(PriceHistory.date.asc())
        )
        rows = res.scalars().all()
        if not rows:
            return None
        return pd.DataFrame([{"date": r.date, "close": float(r.close)} for r in rows])
    except Exception:
        return None

async def _build_sector_data(db: AsyncSession, sector: str) -> SectorData:
    if not sector: return SectorData()
    
    # Fetch all peers in sector
    res = await db.execute(
        select(FundamentalsCache.roe, FundamentalsCache.revenue_growth_3yr, SignalsCache.change_pct)
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

async def _signal_fetcher(
    db: AsyncSession, isin: str, sig_type: str, start: date, end: date
) -> List[SignalEvent]:
    """Reads historical composite_score from SignalsCache joined on ISIN."""
    stmt = (
        select(
            SignalsCache.symbol,
            SignalsCache.composite_score,
            SignalsCache.computed_at,
            StockMaster.sector,
        )
        .join(StockMaster, StockMaster.symbol == SignalsCache.symbol)
        .where(
            StockMaster.isin == isin,
            SignalsCache.computed_at >= start,
            SignalsCache.computed_at <= end,
            SignalsCache.composite_score.isnot(None),
        )
        .order_by(SignalsCache.computed_at.asc())
    )
    res = await db.execute(stmt)
    events = []
    for row in res.fetchall():
        signal_date = row.computed_at.date() if isinstance(row.computed_at, datetime) else row.computed_at
        events.append(SignalEvent(
            signal_date=signal_date,
            symbol=row.symbol,
            isin=isin,
            signal_type="composite_score",
            signal_value=float(row.composite_score),
            composite_score=float(row.composite_score),
            sector=row.sector or "",
        ))
    return events

async def _get_recent_insight(db: AsyncSession, symbol: str, skill_id: str):
    res = await db.execute(
        select(AIInsights)
        .where(AIInsights.symbol == symbol, AIInsights.skill_id == skill_id)
        .order_by(AIInsights.generated_at.desc())
        .limit(1)
    )
    return res.scalars().first()

async def _update_signals_cache(
    db: AsyncSession, 
    isin: str, 
    symbol: str, 
    exchange: str, 
    r, 
    signal_fields: dict, 
    current_price: float
) -> None:
    """Safe upsert — checks row existence before UPDATE vs INSERT."""
    update_data = result_to_cache_dict(r)
    update_data.update(signal_fields)

    existing = await db.execute(
        select(SignalsCache.id)
        .where(SignalsCache.symbol == symbol, SignalsCache.exchange == exchange)
        .order_by(SignalsCache.computed_at.desc())
        .limit(1)
    )
    row_id = existing.scalar_one_or_none()

    if row_id:
        await db.execute(
            update(SignalsCache).where(SignalsCache.id == row_id).values(**update_data)
        )
    else:
        from datetime import timezone
        new_row = SignalsCache(
            symbol=symbol,
            exchange=exchange,
            computed_at=datetime.now(timezone.utc),
            market_session="EOD",
            current_price=current_price,
            **update_data,
        )
        db.add(new_row)

    await db.commit()

def _build_ta(sig: Optional[SignalsCache]) -> TechnicalData:
    """Legacy fallback: parse formatted strings in indicator_breakdown."""
    from backend.engine.scoring.composite_score import TechnicalData
    if not sig or not sig.indicator_breakdown: return TechnicalData()
    ib = sig.indicator_breakdown
    st = ib.get("short_term", {})
    lt = ib.get("long_term", {})
    
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
