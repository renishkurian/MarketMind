import asyncio
import logging
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date, timedelta
from sqlalchemy import select, delete, and_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from typing import Callable, Awaitable, Dict, List

from backend.data.db import (
    SessionLocal, StockMaster, IntradayTicks, SignalsCache,
    PriceHistory, FundamentalsCache, AIInsights
)
from backend.utils.market_hours import is_market_open, get_current_ist_time
from backend.data.fetcher import fetch_live_prices, fetch_fundamentals
from backend.data.bhavcopy import load_bhavcopy_to_db
from backend.engine.ai_engine import generate_insight
from backend.engine.scoring.composite_score import (
    CompositeScorer, ScoreConfig, SectorData, result_to_cache_dict
)
from backend.services.scoring_service import ScoringService

logger = logging.getLogger(__name__)

# ── Broadcast hooks (injected by main.py at startup) ─────────────────────────
broadcast_price_update: Callable[[str, float, float], Awaitable[None]] = None
broadcast_market_status: Callable[[str], Awaitable[None]] = None


async def _safe_broadcast_price(symbol: str, price: float, change_pct: float):
    if broadcast_price_update:
        try:
            await broadcast_price_update(symbol, price, change_pct)
        except Exception as e:
            logger.warning(f"Broadcast failed for {symbol}: {e}")


# ── JOB 1: Intraday fetch (every 5 mins during market hours) ─────────────────
async def intraday_fetch():
    if not is_market_open():
        logger.info("Market closed — skipping intraday fetch.")
        return

    logger.info("Intraday fetch starting…")
    async with SessionLocal() as session:
        result = await session.execute(
            select(StockMaster.symbol).where(StockMaster.is_active == True)
        )
        symbols = [row[0] for row in result.all()]

    if not symbols:
        return

    prices = await fetch_live_prices(symbols)
    if not prices:
        logger.warning("No price data returned from fetcher.")
        return

    ticks_data = []
    for sym, data in prices.items():
        ticks_data.append({
            "symbol": sym,
            "timestamp": data["timestamp"],
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"],
            "volume": data["volume"],
        })

    async with SessionLocal() as session:
        if ticks_data:
            stmt = mysql_insert(IntradayTicks).values(ticks_data)
            stmt = stmt.on_duplicate_key_update(close=stmt.inserted.close)
            await session.execute(stmt)

        for sym, data in prices.items():
            cache_res = await session.execute(
                select(SignalsCache).where(SignalsCache.symbol == sym)
            )
            cache = cache_res.scalars().first()
            change_pct = None

            if cache:
                if cache.prev_close and float(cache.prev_close) > 0:
                    change_pct = ((data["close"] - float(cache.prev_close)) / float(cache.prev_close)) * 100
                cache.current_price = data["close"]
                if change_pct is not None:
                    cache.change_pct = change_pct
                cache.computed_at = data["timestamp"]
                cache.market_session = "LIVE"
            else:
                cache = SignalsCache(
                    symbol=sym,
                    computed_at=data["timestamp"],
                    market_session="LIVE",
                    current_price=data["close"],
                )
                session.add(cache)

            await _safe_broadcast_price(sym, data["close"], change_pct or 0.0)

        await session.commit()

    logger.info(f"Intraday fetch complete — {len(prices)} symbols updated.")


# ── JOB 2: EOD Consolidation (weekdays 18:00) ────────────────────────────────
async def eod_consolidation():
    logger.info("EOD consolidation starting…")
    now = get_current_ist_time()

    try:
        # Step 1: Download and persist Bhavcopy for today (Both NSE and BSE)
        logger.info("Starting NSE Bhavcopy sync...")
        await load_bhavcopy_to_db(now, sync_type='SCHEDULED', exchange='NSE')
        
        logger.info("Starting BSE Bhavcopy sync...")
        await load_bhavcopy_to_db(now, sync_type='SCHEDULED', exchange='BSE')

        # Step 2: Delete today's intraday ticks
        today_start = datetime.combine(now.date(), datetime.min.time())
        async with SessionLocal() as session:
            await session.execute(
                delete(IntradayTicks).where(IntradayTicks.timestamp >= today_start)
            )
            await session.commit()

        # Step 3: Recompute signals for all active stocks
        async with SessionLocal() as session:
            result = await session.execute(
                select(StockMaster.symbol).where(StockMaster.is_active == True)
            )
            symbols = [row[0] for row in result.all()]

        logger.info(f"Recomputing signals for {len(symbols)} symbols…")
        
        # Step 3.5: Pre-fetch all peer data for sector ranking
        sector_vault = await _fetch_all_sector_data()
        
        for sym in symbols:
            try:
                await recompute_signals_for(sym, sector_vault)
            except Exception as e:
                logger.error(f"Signal recompute failed for {sym}: {e}")

        # Step 4: Check for price spikes (>3%) and queue AI insight
        await _check_price_spikes_and_notify()

        logger.info("EOD consolidation complete.")
    except Exception as e:
        logger.error(f"EOD consolidation failed: {e}")


async def recompute_signals_for(symbol: str, sector_vault: Dict[str, SectorData] = None):
    """Fetch price history from DB, recompute all indicators, update SignalsCache using V2 Engine."""
    async with SessionLocal() as session:
        try:
            # ── V2.1 SCORING SERVICE INTEGRATION ────────────────────────────────────
            from backend.services.scoring_service import ScoringService
            
            service = ScoringService(session, profile="long_term_compounding")
            res = await service.score_symbol(symbol, exchange="NSE")
            
            # Legacy fields for backward compatibility with existing UI components
            st_signal = "BUY" if res.technical_score > 60 else "SELL" if res.technical_score < 40 else "HOLD"
            lt_signal = "BUY" if res.composite_score > 65 else "SELL" if res.composite_score < 45 else "HOLD"
            confidence = res.data_confidence * 100
            
            # The service already persisted the core v2.1 columns. 
            # We just need to update legacy UI fields: signals, st/lt_score, and indicators.
            stmt = select(SignalsCache).where(
                and_(SignalsCache.symbol == symbol, SignalsCache.exchange == "NSE")
            ).order_by(SignalsCache.computed_at.desc()).limit(1)
            
            sig_res = await session.execute(stmt)
            signal_row = sig_res.scalar_one_or_none()
            
            if signal_row:
                signal_row.st_signal = st_signal
                signal_row.lt_signal = lt_signal
                signal_row.st_score = res.technical_score
                signal_row.lt_score = res.composite_score
                signal_row.confidence_pct = confidence
                signal_row.data_quality = "FULL" if res.fa_coverage >= 0.5 else "TECHNICALS_ONLY"
                
                # Group V2.1 metrics into timeframe-based UI buckets
                short_term = {}
                long_term = {}
                
                # Map Technical Setup -> Short Term
                short_term.update({k: {"score": v, "max": 100} for k, v in res.ta_breakdown.items()})
                
                # Map Momentum -> Split by Timeframe
                for k, v in res.momentum_breakdown.items():
                    if k in ["roc_20d", "roc_60d", "volume_trend", "trade_activity"]:
                        short_term[k] = {"score": v, "max": 100}
                    else:
                        long_term[k] = {"score": v, "max": 100}

                # Map Fundamentals -> Long Term
                long_term.update({k: {"score": v, "max": 100} for k, v in res.fa_breakdown.items() if k != "pledge_penalty_on_roe"})

                signal_row.indicator_breakdown = {
                    "SHORT_TERM": short_term,
                    "LONG_TERM": long_term
                }
                await session.commit()
                
                # Broadcast update if price metadata is available
                if signal_row.current_price and signal_row.change_pct:
                    await _safe_broadcast_price(symbol, float(signal_row.current_price), float(signal_row.change_pct))
            
            logger.info(f"{symbol}: [V2.1 Institutional] Score={res.composite_score:.1f} Signals={lt_signal}")
            
        except Exception as e:
            await session.rollback()
            logger.error(f"{symbol}: Failed to recompute signals via service: {e}", exc_info=True)
            raise e


async def _check_price_spikes_and_notify():
    """Detect stocks with >3% daily move and queue AI insight generation."""
    async with SessionLocal() as session:
        result = await session.execute(
            select(SignalsCache)
            .where(SignalsCache.change_pct != None)
        )
        signals = result.scalars().all()

    for sig in signals:
        try:
            change = float(sig.change_pct or 0)
            if abs(change) >= 3.0:
                logger.info(f"{sig.symbol}: price spike detected ({change:+.2f}%) — queuing AI insight.")
                asyncio.create_task(_generate_and_store_insight(sig.symbol, "PRICE_SPIKE"))
        except Exception as e:
            logger.error(f"Spike check failed for {sig.symbol}: {e}")


# ── JOB 3: Weekly fundamental + AI refresh (Mondays 08:00) ───────────────────
async def weekly_refresh():
    logger.info("Weekly refresh starting…")

    async with SessionLocal() as session:
        result = await session.execute(
            select(StockMaster.symbol).where(StockMaster.is_active == True)
        )
        symbols = [row[0] for row in result.all()]

    for sym in symbols:
        try:
            # Refresh fundamentals
            fund_data = await fetch_fundamentals(sym)

            async with SessionLocal() as session:
                stmt = mysql_insert(FundamentalsCache).values(
                    symbol=sym,
                    fetched_at=datetime.now(),
                    pe_ratio=fund_data.get("pe_ratio"),
                    eps=fund_data.get("eps"),
                    roe=fund_data.get("roe"),
                    debt_equity=fund_data.get("debt_equity"),
                    revenue_growth=fund_data.get("revenue_growth"),
                    market_cap=fund_data.get("market_cap"),
                    data_quality=fund_data.get("data_quality", "MISSING"),
                )
                FUND_COLS = ["fetched_at", "pe_ratio", "eps", "roe", "debt_equity",
                             "revenue_growth", "market_cap", "data_quality"]
                stmt = stmt.on_duplicate_key_update(
                    **{c: stmt.inserted[c] for c in FUND_COLS}
                )
                await session.execute(stmt)
                await session.commit()

            # Recompute full signals with updated fundamentals
            await recompute_signals_for(sym)

            # Generate weekly AI insight
            await _generate_and_store_insight(sym, "WEEKLY")

        except Exception as e:
            logger.error(f"Weekly refresh failed for {sym}: {e}")

    logger.info("Weekly refresh complete.")


async def _generate_and_store_insight(symbol: str, trigger: str):
    """Generate AI insight and persist it to AIInsights table."""
    try:
        async with SessionLocal() as session:
            ph_res = await session.execute(
                select(PriceHistory)
                .where(PriceHistory.symbol == symbol)
                .order_by(PriceHistory.date.asc())
            )
            history = ph_res.scalars().all()
            df = pd.DataFrame([
                {"date": str(h.date), "close": float(h.close),
                 "open": float(h.open or 0), "high": float(h.high or 0),
                 "low": float(h.low or 0), "volume": int(h.volume or 0)}
                for h in history
            ])

            sig_res = await session.execute(
                select(SignalsCache).where(SignalsCache.symbol == symbol)
            )
            sig = sig_res.scalars().first()
            signals = {
                "st_signal": sig.st_signal if sig else None,
                "st_score": float(sig.st_score) if sig and sig.st_score else None,
                "lt_signal": sig.lt_signal if sig else None,
                "lt_score": float(sig.lt_score) if sig and sig.lt_score else None,
                "confidence_pct": float(sig.confidence_pct) if sig and sig.confidence_pct else None,
                "data_quality": sig.data_quality if sig else None,
                "indicator_breakdown": sig.indicator_breakdown if sig else {},
            }

            fund_res = await session.execute(
                select(FundamentalsCache).where(FundamentalsCache.symbol == symbol)
            )
            fund = fund_res.scalars().first()
            fundamentals = {
                k: (float(getattr(fund, k)) if getattr(fund, k) else None)
                for k in ["pe_ratio", "eps", "roe", "debt_equity", "revenue_growth"]
            } if fund else {}

        insight_data = await generate_insight(symbol, df, signals, fundamentals, trigger)

        async with SessionLocal() as session:
            new_insight = AIInsights(
                symbol=symbol,
                generated_at=datetime.now(),
                trigger_reason=trigger,
                skill_id=None, # Default for background triggers unless specified
                verdict=insight_data.get("verdict"),
                short_summary=insight_data.get("short_summary"),
                long_summary=insight_data.get("long_summary"),
                key_risks=insight_data.get("key_risks", []),
                key_opportunities=insight_data.get("key_opportunities", []),
                sentiment_score=insight_data.get("sentiment_score"),
            )
            session.add(new_insight)
            await session.commit()
            logger.info(f"AI insight ({trigger}, verdict: {insight_data.get('verdict')}) stored for {symbol}.")

    except Exception as e:
        logger.error(f"generate_and_store_insight failed for {symbol}: {e}")

async def _fetch_all_sector_data() -> Dict[str, SectorData]:
    """Pre-fetch and group sector peers for efficient ranking."""
    logger.info("Batch fetching sector data for institutional ranking...")
    vault = {}
    async with SessionLocal() as session:
        # Fetch all fundamentals and recent signals
        res = await session.execute(
            select(
                StockMaster.sector, 
                FundamentalsCache.pe_ratio, 
                FundamentalsCache.roe, 
                FundamentalsCache.revenue_growth,
                SignalsCache.change_pct
            )
            .join(FundamentalsCache, StockMaster.symbol == FundamentalsCache.symbol)
            .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
            .where(StockMaster.is_active == True)
        )
        rows = res.all()
        
        # Group by sector
        grouped: Dict[str, Dict[str, List[float]]] = {}
        for sector, pe, roe, rev, mom in rows:
            if not sector: continue
            if sector not in grouped:
                grouped[sector] = {"pe": [], "roe": [], "rev": [], "mom": []}
            if pe: grouped[sector]["pe"].append(float(pe))
            if roe: grouped[sector]["roe"].append(float(roe))
            if rev: grouped[sector]["rev"].append(float(rev))
            if mom: grouped[sector]["mom"].append(float(mom))
            
        for sector, data in grouped.items():
            vault[sector] = SectorData(
                sector=sector,
                sector_pe_list=data["pe"],
                sector_roe_list=data["roe"],
                sector_revenue_growth_list=data["rev"],
                sector_momentum_list=data["mom"]
            )
    return vault


# ── Scheduler setup ───────────────────────────────────────────────────────────
def start_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

    # Every 5 minutes Mon–Fri during market hours
    scheduler.add_job(intraday_fetch, "cron",
                      day_of_week="mon-fri", hour="9-15", minute="*/5",
                      id="intraday_fetch", replace_existing=True)

    # EOD at 18:00 Mon–Fri
    scheduler.add_job(eod_consolidation, "cron",
                      day_of_week="mon-fri", hour=18, minute=0,
                      id="eod_consolidation", replace_existing=True)

    # Weekly on Monday at 08:00
    scheduler.add_job(weekly_refresh, "cron",
                      day_of_week="mon", hour=8, minute=0,
                      id="weekly_refresh", replace_existing=True)

    scheduler.start()
    logger.info("Scheduler started with 3 jobs: intraday_fetch, eod_consolidation, weekly_refresh")
    return scheduler


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scheduler = start_scheduler()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
