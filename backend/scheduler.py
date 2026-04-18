import asyncio
import logging
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date, timedelta
from sqlalchemy import select, delete
from sqlalchemy.dialects.mysql import insert as mysql_insert
from typing import Callable, Awaitable

from backend.data.db import (
    SessionLocal, StockMaster, IntradayTicks, SignalsCache,
    PriceHistory, FundamentalsCache, AIInsights
)
from backend.utils.market_hours import is_market_open, get_current_ist_time
from backend.data.fetcher import fetch_live_prices, fetch_fundamentals
from backend.data.bhavcopy import load_bhavcopy_to_db
from backend.engine.ai_engine import generate_insight
from backend.engine.indicators import compute_short_term_indicators, compute_long_term_indicators
from backend.engine.scorer import score_short_term, score_long_term, calculate_confidence

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
        for sym in symbols:
            try:
                await _recompute_signals_for(sym)
            except Exception as e:
                logger.error(f"Signal recompute failed for {sym}: {e}")

        # Step 4: Check for price spikes (>3%) and queue AI insight
        await _check_price_spikes_and_notify()

        logger.info("EOD consolidation complete.")
    except Exception as e:
        logger.error(f"EOD consolidation failed: {e}")


async def _recompute_signals_for(symbol: str):
    """Fetch price history from DB, recompute all indicators, update SignalsCache."""
    async with SessionLocal() as session:
        # Fetch full price history
        ph_res = await session.execute(
            select(PriceHistory)
            .where(PriceHistory.symbol == symbol)
            .order_by(PriceHistory.date.asc())
        )
        history = ph_res.scalars().all()

        if len(history) < 50:
            logger.warning(f"{symbol}: insufficient history ({len(history)} bars) — skipping.")
            return

        df = pd.DataFrame([
            {
                "date": str(h.date),
                "open": float(h.open or 0),
                "high": float(h.high or 0),
                "low": float(h.low or 0),
                "close": float(h.close),
                "volume": int(h.volume or 0),
            }
            for h in history
        ])

        # Fetch fundamentals from cache
        fund_res = await session.execute(
            select(FundamentalsCache).where(FundamentalsCache.symbol == symbol)
        )
        fund = fund_res.scalars().first()
        fundamentals = {}
        if fund:
            fundamentals = {
                "pe_ratio": float(fund.pe_ratio) if fund.pe_ratio else None,
                "eps": float(fund.eps) if fund.eps else None,
                "roe": float(fund.roe) if fund.roe else None,
                "debt_equity": float(fund.debt_equity) if fund.debt_equity else None,
                "revenue_growth": float(fund.revenue_growth) if fund.revenue_growth else None,
                "sector_pe": float(fund.sector_pe) if fund.sector_pe else 20.0,
            }

        # Compute indicators
        st_indicators = compute_short_term_indicators(df)
        lt_indicators = compute_long_term_indicators(df) if len(df) >= 200 else {}

        if not st_indicators:
            logger.warning(f"{symbol}: short-term indicators empty — skipping.")
            return

        # Score
        st_result = score_short_term(st_indicators, fundamentals)
        lt_result = score_long_term(lt_indicators, fundamentals) if lt_indicators else {"signal": "HOLD", "score": 50, "breakdown": {}}
        confidence = calculate_confidence(st_result, lt_result)

        # Previous close for change_pct
        prev_close = float(df.iloc[-2]["close"]) if len(df) >= 2 else None
        current_close = float(df.iloc[-1]["close"])
        change_pct = ((current_close - prev_close) / prev_close) * 100 if prev_close else None

        data_quality = "FULL" if lt_indicators else "TECHNICALS_ONLY"

        stmt = mysql_insert(SignalsCache).values(
            symbol=symbol,
            computed_at=datetime.now(),
            market_session="EOD",
            current_price=current_close,
            prev_close=prev_close,
            change_pct=change_pct,
            st_signal=st_result["signal"],
            st_score=st_result["score"],
            lt_signal=lt_result["signal"],
            lt_score=lt_result["score"],
            confidence_pct=confidence,
            data_quality=data_quality,
            indicator_breakdown={
                "short_term": st_result["breakdown"],
                "long_term": lt_result["breakdown"],
            }
        )
        stmt = stmt.on_duplicate_key_update(
            computed_at=stmt.inserted.computed_at,
            market_session=stmt.inserted.market_session,
            current_price=stmt.inserted.current_price,
            prev_close=stmt.inserted.prev_close,
            change_pct=stmt.inserted.change_pct,
            st_signal=stmt.inserted.st_signal,
            st_score=stmt.inserted.st_score,
            lt_signal=stmt.inserted.lt_signal,
            lt_score=stmt.inserted.lt_score,
            confidence_pct=stmt.inserted.confidence_pct,
            data_quality=stmt.inserted.data_quality,
            indicator_breakdown=stmt.inserted.indicator_breakdown,
        )
        await session.execute(stmt)
        await session.commit()

        logger.info(
            f"{symbol}: ST={st_result['signal']}({st_result['score']}) "
            f"LT={lt_result['signal']}({lt_result['score']}) "
            f"Conf={confidence:.1f}%"
        )


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
            await _recompute_signals_for(sym)

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
                short_summary=insight_data.get("short_summary"),
                long_summary=insight_data.get("long_summary"),
                key_risks=insight_data.get("key_risks", []),
                key_opportunities=insight_data.get("key_opportunities", []),
                sentiment_score=insight_data.get("sentiment_score"),
            )
            session.add(new_insight)
            await session.commit()
            logger.info(f"AI insight ({trigger}) stored for {symbol}.")

    except Exception as e:
        logger.error(f"generate_and_store_insight failed for {symbol}: {e}")


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
