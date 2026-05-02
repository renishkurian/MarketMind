import asyncio
import logging
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date, timedelta
from sqlalchemy import select, delete, and_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Callable, Awaitable, Dict, List

from backend.data.db import (
    SessionLocal, StockMaster, IntradayTicks, SignalsCache,
    PriceHistory, FundamentalsCache, AIInsights, PriceAlert
)
from backend.utils.market_hours import is_market_open, get_current_ist_time
from backend.data.fetcher import fetch_live_prices, fetch_fundamentals
from backend.data.bhavcopy import load_bhavcopy_to_db
from backend.engine.ai_engine import generate_insight
from backend.engine.scoring.composite_score import SectorData
from backend.services.scoring_service import ScoringService

logger = logging.getLogger(__name__)

# ── Broadcast hooks (injected by main.py at startup) ─────────────────────────
broadcast_price_update: Callable[[str, float, float], Awaitable[None]] = None
broadcast_market_status: Callable[[str], Awaitable[None]] = None
broadcast_alert_notification: Callable[[int, dict], Awaitable[None]] = None


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
            select(StockMaster.symbol, StockMaster.yahoo_symbol).where(StockMaster.is_active == True)
        )
        symbols_map = {row[0]: row[1] for row in result.all() if row[1]}

    if not symbols_map:
        return

    prices = await fetch_live_prices(symbols_map)
    if not prices:
        logger.warning("No price data returned from fetcher.")
        return

    # --- 1. Fetch Official Baselines (Yesterday Close) ---
    baselines = {}
    async with SessionLocal() as session:
        # For each symbol, get the most recent EOD close from PriceHistory
        # This prevents % change from being calculated against a stale or missing cache value
        from sqlalchemy import func
        
        # Subquery to get latest date per symbol
        subq = select(
            PriceHistory.symbol, 
            func.max(PriceHistory.date).label("max_date")
        ).where(
            and_(
                PriceHistory.symbol.in_(list(prices.keys())),
                PriceHistory.date < date.today()
            )
        ).group_by(PriceHistory.symbol).subquery()
        
        baseline_query = select(PriceHistory.symbol, PriceHistory.close).join(
            subq, and_(PriceHistory.symbol == subq.c.symbol, PriceHistory.date == subq.c.max_date)
        )
        baseline_res = await session.execute(baseline_query)
        baselines = {row[0]: float(row[1]) for row in baseline_res.all()}

    # --- 2. Record 5-Min Ticks ---
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

        # --- 3. Update SignalsCache (Batch) ---
        cache_updates = []
        for sym, data in prices.items():
            prev_close = baselines.get(sym)
            change_pct = 0.0
            if prev_close and prev_close > 0:
                change_pct = ((data["close"] - prev_close) / prev_close) * 100
            
            cache_updates.append({
                "symbol": sym,
                "exchange": "NSE",
                "computed_at": data["timestamp"],
                "market_session": "LIVE",
                "current_price": data["close"],
                "prev_close": prev_close,
                "change_pct": change_pct
            })
            
            # Trigger immediate broadcast
            await _safe_broadcast_price(sym, data["close"], change_pct)

        if cache_updates:
            # Batch update SignalsCache using UPSERT
            cache_stmt = mysql_insert(SignalsCache).values(cache_updates)
            cache_stmt = cache_stmt.on_duplicate_key_update(
                current_price=cache_stmt.inserted.current_price,
                prev_close=cache_stmt.inserted.prev_close,
                change_pct=cache_stmt.inserted.change_pct,
                computed_at=cache_stmt.inserted.computed_at,
                market_session=cache_stmt.inserted.market_session
            )
            await session.execute(cache_stmt)

        await session.commit()

    logger.info(f"Intraday fetch complete — {len(prices)} symbols updated (Baseline verified).")

    # Intraday alert check — piggybacks on live prices already fetched above
    # Catches alerts that cross intraday but may close back before EOD
    if prices:
        try:
            async with SessionLocal() as alert_session:
                from sqlalchemy import and_
                alert_result = await alert_session.execute(
                    select(PriceAlert).where(
                        and_(PriceAlert.is_active == True, PriceAlert.is_triggered == False)
                    )
                )
                live_alerts = alert_result.scalars().all()
                intraday_triggered = 0
                for alert in live_alerts:
                    live_price = prices.get(alert.symbol, {}).get("close")
                    if live_price is None:
                        continue
                    triggered = (
                        (alert.direction == "ABOVE" and live_price >= alert.price_level) or
                        (alert.direction == "BELOW" and live_price <= alert.price_level)
                    )
                    if triggered:
                        alert.is_triggered    = True
                        alert.triggered_at    = datetime.utcnow()
                        alert.triggered_price = live_price
                        intraday_triggered += 1
                        logger.info(
                            f"INTRADAY ALERT — {alert.symbol} {alert.direction} "
                            f"₹{alert.price_level} | live: ₹{live_price} | user: {alert.user_id}"
                        )
                        try:
                            if broadcast_alert_notification:
                                await broadcast_alert_notification(alert.user_id, {
                                    "id":              alert.id,
                                    "symbol":          alert.symbol,
                                    "label":           alert.label,
                                    "direction":       alert.direction,
                                    "price_level":     float(alert.price_level),
                                    "triggered_price": live_price,
                                    "triggered_at":    datetime.utcnow().isoformat(),
                                })
                                alert.notified = True
                        except Exception as _ne:
                            logger.warning(f"Intraday alert WS push failed: {_ne}")
                if intraday_triggered:
                    await alert_session.commit()
                    logger.info(f"Intraday alert checker: {intraday_triggered} alert(s) triggered.")
        except Exception as _ae:
            logger.error(f"Intraday alert check failed: {_ae}")


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

        # Step 5: Check Price Alerts (User and AI)
        async with SessionLocal() as session:
            await check_price_alerts(session)

        logger.info("EOD consolidation complete.")
    except Exception as e:
        logger.error(f"EOD consolidation failed: {e}")


async def recompute_signals_for(symbol: str, sector_vault: Dict[str, SectorData] = None):
    """Fetch price history from DB, recompute all indicators, update SignalsCache using V2 Engine."""
    async with SessionLocal() as session:
        try:
            # ── V2.1 SCORING SERVICE INTEGRATION ────────────────────────────────────
            # The service already handles:
            # 1. Expert indicator compute (Overall_Trend, LT EMA rec logic)
            # 2. Expert signal mapping (BUY/SELL/HOLD from trends)
            # 3. Persistence of both V2.1 and Legacy columns (indicator_breakdown)
            service = ScoringService(session, profile="long_term_compounding")
            
            # Bug 1 & Fix 2: Trigger backtest if not already persisted
            # Check if backtest_cagr already exists to avoid heavy EOD re-compute
            bt_stmt = select(SignalsCache.backtest_cagr).where(
                and_(SignalsCache.symbol == symbol, SignalsCache.exchange == "NSE")
            )
            bt_res = await session.execute(bt_stmt)
            existing_cagr = bt_res.scalar_one_or_none()
            
            res = await service.score_symbol(symbol, exchange="NSE", run_backtest=(existing_cagr is None))
            
            # Broadcast update if results are successfully persisted
            # We re-fetch to get the final computed columns (like change_pct)
            stmt = select(SignalsCache).where(
                and_(SignalsCache.symbol == symbol, SignalsCache.exchange == "NSE")
            ).order_by(SignalsCache.computed_at.desc()).limit(1)
            
            sig_res = await session.execute(stmt)
            signal_row = sig_res.scalar_one_or_none()
            
            if signal_row and signal_row.current_price and signal_row.change_pct:
                await _safe_broadcast_price(symbol, float(signal_row.current_price), float(signal_row.change_pct))
            
            logger.info(f"{symbol}: [V2.1 Institutional] Score={res.composite_score:.1f} Signal={signal_row.lt_signal if signal_row else 'N/A'}")
            
        except Exception as e:
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
async def run_bulk_fundamental_sync():
    """Logic for bulk fundamental refresh, reusable by scheduler and API."""
    logger.info("Bulk fundamental sync starting…")

    async with SessionLocal() as session:
        result = await session.execute(
            select(StockMaster.symbol, StockMaster.yahoo_symbol).where(StockMaster.is_active == True)
        )
        stocks = result.all()

    for sym, yf_sym in stocks:
        try:
            # Refresh fundamentals using designated Yahoo symbol
            fund_data = await fetch_fundamentals(sym, yahoo_symbol=yf_sym)

            async with SessionLocal() as session:
                # Map to current DB columns
                stmt = mysql_insert(FundamentalsCache).values(
                    symbol=sym,
                    fetched_at=datetime.now(),
                    pe_ratio=fund_data.get("pe_ratio"),
                    eps=fund_data.get("eps"),
                    roe=fund_data.get("roe"),
                    debt_equity=fund_data.get("debt_equity"),
                    revenue_growth=fund_data.get("revenue_growth"), # YoY
                    revenue_growth_3yr=fund_data.get("revenue_growth_3yr"), # CAGR
                    pat_growth_3yr=fund_data.get("pat_growth_3yr"),
                    market_cap=fund_data.get("market_cap"),
                    
                    # -- Institutional Upgrade --
                    peg_ratio=fund_data.get("peg_ratio"),
                    ps_ratio=fund_data.get("ps_ratio"),
                    pb_ratio=fund_data.get("pb_ratio"),
                    ev_ebitda=fund_data.get("ev_ebitda"),
                    book_value=fund_data.get("book_value"),
                    ebitda=fund_data.get("ebitda"),
                    held_percent_institutions=fund_data.get("held_percent_institutions"),
                    shares_outstanding=fund_data.get("shares_outstanding"),
                    
                    # -- Phase 3: Health & Sentiment --
                    analyst_rating=fund_data.get("analyst_rating"),
                    recommendation_key=fund_data.get("recommendation_key"),
                    total_cash=fund_data.get("total_cash"),
                    total_debt=fund_data.get("total_debt"),
                    current_ratio=fund_data.get("current_ratio"),
                    
                    promoter_holding=fund_data.get("promoter_holding"),
                    promoter_pledge_pct=fund_data.get("promoter_pledge_pct"),
                    data_quality=fund_data.get("data_quality", "PARTIAL"), # Hybrid engine is always at least partial
                )
                FUND_COLS = [
                    "fetched_at", "pe_ratio", "eps", "roe", "debt_equity",
                    "revenue_growth", "revenue_growth_3yr", "pat_growth_3yr", 
                    "market_cap", "peg_ratio", "ps_ratio", "pb_ratio", "ev_ebitda",
                    "book_value", "ebitda", "held_percent_institutions", "shares_outstanding",
                    "analyst_rating", "recommendation_key", "total_cash", "total_debt", "current_ratio",
                    "promoter_holding", "promoter_pledge_pct", "data_quality"
                ]
                stmt = stmt.on_duplicate_key_update(
                    **{c: stmt.inserted[c] for c in FUND_COLS}
                )
                await session.execute(stmt)
                await session.commit()

            # Recompute full signals with updated fundamentals
            from backend.services.scoring_service import ScoringService
            async with SessionLocal() as session:
                service = ScoringService(session)
                await service.score_symbol(sym, exchange="NSE")
                
                # -- Phase 3: Push fresh Price Action V3 stats to SignalsCache --
                sig_stmt = mysql_insert(SignalsCache).values(
                    symbol=sym,
                    computed_at=datetime.now(),
                    market_session='EOD',
                    fifty_two_week_high=fund_data.get("fifty_two_week_high"),
                    fifty_two_week_low=fund_data.get("fifty_two_week_low"),
                    fifty_two_week_change=fund_data.get("fifty_two_week_change"),
                    beta=fund_data.get("beta")
                ).on_duplicate_key_update(
                    fifty_two_week_high=fund_data.get("fifty_two_week_high"),
                    fifty_two_week_low=fund_data.get("fifty_two_week_low"),
                    fifty_two_week_change=fund_data.get("fifty_two_week_change"),
                    beta=fund_data.get("beta")
                )
                await session.execute(sig_stmt)
                await session.commit()

            # Generate weekly AI insight (optional but kept as per original weekly flow)
            await _generate_and_store_insight(sym, "WEEKLY")

        except Exception as e:
            logger.error(f"Fundamental sync failed for {sym}: {e}")

async def weekly_refresh():
    logger.info("Triggering automated weekly refresh job.")
    await run_bulk_fundamental_sync()
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
                .order_by(SignalsCache.computed_at.desc()).limit(1)
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
                for k in ["pe_ratio", "eps", "roe", "debt_equity", "revenue_growth", "revenue_growth_3yr"]
            } if fund else {}

        insight_data = await generate_insight(symbol, df, signals, fundamentals, trigger)

        async with SessionLocal() as session:
            new_insight = AIInsights(
                symbol=symbol,
                generated_at=datetime.now(),
                trigger_reason=trigger,
                skill_id=None,
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
        res = await session.execute(
            select(
                StockMaster.sector, 
                FundamentalsCache.pe_ratio, 
                FundamentalsCache.roe, 
                FundamentalsCache.revenue_growth_3yr,
                SignalsCache.change_pct
            )
            .join(FundamentalsCache, StockMaster.symbol == FundamentalsCache.symbol)
            .outerjoin(SignalsCache, StockMaster.symbol == SignalsCache.symbol)
            .where(StockMaster.is_active == True)
        )
        rows = res.all()
        
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


async def check_price_alerts(db: AsyncSession):
    """
    EOD job — checks all active alerts against latest close price.
    Marks triggered alerts and logs them.
    Run after EOD price consolidation completes.
    """
    logger.info("Running EOD price alert checker...")

    # Fetch all active non-triggered alerts
    result = await db.execute(
        select(PriceAlert).where(
            and_(PriceAlert.is_active == True, PriceAlert.is_triggered == False)
        )
    )
    alerts = result.scalars().all()

    if not alerts:
        logger.info("No active alerts to check.")
        return

    # Get unique symbols
    symbols = list({a.symbol for a in alerts})

    # Fetch latest close for each symbol in one query
    from sqlalchemy import func
    subq = (
        select(PriceHistory.symbol, func.max(PriceHistory.date).label("max_date"))
        .where(PriceHistory.symbol.in_(symbols))
        .group_by(PriceHistory.symbol)
        .subquery()
    )
    price_result = await db.execute(
        select(PriceHistory.symbol, PriceHistory.close)
        .join(subq, and_(
            PriceHistory.symbol == subq.c.symbol,
            PriceHistory.date   == subq.c.max_date
        ))
    )
    latest_prices = {row.symbol: float(row.close) for row in price_result}

    triggered_count = 0
    for alert in alerts:
        current_price = latest_prices.get(alert.symbol)
        if current_price is None:
            continue

        triggered = (
            (alert.direction == "ABOVE" and current_price >= alert.price_level) or
            (alert.direction == "BELOW" and current_price <= alert.price_level)
        )

        if triggered:
            alert.is_triggered   = True
            alert.triggered_at   = datetime.utcnow()
            alert.triggered_price= current_price
            triggered_count += 1
            logger.info(
                f"ALERT TRIGGERED — {alert.symbol} {alert.direction} "
                f"₹{alert.price_level} | current: ₹{current_price} | "
                f"user: {alert.user_id} | label: {alert.label}"
            )
            # Push real-time notification to user via WebSocket
            try:
                if broadcast_alert_notification:
                    await broadcast_alert_notification(alert.user_id, {
                        "id":              alert.id,
                        "symbol":          alert.symbol,
                        "label":           alert.label,
                        "direction":       alert.direction,
                        "price_level":     float(alert.price_level),
                        "triggered_price": current_price,
                        "triggered_at":    datetime.utcnow().isoformat(),
                    })
                    alert.notified = True
            except Exception as _e:
                logger.warning(f"Alert WS push failed for user {alert.user_id}: {_e}")

    if triggered_count:
        await db.commit()
        logger.info(f"Alert checker: {triggered_count} alert(s) triggered.")
    else:
        logger.info("Alert checker: no alerts triggered.")


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

    # Added: Manually trigger alert check every hour outside market hours as fallback
    # But usually EOD consolidation is enough.
    
    scheduler.start()
    logger.info("Scheduler started with core jobs: intraday_fetch, eod_consolidation, weekly_refresh")
    return scheduler


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scheduler = start_scheduler()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
