import asyncio
import logging
from datetime import date
from sqlalchemy import select, and_, func
from backend.data.db import SessionLocal, StockMaster, SignalsCache, PriceHistory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def sync_all_reference_prices():
    """
    Mass-synchronizes SignalsCache.prev_close with the latest record in PriceHistory.
    This corrects Day P0L alignment errors (e.g. Monday comparing against Thursday instead of Friday).
    """
    today = date.today()
    logger.info(f"Starting mass sync of reference prices as of {today}...")

    async with SessionLocal() as session:
        # 1. Fetch all active symbols
        res = await session.execute(
            select(StockMaster.symbol, StockMaster.exchange)
            .where(StockMaster.is_active == True)
        )
        stocks = res.all()
        logger.info(f"Found {len(stocks)} active stocks to check.")

        updated_count = 0
        for symbol, exchange in stocks:
            # 2. Get the most recent close BEFORE today
            # We use PriceHistory as the source of truth for "Yesterday"
            ph_stmt = (
                select(PriceHistory.close, PriceHistory.date)
                .where(
                    and_(
                        PriceHistory.symbol == symbol,
                        PriceHistory.exchange == exchange,
                        PriceHistory.date < today
                    )
                )
                .order_by(PriceHistory.date.desc())
                .limit(1)
            )
            ph_res = await session.execute(ph_stmt)
            latest_close = ph_res.fetchone()

            if not latest_close:
                logger.debug(f"No history found for {symbol} before {today}. Skipping.")
                continue

            ref_price = float(latest_close.close)
            ref_date = latest_close.date

            # 3. Update SignalsCache
            sig_stmt = select(SignalsCache).where(
                and_(SignalsCache.symbol == symbol, SignalsCache.exchange == exchange)
            )
            sig_res = await session.execute(sig_stmt)
            cache = sig_res.scalar_one_or_none()

            if cache:
                if cache.prev_close != ref_price:
                    logger.info(f"Syncing {symbol}: {cache.prev_close} -> {ref_price} (from {ref_date})")
                    cache.prev_close = ref_price
                    
                    # Recalculate change_pct if current_price exists
                    if cache.current_price and ref_price > 0:
                        cache.change_pct = ((float(cache.current_price) - ref_price) / ref_price) * 100
                    
                    updated_count += 1
            else:
                # If no cache exists, we create a skeleton one with the reference price
                new_cache = SignalsCache(
                    symbol=symbol,
                    exchange=exchange,
                    prev_close=ref_price,
                    market_session='EOD'
                )
                session.add(new_cache)
                updated_count += 1

        await session.commit()
        logger.info(f"Sync complete! {updated_count} symbols aligned.")

if __name__ == "__main__":
    asyncio.run(sync_all_reference_prices())
