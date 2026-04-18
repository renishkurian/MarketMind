import asyncio
import os
import sys
import logging
from datetime import datetime
import yfinance as yf
import pandas as pd

# SETUP LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("backfill")

# Add project root to path
sys.path.append(os.getcwd())

from backend.data.db import SessionLocal, StockMaster, SignalsCache
from backend.engine.indicators import compute_short_term_indicators, compute_long_term_indicators
from backend.engine.scorer import score_short_term, score_long_term, calculate_confidence
from sqlalchemy import select

async def backfill():
    logger.info("Starting Price Backfill for 52 stocks...")
    async with SessionLocal() as db:
        # Get all master symbols
        res = await db.execute(select(StockMaster))
        stocks = res.scalars().all()
        symbols = [s.symbol for s in stocks]
        logger.info(f"Targeting {len(symbols)} symbols")

        for symbol in symbols:
            try:
                # Fetch yfinance data (need ~1 year for long term indicators)
                ticker = yf.Ticker(f"{symbol}.NS")
                data = ticker.history(period="1y")
                
                if data.empty or len(data) < 2:
                    logger.warning(f"No data for {symbol}")
                    continue
                
                # Cleanup data for indicators (lower case columns)
                df = data.copy()
                df.columns = [c.lower() for c in df.columns]
                df.reset_index(inplace=True)
                df.rename(columns={'Date': 'date'}, inplace=True)

                current_price = df['close'].iloc[-1]
                prev_close = df['close'].iloc[-2]
                change_pct = ((current_price - prev_close) / prev_close) * 100

                # Compute Indicators
                st_indicators = compute_short_term_indicators(df)
                lt_indicators = compute_long_term_indicators(df)
                
                # Compute Scores (pass empty fundamentals for now)
                st_result = score_short_term(st_indicators, {})
                lt_result = score_long_term(lt_indicators, {})
                confidence = calculate_confidence(st_result, lt_result)
                
                # Upsert SignalsCache
                q = select(SignalsCache).where(SignalsCache.symbol == symbol)
                res_sig = await db.execute(q)
                sig_record = res_sig.scalars().first()

                if not sig_record:
                    sig_record = SignalsCache(symbol=symbol)
                    db.add(sig_record)

                sig_record.current_price = float(current_price)
                sig_record.change_pct = float(change_pct)
                sig_record.prev_close = float(prev_close)
                sig_record.st_signal = st_result['signal']
                sig_record.lt_signal = lt_result['signal']
                sig_record.st_score = float(st_result['score'])
                sig_record.lt_score = float(lt_result['score'])
                sig_record.confidence_pct = float(confidence)
                sig_record.indicator_breakdown = {
                    "short_term": st_result['breakdown'],
                    "long_term": lt_result['breakdown']
                }
                sig_record.computed_at = datetime.now()
                sig_record.market_session = "BACKFILL"
                sig_record.data_quality = "FULL" if len(df) > 200 else "PARTIAL"

                await db.commit()
                logger.info(f"✅ Updated {symbol}: ₹{current_price:.2f} ({change_pct:.2f}%)")
                
                # Tiny sleep to be polite to YF
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"❌ Failed {symbol}: {e}")

    logger.info("Backfill complete.")

if __name__ == "__main__":
    asyncio.run(backfill())
