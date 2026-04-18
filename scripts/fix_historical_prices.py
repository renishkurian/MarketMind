import asyncio
import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add parent dir to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.data.db import engine, SessionLocal, PriceHistory, StockMaster, SignalsCache
from backend.data.nse_bhavcopy import download_nse_official, parse_nse
from backend.utils.market_hours import is_trading_day
from sqlalchemy import select, update, delete
from sqlalchemy.dialects.mysql import insert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_int(val):
    if val is None: return None
    try:
        f_val = float(val)
        if np.isnan(f_val): return None
        return int(f_val)
    except:
        return None

def safe_float(val):
    if val is None: return 0.0
    try:
        f_val = float(val)
        if np.isnan(f_val): return 0.0
        return f_val
    except:
        return 0.0

async def fix_prices(lookback_days=5):
    load_dotenv('backend/.env')
    
    # 1. Determine target dates
    target_dates = []
    curr = datetime.now()
    while len(target_dates) < lookback_days:
        if is_trading_day(curr):
            target_dates.append(curr)
        curr -= timedelta(days=1)
    
    target_dates.sort()
    
    # 2. Get active portfolio ISINs
    async with SessionLocal() as session:
        result = await session.execute(
            select(StockMaster.isin, StockMaster.symbol).where(StockMaster.type == 'PORTFOLIO', StockMaster.is_active == 1)
        )
        data = result.all()
        isin_to_sym = {row.isin: row.symbol for row in data if row.isin}
        portfolio_isins = set(isin_to_sym.keys())

    # 3. Sync PriceHistory
    for t_date in target_dates:
        t_d = t_date.date()
        logger.info(f"Syncing NSE Bhavcopy for {t_d}...")
        try:
            df = await download_nse_official(t_date)
            if df is None:
                continue
            df_parsed = parse_nse(df, t_date)
            portfolio_df = df_parsed[df_parsed['isin'].isin(portfolio_isins)].copy()
            if portfolio_df.empty:
                continue
            
            async with SessionLocal() as session:
                # Clean old
                await session.execute(
                    delete(PriceHistory).where(PriceHistory.date == t_d, PriceHistory.isin.in_(portfolio_isins))
                )
                
                records = []
                for _, row in portfolio_df.iterrows():
                    sym = isin_to_sym.get(row['isin'], row['symbol'])
                    records.append({
                        'symbol': sym,
                        'isin': row['isin'],
                        'exchange': 'NSE',
                        'date': row['date'],
                        'open': safe_float(row['open']),
                        'high': safe_float(row['high']),
                        'low': safe_float(row['low']),
                        'close': safe_float(row['close']),
                        'volume': int(safe_float(row['volume'])),
                        'no_of_trades': safe_int(row.get('no_of_trades')),
                        'source': 'bhavcopy'
                    })
                if records:
                    await session.execute(insert(PriceHistory).values(records))
                await session.commit()
            logger.info(f"Fixed {len(records)} records for {t_d}")
        except Exception as e:
            logger.error(f"Failed {t_d}: {e}")

    # 4. Update SignalsCache
    if len(target_dates) >= 2:
        today_date = target_dates[-1].date()
        yesterday_date = target_dates[-2].date()
        logger.info(f"Updating SignalsCache: {today_date} vs {yesterday_date}")
        
        async with SessionLocal() as session:
            for isin, sym in isin_to_sym.items():
                res_t = await session.execute(
                    select(PriceHistory.close).where(PriceHistory.isin == isin, PriceHistory.date == today_date)
                )
                tc = res_t.scalar()
                
                res_y = await session.execute(
                    select(PriceHistory.close).where(PriceHistory.isin == isin, PriceHistory.date == yesterday_date)
                )
                pc = res_y.scalar()
                
                if tc is not None and pc is not None:
                    tc, pc = float(tc), float(pc)
                    change = ((tc - pc) / pc) * 100 if pc > 0 else 0
                    await session.execute(
                        update(SignalsCache)
                        .where(SignalsCache.symbol == sym)
                        .values(
                            current_price=tc,
                            prev_close=pc,
                            change_pct=change,
                            computed_at=datetime.now(),
                            market_session='EOD'
                        )
                    )
            await session.commit()
        logger.info("Baseline alignment complete.")

async def main():
    try:
        await fix_prices()
    finally:
        # Proper cleanup of engine to avoid Event Loop errors
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
