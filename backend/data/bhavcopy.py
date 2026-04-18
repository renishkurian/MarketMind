import asyncio
from datetime import datetime, timedelta
import logging
import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.dialects.mysql import insert

from backend.data.db import SessionLocal, PriceHistory, StockMaster, SyncLog, SystemConfig
from backend.utils.market_hours import is_trading_day

# Importers for modular logic
from backend.data.nse_bhavcopy import download_nse_official, parse_nse
from backend.data.bse_bhavcopy import download_bse_samco, parse_bse

logger = logging.getLogger(__name__)

async def get_config(key: str, default: str) -> str:
    """Fetch configuration value from DB."""
    try:
        async with SessionLocal() as session:
            result = await session.execute(select(SystemConfig.value).where(SystemConfig.key == key))
            val = result.scalar()
            return val if val else default
    except Exception as e:
        logger.error(f"Error fetching config {key}: {e}")
        return default

async def load_bhavcopy_to_db(target_date: datetime, sync_type: str = 'MANUAL', exchange: str = 'NSE'):
    """Orchestrates Bhavcopy sync based on system settings."""
    records_count = 0
    status = 'FAILED'
    error_msg = None
    
    try:
        source_key = f"{exchange.upper()}_SOURCE"
        source = await get_config(source_key, "OFFICIAL" if exchange == 'NSE' else "SAMCO")
        
        logger.info(f"Syncing {exchange} from source: {source} for {target_date.date()}")
        
        # Dispatch to specific module logic
        df = None
        if exchange == 'NSE':
            if source == 'OFFICIAL':
                df = await download_nse_official(target_date)
            else:
                # Fallback or Samco logic for NSE if implemented
                df = await download_bse_samco(target_date) # Samco also supports NSE
            clean_df = parse_nse(df, target_date)
        else:
            if source == 'SAMCO':
                df = await download_bse_samco(target_date)
            else:
                raise Exception(f"Official BSE source is currently unsupported. Switch to SAMCO in settings.")
            clean_df = parse_bse(df, target_date)
            
        if clean_df is None or clean_df.empty:
            logger.warning(f"No valid records found for {exchange} on {target_date.date()}")
            status = 'SUCCESS'
        else:
            # Portfolio Filter Check
            sync_mode = await get_config("SYNC_MODE", "ALL")
            if sync_mode == "PORTFOLIO":
                async with SessionLocal() as session:
                    res = await session.execute(select(StockMaster.symbol).where(StockMaster.exchange == exchange))
                    valid_symbols = {row[0] for row in res.all()}
                    clean_df = clean_df[clean_df['symbol'].isin(valid_symbols)].copy()

            records = clean_df.to_dict('records')
            records_count = len(records)
            
            if records_count > 0:
                async with SessionLocal() as session:
                    stmt = insert(PriceHistory).values([{
                        'symbol': r['symbol'],
                        'exchange': r['exchange'],
                        'date': r['date'],
                        'open': r['open'],
                        'high': r['high'],
                        'low': r['low'],
                        'close': r['close'],
                        'volume': r['volume'],
                        'source': 'bhavcopy'
                    } for r in records])
                    
                    update_dict = {
                        'open': stmt.inserted.open,
                        'high': stmt.inserted.high,
                        'low': stmt.inserted.low,
                        'close': stmt.inserted.close,
                        'volume': stmt.inserted.volume,
                        'source': 'bhavcopy'
                    }
                    
                    await session.execute(stmt.on_duplicate_key_update(**update_dict))
                    await session.commit()
                status = 'SUCCESS'
            else:
                status = 'SUCCESS'

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Sync error for {exchange} on {target_date.date()}: {e}")
        status = 'FAILED'
    finally:
        # Record Log
        try:
            async with SessionLocal() as log_session:
                new_log = SyncLog(
                    target_date=target_date.date(),
                    exchange=exchange,
                    sync_type=sync_type,
                    status=status,
                    records_count=records_count,
                    error_message=error_msg[:500] if error_msg else None,
                    completed_at=datetime.utcnow()
                )
                log_session.add(new_log)
                await log_session.commit()
        except Exception as log_err:
            logger.error(f"Failed to record {exchange} log: {log_err}")
            
    return records_count

async def load_historical_bhavcopy(start_date: datetime, end_date: datetime, exchange: str = 'NSE'):
    """Load historical data for a specific exchange."""
    current_date = start_date
    while current_date <= end_date:
        if is_trading_day(current_date):
            await load_bhavcopy_to_db(current_date, sync_type='MANUAL', exchange=exchange)
            await asyncio.sleep(1.0)
        current_date += timedelta(days=1)
