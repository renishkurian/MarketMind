import asyncio
from datetime import datetime, timedelta
import logging
import pandas as pd
from sqlalchemy import select, text, update, and_
from sqlalchemy.dialects.mysql import insert

from backend.data.db import (
    SessionLocal, PriceHistory, StockMaster, SyncLog, 
    SystemConfig, SignalsCache
)
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
            async with SessionLocal() as session:
                # Always fetch portfolio mappings (ISIN -> internal_symbol)
                # to ensure we match correctly regardless of what the CSV calls it.
                res = await session.execute(
                    select(StockMaster.isin, StockMaster.symbol)
                    .where(StockMaster.isin != None)
                )
                # Create a map of ISIN -> internal symbol
                isin_to_symbol = {row[0]: row[1] for row in res.all()}
                sync_mode = await get_config("SYNC_MODE", "ALL")
                
                if sync_mode == "PORTFOLIO":
                    if 'isin' in clean_df.columns:
                        clean_df['internal_symbol'] = clean_df['isin'].map(isin_to_symbol)
                        clean_df = clean_df[clean_df['internal_symbol'].notna()].copy()
                    else:
                        res_sym = await session.execute(select(StockMaster.symbol))
                        valid_symbols = {row[0] for row in res_sym.all()}
                        clean_df = clean_df[clean_df['symbol'].isin(valid_symbols)].copy()
                        clean_df['internal_symbol'] = clean_df['symbol']
                else:
                    if 'isin' in clean_df.columns:
                        clean_df['internal_symbol'] = clean_df['isin'].map(isin_to_symbol).fillna(clean_df['symbol'])
                    else:
                        clean_df['internal_symbol'] = clean_df['symbol']

            records = clean_df.to_dict('records')
            records_count = len(records)
            
            if records_count > 0:
                async with SessionLocal() as session:
                    # Map records to PriceHistory schema
                    price_records = []
                    for r in records:
                        price_records.append({
                            'symbol': r['internal_symbol'],
                            'isin': r.get('isin'),
                            'exchange': r['exchange'],
                            'date': r['date'],
                            'open': r['open'],
                            'high': r['high'],
                            'low': r['low'],
                            'close': r['close'],
                            'volume': r['volume'],
                            'no_of_trades': r.get('no_of_trades'),
                            'source': 'bhavcopy'
                        })
                    
                    stmt = insert(PriceHistory).values(price_records)
                    
                    update_dict = {
                        'isin': stmt.inserted.isin,
                        'open': stmt.inserted.open,
                        'high': stmt.inserted.high,
                        'low': stmt.inserted.low,
                        'close': stmt.inserted.close,
                        'volume': stmt.inserted.volume,
                        'no_of_trades': stmt.inserted.no_of_trades,
                        'source': 'bhavcopy'
                    }
                    
                    await session.execute(stmt.on_duplicate_key_update(**update_dict))
                    
                    # ── Phase 3.5: Sync prev_close to SignalsCache ───────────────────
                    # We also update the reference price so P&L is accurate next session.
                    # This handles the "relay on bhavcopy if closed" requirement.
                    sig_records = [{
                        'symbol': r['internal_symbol'],
                        'prev_close': r['close'],
                        'computed_at': r['date'],
                        'market_session': 'EOD'
                    } for r in records]
                    
                    sig_stmt = insert(SignalsCache).values(sig_records)
                    sig_update = {
                        'prev_close': sig_stmt.inserted.prev_close,
                        'computed_at': sig_stmt.inserted.computed_at,
                        'market_session': 'EOD'
                    }
                    await session.execute(sig_stmt.on_duplicate_key_update(**sig_update))
                    
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
