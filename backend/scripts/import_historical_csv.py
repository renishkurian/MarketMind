import os
import sys
import asyncio
import pandas as pd
from datetime import datetime
import logging
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert

# Add project root to python path so we can import from backend
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.data.db import SessionLocal, PriceHistory, StockMaster

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Root directory for extracted archives
ARCHIVE_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "archive", "2015-2026-April17-bhavacopy")

# MySQL limit is 65535 parameters. A row has 9 parameters now (added exchange). 
# 65535 / 9 = 7281 max rows per execute. 
# We use 1000 for safety against max_allowed_packet size.
CHUNK_SIZE = 1000

async def process_chunk(chunk_df, exchange, session):
    """Upsert a chunk of data into price_history."""
    if chunk_df.empty:
        return 0
        
    records = chunk_df.to_dict('records')
    
    # Batch UPSERT
    stmt = insert(PriceHistory).values([{
        'symbol': r['symbol'],
        'exchange': exchange,
        'date': r['date'],
        'open': r['open'],
        'high': r['high'],
        'low': r['low'],
        'close': r['close'],
        'volume': r['volume'],
        'source': 'historical_import'
    } for r in records])
    
    update_dict = {
        'open': stmt.inserted.open,
        'high': stmt.inserted.high,
        'low': stmt.inserted.low,
        'close': stmt.inserted.close,
        'volume': stmt.inserted.volume,
        'source': stmt.inserted.source
    }
    
    stmt = stmt.on_duplicate_key_update(**update_dict)
    await session.execute(stmt)
    await session.commit()
    
    return len(records)

async def import_csv_file(filepath, exchange, session):
    """Processes a single CSV file in chunks."""
    try:
        # Determine file date from name (Format: YYYYMMDD_EXCH.csv)
        filename = os.path.basename(filepath)
        date_str = filename.split('_')[0]
        file_date = datetime.strptime(date_str, '%Y%m%d').date()
        
        logger.info(f"Processing {exchange} data for {file_date} ...")
        
        # Load and rename columns based on exchange
        df = pd.read_csv(filepath)
        if df.empty:
            return 0

        if exchange == 'NSE':
            # Headers: SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN
            if 'SYMBOL' not in df.columns and 'Symbol' in df.columns:
                df = df.rename(columns={'Symbol': 'SYMBOL', 'Series': 'SERIES', 'Open': 'OPEN', 'High': 'HIGH', 'Low': 'LOW', 'Close': 'CLOSE', 'Qty': 'TOTTRDQTY'})
            
            # Filter for Equity
            if 'SERIES' in df.columns:
                df = df[df['SERIES'].astype(str).str.strip() == 'EQ'].copy()
            
            df = df.rename(columns={
                'SYMBOL': 'symbol', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close', 'TOTTRDQTY': 'volume'
            })
        else:
            # BSE Headers: SC_CODE,SC_NAME,SC_GROUP,SC_TYPE,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,NO_TRADES,NO_OF_SHRS,NET_TURNOV,TDCLOINDI
            df = df.rename(columns={
                'SC_CODE': 'symbol', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close', 'NO_OF_SHRS': 'volume', 'SC_TYPE': 'series'
            })
            if 'series' in df.columns:
                df = df[df['series'].astype(str).str.strip() == 'STK'].copy()

        # Common cleaning
        df['symbol'] = df['symbol'].astype(str).str.strip()
        df['date'] = file_date
        
        # Select target columns
        df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        # Process in batches
        total = 0
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            inserted = await process_chunk(chunk, exchange, session)
            total += inserted
            
        return total
    except Exception as e:
        logger.error(f"Error importing {filepath}: {e}")
        return 0

async def main():
    if not os.path.exists(ARCHIVE_ROOT):
        logger.error(f"Archive directory not found: {ARCHIVE_ROOT}")
        return

    logger.info(f"Starting historical import from {ARCHIVE_ROOT}")
    total_nse = 0
    total_bse = 0
    
    async with SessionLocal() as session:
        # Walk through all directories
        for root, dirs, files in os.walk(ARCHIVE_ROOT):
            for file in sorted(files):
                if not file.endswith('.csv'):
                    continue
                    
                exchange = None
                if file.endswith('_NSE.csv'):
                    exchange = 'NSE'
                elif file.endswith('_BSE.csv'):
                    exchange = 'BSE'
                
                if not exchange:
                    continue
                
                filepath = os.path.join(root, file)
                count = await import_csv_file(filepath, exchange, session)
                
                if exchange == 'NSE':
                    total_nse += count
                else:
                    total_bse += count
                
                # Optional: Log every file or every X records
                if count > 0:
                    logger.info(f"Imported {count} records from {file}. Total NSE: {total_nse}, BSE: {total_bse}")

    logger.info("Historical data import process complete!")
    logger.info(f"Summary: NSE Records: {total_nse}, BSE Records: {total_bse}")

if __name__ == "__main__":
    asyncio.run(main())
