import os
import sys
import asyncio
import logging
from datetime import datetime

# Dependency check
try:
    import pandas as pd
except ImportError:
    print("Error: 'pandas' not found. Please run this script using the project's virtual environment:")
    print("  ./.venv/bin/python3 -m backend.scripts.import_historical_csv")
    sys.exit(1)

# Add project root to python path so we can import from backend
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from backend.data.db import SessionLocal, PriceHistory
    from sqlalchemy.dialects.mysql import insert
except ImportError:
    print("Error: Could not import backend modules. Ensure you are running from the project root.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Root directory for extracted archives
ARCHIVE_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "archive", "2015-2026-April17-bhavacopy")
CHUNK_SIZE = 1000

async def process_chunk(chunk_df, exchange, session):
    """Upsert a chunk of data into price_history."""
    if chunk_df.empty:
        return 0
        
    records = chunk_df.to_dict('records')
    
    try:
        # Batch UPSERT attempt
        stmt = insert(PriceHistory).values([{
            'symbol': str(r['symbol']),
            'exchange': exchange,
            'date': r['date'],
            'open': r['open'],
            'high': r['high'],
            'low': r['low'],
            'close': r['close'],
            'volume': int(r['volume']) if pd.notnull(r['volume']) else 0,
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
    except Exception as e:
        await session.rollback()
        logger.error(f"Bulk insert failed for {exchange} chunk: {e}")
        return 0

async def import_csv_file(filepath, exchange, session):
    """Processes a single CSV file in chunks and handles data cleaning."""
    try:
        # Determine file date from name (Format: YYYYMMDD_EXCH.csv)
        filename = os.path.basename(filepath)
        date_str = filename.split('_')[0]
        file_date = datetime.strptime(date_str, '%Y%m%d').date()
        
        # Load data
        df = pd.read_csv(filepath)
        if df.empty:
            return 0

        # Normalize column names to uppercase for easier mapping
        df.columns = [c.strip().upper() for c in df.columns]

        if exchange == 'NSE':
            # Support common variations in Bhavcopy headers
            mapping = {
                'SYMBOL': 'symbol', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close', 
                'TOTTRDQTY': 'volume', 'QTY': 'volume'
            }
            df = df.rename(columns=mapping)
            
            # Filter for Equity (EQ) series - very important for NSE
            if 'SERIES' in df.columns:
                df = df[df['SERIES'].astype(str).str.strip().isin(['EQ', 'BE'])].copy()
        else:
            # BSE Mapping
            mapping = {
                'SC_CODE': 'symbol', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close', 
                'NO_OF_SHRS': 'volume', 'SC_TYPE': 'series'
            }
            df = df.rename(columns=mapping)
            if 'series' in df.columns:
                df = df[df['series'].astype(str).str.strip() == 'STK'].copy()

        # --- ROBUST DATA CLEANING ---
        if 'symbol' not in df.columns or 'close' not in df.columns:
            logger.warning(f"File {filename} missing critical columns. Skipping.")
            return 0

        # 1. Clean Symbol
        df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
        df = df[df['symbol'] != '']

        # 2. Add Date
        df['date'] = file_date

        # 3. Clean Numeric Columns (Handle commas and non-numeric garbage)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                # Remove commas from strings like "1,234.50"
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                # Coerce to numeric, setting errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = pd.NA

        # 4. Handle NaNs in Price Data
        # We MUST have a close price.
        df = df.dropna(subset=['symbol', 'close'])
        
        # If open/high/low are missing, default them to close
        for col in ['open', 'high', 'low']:
            df[col] = df[col].fillna(df['close'])
        
        # volume defaults to 0
        df['volume'] = df['volume'].fillna(0)

        # 5. Final selection (Ensure fixed column order)
        df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        # Process in batches
        total_inserted = 0
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            inserted = await process_chunk(chunk, exchange, session)
            total_inserted += inserted
            
        return total_inserted
    except Exception as e:
        logger.error(f"Error importing {filepath}: {e}")
        return 0

async def main():
    if not os.path.exists(ARCHIVE_ROOT):
        logger.error(f"Archive directory not found: {ARCHIVE_ROOT}")
        return

    logger.info(f"Starting robust historical import from {ARCHIVE_ROOT}")
    total_nse = 0
    total_bse = 0
    
    async with SessionLocal() as session:
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
                
                if count > 0:
                    logger.info(f"Processed {file}: {count} records. [Total NSE: {total_nse}, BSE: {total_bse}]")

    logger.info("Import process complete!")

if __name__ == "__main__":
    asyncio.run(main())
