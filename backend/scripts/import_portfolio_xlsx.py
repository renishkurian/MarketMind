import pandas as pd
import os
import sys
import asyncio
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal, StockMaster
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS
from backend.main import _bootstrap_new_stock

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def import_portfolio(file_path: str):
    logger.info(f"Importing holdings from {file_path}")
    
    # ISIN to Symbol reverse mapping
    isin_map = {v['isin']: k for k, v in PORTFOLIO_STOCKS.items() if 'isin' in v}
    
    try:
        # Read Excel skipping the Upstox headers (row 9 is index 8, but we found skiprows=9 works for header)
        df = pd.read_excel(file_path, skiprows=9)
    except Exception as e:
        logger.error(f"Failed to read excel file: {e}")
        return

    logger.info(f"Found {len(df)} rows in Excel file.")
    
    async with SessionLocal() as db:
        imported_count = 0
        for _, row in df.iterrows():
            isin = str(row.get('ISIN', '')).strip()
            if not isin or isin.lower() == 'nan':
                continue
                
            scrip_name = str(row.get('Scrip Name', 'Unknown'))
            qty = float(row.get('Current Qty', 0))
            if pd.isna(qty) or qty <= 0:
                continue
                
            avg_price = float(row.get('Rate', 0))
            val_date_str = str(row.get('Value Date', ''))
            
            try:
                buy_date = datetime.strptime(val_date_str, "%d-%m-%Y").date()
            except:
                buy_date = datetime.utcnow().date()
            
            # Determine symbol
            symbol = isin_map.get(isin)
            if not symbol:
                # Guess from Scrip Name (e.g. 'AETHER INDUSTRIES-EQ' -> 'AETHER')
                symbol_guess = scrip_name.split('-')[0].split(' ')[0].strip().upper()
                if not symbol_guess:
                   logger.warning(f"Could not map ISIN {isin} ({scrip_name}) to a symbol.")
                   continue
                symbol = symbol_guess
                
            # Upsert into StockMaster
            # Wait, SQLAlchemy AsyncSession doesn't have an easy on_duplicate_key_update
            # We'll select and update or insert
            stmt = "SELECT id FROM stocks_master WHERE symbol = :symbol OR isin = :isin"
            from sqlalchemy import text
            result = await db.execute(text(stmt), {"symbol": symbol, "isin": isin})
            existing = result.scalar()
            
            if existing: # Update
                update_stmt = text("""
                    UPDATE stocks_master 
                    SET type = 'PORTFOLIO', is_active = 1, quantity = :qty, avg_buy_price = :price, buy_date = :bdate, isin = :isin, company_name = :name
                    WHERE id = :id
                """)
                await db.execute(update_stmt, {
                    "qty": qty, "price": avg_price, "bdate": buy_date, 
                    "isin": isin, "name": scrip_name, "id": existing
                })
            else: # Insert
                insert_stmt = text("""
                    INSERT INTO stocks_master (symbol, exchange, company_name, isin, type, added_date, is_active, quantity, avg_buy_price, buy_date)
                    VALUES (:sym, 'NSE', :name, :isin, 'PORTFOLIO', :bdate, 1, :qty, :price, :bdate)
                """)
                await db.execute(insert_stmt, {
                    "sym": symbol, "name": scrip_name, "isin": isin, 
                    "bdate": buy_date, "qty": qty, "price": avg_price
                })
                
            imported_count += 1
            
            # Bootstrap data for the new stock in background
            # asyncio.create_task(_bootstrap_new_stock(symbol))
            
        await db.commit()
        logger.info(f"Successfully processed {imported_count} holdings.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "/home/nicky/Downloads/holdings_01-01-2025_DT2105.xlsx"
        
    asyncio.run(import_portfolio(file_path))
