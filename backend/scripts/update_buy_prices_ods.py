import pandas as pd
import os
import sys
import asyncio
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def update_avg_prices(file_path: str):
    logger.info(f"Importing P&L from {file_path}")
    
    try:
        # Read ODS skipping the Upstox headers
        df = pd.read_excel(file_path, engine='odf', skiprows=9)
    except Exception as e:
        logger.error(f"Failed to read ods file: {e}")
        return

    logger.info(f"Found {len(df)} rows in ODS file.")
    
    # Standardize column names if Unnamed
    cols = ['scrip_name', 'scrip_code', 'buy_date', 'symbol', 'isin', 'scrip_opt', 
            'open_qty', 'avg_rate', 'open_amt', 'closing_rate_exch', 'closing_rate_date', 
            'closing_rate', 'closing_amt', 'unrealized_pnl', 'unrealized_pnl_pct']
    
    if len(df.columns) == len(cols):
        df.columns = cols
    
    async with SessionLocal() as db:
        updated_count = 0
        from sqlalchemy import text
        for _, row in df.iterrows():
            isin = str(row.get('isin', '')).strip()
            if not isin or isin.lower() == 'nan':
                continue
                
            raw_avg = row.get('avg_rate', 0)
            if pd.isna(raw_avg):
                continue
            try:
                avg_price = float(raw_avg)
            except (ValueError, TypeError):
                continue
            
            if avg_price <= 0:
                continue
            if pd.isna(avg_price) or avg_price <= 0:
                continue
                
            bdate_str = str(row.get('buy_date', ''))
            buy_date = None
            try:
                if bdate_str and bdate_str.lower() != 'nan':
                   buy_date = datetime.strptime(bdate_str, "%d-%m-%Y").date()
            except Exception as e:
                pass
            
            symbol = str(row.get('symbol', '')).strip()
            
            # Upsert into StockMaster
            update_stmt = text("""
                UPDATE stocks_master 
                SET avg_buy_price = :price, buy_date = COALESCE(:bdate, buy_date)
                WHERE isin = :isin OR symbol = :symbol
            """)
            
            result = await db.execute(update_stmt, {
                "price": avg_price, "bdate": buy_date, 
                "isin": isin, "symbol": symbol
            })
            
            if result.rowcount > 0:
                updated_count += result.rowcount
            
        await db.commit()
        logger.info(f"Successfully updated prices for {updated_count} holdings.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "/home/nicky/work/stockmarket/marketwatch/marketmind/backend/portfolio/unrealizedPnL_EQ_2026-04-01_To_2026-04-18_DT2105.ods"
        
    asyncio.run(update_avg_prices(file_path))
