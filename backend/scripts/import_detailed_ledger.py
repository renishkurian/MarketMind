import os
import sys
import json
import asyncio
from datetime import datetime
import logging
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def import_detailed_ledger(file_path: str):
    logger.info(f"Importing detailed ledger from {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
            
        # The file contains multiple JSON objects concatenated like }{
        # We need to turn them into a valid JSON array
        fixed_content = "[" + raw_content.replace("}{", "},{") + "]"
        data_blocks = json.loads(fixed_content)
    except Exception as e:
        logger.error(f"Failed to parse JSON blocks: {e}")
        return

    logger.info(f"Found {len(data_blocks)} JSON blocks in file.")
    
    # Reverse mapping for symbol lookup if needed
    isin_map = {v['isin']: k for k, v in PORTFOLIO_STOCKS.items() if 'isin' in v}
    
    async with SessionLocal() as db:
        imported_count = 0
        
        # 1. Clear existing portfolio transactions to prevent duplicates during re-imports
        await db.execute(text("TRUNCATE TABLE portfolio_transactions"))
        
        for block in data_blocks:
            if not block.get("success") or "data" not in block:
                continue
                
            breakdowns = block["data"].get("unRealisedPnLBreakdowns", [])
            for item in breakdowns:
                isin = item.get("isin", "").strip()
                if not isin:
                    continue
                    
                # Identify Symbol
                symbol = isin_map.get(isin)
                if not symbol:
                    # Fallback to the symbol field from Upstox (which might be generic)
                    symbol = item.get("symbol", "").strip()
                
                qty = float(item.get("openQty", 0))
                if qty <= 0:
                    continue
                    
                avg_rate = float(item.get("avgRate", 0))
                
                bdate_str = item.get("buyDate", "")
                try:
                    buy_date = datetime.strptime(bdate_str, "%d-%m-%Y").date()
                except Exception:
                    buy_date = datetime.utcnow().date()
                    
                # Insert into portfolio_transactions
                insert_stmt = text("""
                    INSERT INTO portfolio_transactions (symbol, isin, quantity, buy_price, buy_date, status)
                    VALUES (:sym, :isin, :qty, :price, :bdate, 'OPEN')
                """)
                await db.execute(insert_stmt, {
                    "sym": symbol, "isin": isin, "qty": qty, 
                    "price": avg_rate, "bdate": buy_date
                })
                imported_count += 1
                
        # 2. Recompute aggregates and update StockMaster
        logger.info(f"Imported {imported_count} individual lots. Updating StockMaster aggregates...")
        
        agg_stmt = text("""
            SELECT symbol, isin, SUM(quantity) as total_qty, 
                   SUM(quantity * buy_price) / SUM(quantity) as weighted_avg_price
            FROM portfolio_transactions 
            WHERE status = 'OPEN'
            GROUP BY symbol, isin
        """)
        
        result = await db.execute(agg_stmt)
        aggregates = result.all()
        
        updated_stocks = 0
        for agg in aggregates:
            sym = agg.symbol
            total_qty = float(agg.total_qty)
            w_avg_price = float(agg.weighted_avg_price)
            
            update_sm = text("""
                UPDATE stocks_master 
                SET quantity = :qty, avg_buy_price = :price, type = 'PORTFOLIO', is_active = 1
                WHERE symbol = :sym OR isin = :isin
            """)
            res = await db.execute(update_sm, {
                "qty": total_qty, "price": w_avg_price, "sym": sym, "isin": agg.isin
            })
            if res.rowcount > 0:
                updated_stocks += 1
                
        await db.commit()
        logger.info(f"Successfully updated {updated_stocks} aggregated records in StockMaster.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "/home/nicky/work/stockmarket/marketwatch/marketmind/backend/portfolio/uptock.txt"
        
    asyncio.run(import_detailed_ledger(file_path))
