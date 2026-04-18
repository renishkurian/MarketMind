import json
import asyncio
import os
import sys
from datetime import datetime
from decimal import Decimal
from sqlalchemy import select, update, delete, func

# Add parent dir to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal, StockMaster, PortfolioTransaction, SignalsCache

async def rebuild_stocks_master(file_path):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    print(f"--- RESET & REBUILD INITIATED ---")
    
    # 1. Parsing all JSON blocks from uptock.txt
    print(f"Parsing {file_path}...")
    with open(file_path, 'r') as f:
        full_content = f.read()

    import re
    blocks = []
    # This regex matches the start of each JSON response object
    for match in re.finditer(r'\{\s*"success":\s*true', full_content):
        start = match.start()
        blocks.append(start)
    
    all_lots_map = {} # (isin, buyDate, openQty, avgRate) -> lot_dict
    success_count = 0
    
    for i in range(len(blocks)):
        start = blocks[i]
        end = blocks[i+1] if i+1 < len(blocks) else len(full_content)
        
        block = full_content[start:end].strip()
        if block.endswith('{'): block = block[:-1].strip()
        if not block.endswith('}'):
            last_brace = block.rfind('}')
            if last_brace != -1: block = block[:last_brace+1]
            
        try:
            obj = json.loads(block)
            lots = obj.get("data", {}).get("unRealisedPnLBreakdowns", [])
            for lot in lots:
                # Deduplicate by unique transaction finger print
                key = (
                    lot.get('isin'),
                    lot.get('buyDate', 'N/A'),
                    lot.get('openQty', 0),
                    lot.get('avgRate', 0)
                )
                all_lots_map[key] = lot
            success_count += 1
        except json.JSONDecodeError:
            continue

    all_lots = list(all_lots_map.values())
    print(f"Successfully parsed {success_count} JSON blocks.")
    print(f"Total unique transaction lots captured: {len(all_lots)}")

    async with SessionLocal() as db:
        # 2. BACKUP current metadata
        print("Backing up current metadata (Watchlist, Sectors)...")
        res = await db.execute(select(StockMaster))
        current_stocks = res.scalars().all()
        
        backup_data = {} # symbol -> {sector, is_active, type}
        for s in current_stocks:
            backup_data[s.symbol] = {
                "sector": s.sector,
                "is_active": s.is_active,
                "type": s.type,
                "market_cap_cat": s.market_cap_cat
            }
        
        # 3. WIPE everything
        print("Wiping existing records from stocks_master and portfolio_transactions...")
        await db.execute(delete(PortfolioTransaction))
        await db.execute(delete(SignalsCache))
        await db.execute(delete(StockMaster))
        await db.commit()

        # 4. REBUILD Master from all_lots
        print("Rebuilding StockMaster entries from file...")
        seen_isins = {} # isin -> symbol
        unique_stocks = []
        
        # First pass: collect unique stocks
        for lot in all_lots:
            isin = lot.get("isin")
            symbol = lot.get("symbol")
            scp_name = lot.get("scpName")
            
            if isin not in seen_isins:
                seen_isins[isin] = symbol
                
                # Check for backup
                meta = backup_data.get(symbol, {})
                
                stock = StockMaster(
                    symbol=symbol,
                    company_name=scp_name, # User wants scpName as primary
                    scp_name=scp_name,
                    isin=isin,
                    exchange='BSE' if lot.get("closingRateExchange") == 'BSE' else 'NSE',
                    sector=meta.get("sector"),
                    market_cap_cat=meta.get("market_cap_cat") or 'UNKNOWN',
                    type='PORTFOLIO', # These are all portfolio stocks
                    added_date=datetime.now().date(),
                    is_active=True,
                    quantity=0,
                    avg_buy_price=0
                )
                unique_stocks.append(stock)

        # 5. RESTORE non-portfolio watchlist stocks
        print("Restoring watchlist-only stocks from backup...")
        for sym, meta in backup_data.items():
            if meta.get("type") == 'WATCHLIST':
                # Only add if it wasn't already added as a portfolio stock
                if not any(s.symbol == sym for s in unique_stocks):
                    stock = StockMaster(
                        symbol=sym,
                        company_name=sym, # We don't have scpName for watchlist-only
                        type='WATCHLIST',
                        sector=meta.get("sector"),
                        market_cap_cat=meta.get("market_cap_cat") or 'UNKNOWN',
                        added_date=datetime.now().date(),
                        is_active=meta.get("is_active", True),
                        quantity=0,
                        avg_buy_price=0
                    )
                    unique_stocks.append(stock)

        db.add_all(unique_stocks)
        await db.commit()
        print(f"Created {len(unique_stocks)} stock master entries.")

        # 6. IMPORT Transactions
        print("Importing transaction lots...")
        for lot in all_lots:
            isin = lot.get("isin")
            symbol = lot.get("symbol")
            qty = Decimal(str(lot.get("openQty", 0)))
            price = Decimal(str(lot.get("avgRate", 0)))
            buy_date_str = lot.get("buyDate")
            
            try:
                buy_date = datetime.strptime(buy_date_str, "%d-%m-%Y").date()
            except:
                buy_date = datetime.now().date()

            tx = PortfolioTransaction(
                symbol=symbol,
                isin=isin,
                quantity=qty,
                buy_price=price,
                buy_date=buy_date,
                status='OPEN'
            )
            db.add(tx)
        
        await db.commit()

        # 7. AGGREGATE
        print("Resyncing portfolio metrics...")
        stocks = (await db.execute(select(StockMaster).where(StockMaster.type == 'PORTFOLIO'))).scalars().all()
        for stock in stocks:
            lots_res = await db.execute(
                select(PortfolioTransaction)
                .where(PortfolioTransaction.symbol == stock.symbol)
            )
            lots_data = lots_res.scalars().all()
            if lots_data:
                total_q = sum(l.quantity for l in lots_data)
                weighted_p = sum(l.quantity * l.buy_price for l in lots_data)
                stock.quantity = total_q
                stock.avg_buy_price = (weighted_p / total_q) if total_q > 0 else 0
                stock.buy_date = min(l.buy_date for l in lots_data)

        await db.commit()
        print("--- REBUILD COMPLETE! ---")

if __name__ == "__main__":
    file_path = "/home/nicky/work/stockmarket/marketwatch/marketmind/backend/portfolio/uptock.txt"
    asyncio.run(rebuild_stocks_master(file_path))
