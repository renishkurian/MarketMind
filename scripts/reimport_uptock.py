import json
import asyncio
import os
import sys
from datetime import datetime
from decimal import Decimal
from sqlalchemy import select, update, delete

# Add parent dir to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal, StockMaster, PortfolioTransaction, Base

async def reimport_uptock(file_path):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    print(f"Loading portfolio from {file_path}...")
    with open(file_path, 'r') as f:
        content = f.read().strip()

    decoder = json.JSONDecoder()
    pos = 0
    all_holdings = []
    
    while pos < len(content):
        try:
            # Skip any leading whitespace or newlines between objects
            content = content[pos:].lstrip()
            if not content:
                break
            obj, pos = decoder.raw_decode(content)
            holdings = obj.get("data", {}).get("unRealisedPnLBreakdowns", [])
            all_holdings.extend(holdings)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON at position {pos}: {e}")
            print(f"Context: {content[:100]}...")
            break

    if not all_holdings:
        print("No holdings found in the file.")
        return

    print(f"Found {len(all_holdings)} transaction lots across all data blocks.")

    async with SessionLocal() as db:
        # 1. Clean up existing portfolio data
        print("Cleaning existing portfolio data...")
        await db.execute(delete(PortfolioTransaction))
        # Reset all stocks that were portfolio type
        await db.execute(
            update(StockMaster)
            .where(StockMaster.type == 'PORTFOLIO')
            .values(quantity=0, avg_buy_price=0, buy_date=None, type='WATCHLIST')
        )
        await db.commit()

        # 2. Map ISINs in DB
        res = await db.execute(select(StockMaster))
        all_stocks = res.scalars().all()
        db_stocks = {s.isin: s for s in all_stocks if s.isin}
        db_symbols = {s.symbol: s for s in all_stocks}

        processed_isins = set()
        print("Importing lots...")
        for lot in all_holdings:
            isin = lot.get("isin")
            symbol = lot.get("symbol")
            scp_name = lot.get("scpName")
            qty = Decimal(str(lot.get("openQty", 0)))
            price = Decimal(str(lot.get("avgRate", 0)))
            buy_date_str = lot.get("buyDate") # format DD-MM-YYYY
            
            try:
                buy_date = datetime.strptime(buy_date_str, "%d-%m-%Y").date()
            except:
                buy_date = datetime.now().date()

            # Find matching stock
            stock = db_stocks.get(isin)
            if not stock:
                # Try fallback to symbol (risky but better than nothing)
                stock = db_symbols.get(symbol)
            
            if not stock:
                # Create new stock entry
                print(f"  Creating new stock entry for {symbol} ({isin})")
                stock = StockMaster(
                    symbol=symbol,
                    company_name=scp_name,
                    scp_name=scp_name,
                    isin=isin,
                    type='PORTFOLIO',
                    added_date=datetime.now().date(),
                    is_active=True,
                    quantity=0,
                    avg_buy_price=0
                )
                db.add(stock)
                await db.flush() # Get ID
                db_stocks[isin] = stock
            else:
                # Update existing stock
                stock.scp_name = scp_name
                stock.is_active = True
                stock.type = 'PORTFOLIO'
                processed_isins.add(isin)

            # Create transaction
            tx = PortfolioTransaction(
                symbol=stock.symbol,
                isin=isin,
                quantity=qty,
                buy_price=price,
                buy_date=buy_date,
                status='OPEN'
            )
            db.add(tx)

        await db.commit()
        print("Transactions imported. Re-calculating aggregations...")

        # 3. Aggregate totals back into StockMaster
        # We'll do it symbol by symbol for clarity
        stocks_to_update = await db.execute(select(StockMaster).where(StockMaster.type == 'PORTFOLIO'))
        for stock in stocks_to_update.scalars().all():
            tx_res = await db.execute(
                select(PortfolioTransaction)
                .where(PortfolioTransaction.symbol == stock.symbol)
                .where(PortfolioTransaction.status == 'OPEN')
            )
            lots = tx_res.scalars().all()
            if not lots:
                continue
                
            total_qty = sum(l.quantity for l in lots)
            if total_qty > 0:
                weighted_sum = sum(l.quantity * l.buy_price for l in lots)
                avg_price = weighted_sum / total_qty
                first_date = min(l.buy_date for l in lots)
                
                stock.quantity = total_qty
                stock.avg_buy_price = round(avg_price, 2)
                stock.buy_date = first_date
        
        await db.commit()
        print("Re-import complete!")

if __name__ == "__main__":
    file_path = "/home/nicky/work/stockmarket/marketwatch/marketmind/backend/portfolio/uptock.txt"
    asyncio.run(reimport_uptock(file_path))
