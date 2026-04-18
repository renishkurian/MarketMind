import asyncio
import logging
import sys
import os
from sqlalchemy import select

# Add parent dir to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal, StockMaster
from backend.scheduler import _recompute_signals_for

async def refresh_all_signals():
    logging.basicConfig(level=logging.INFO)
    print("Fetching active symbols...")
    
    async with SessionLocal() as session:
        result = await session.execute(
            select(StockMaster.symbol).where(StockMaster.is_active == True)
        )
        symbols = [row[0] for row in result.all()]

    print(f"Starting signal recomputation for {len(symbols)} symbols...")
    
    # Process in chunks of 5 to avoid overwhelming the database/CPU
    chunk_size = 5
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        print(f"Processing chunk {i//chunk_size + 1}: {chunk}")
        tasks = [_recompute_signals_for(sym) for sym in chunk]
        await asyncio.gather(*tasks, return_exceptions=True)

    print("Signal refresh complete!")

if __name__ == "__main__":
    asyncio.run(refresh_all_signals())
