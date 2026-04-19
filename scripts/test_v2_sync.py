import asyncio
import logging
from backend.scheduler import _recompute_signals_for, _fetch_all_sector_data

async def test_v2_sync(symbol: str):
    logging.basicConfig(level=logging.INFO)
    print(f"Testing V2 Sync for {symbol}...")
    
    # 1. Fetch sector vault
    vault = await _fetch_all_sector_data()
    
    # 2. Run recompute
    await _recompute_signals_for(symbol, vault)
    print(f"V2 Sync test complete for {symbol}. Check the database.")

if __name__ == "__main__":
    # Test with a symbol that has data (e.g. RELIANCE)
    asyncio.run(test_v2_sync("RELIANCE"))
