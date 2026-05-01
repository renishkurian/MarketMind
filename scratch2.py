import asyncio
import logging
from backend.data.fetcher import fetch_live_prices
logging.basicConfig(level=logging.ERROR)
async def main():
    prices = await fetch_live_prices({"RELIANCE": "RELIANCE.NS"})
    print("Prices:", prices)
asyncio.run(main())
