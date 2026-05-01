import asyncio
import logging
from backend.data.fetcher import _get_live_ltp_google_async
logging.basicConfig(level=logging.ERROR)
async def main():
    price = await _get_live_ltp_google_async("RELIANCE")
    print("Google Price:", price)
asyncio.run(main())
