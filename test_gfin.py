import asyncio
import httpx
import re

async def fetch_gfin(symbol):
    url = f"https://www.google.com/finance/quote/{symbol}:NSE"
    async with httpx.AsyncClient(headers={'User-Agent': 'Mozilla/5.0'}) as client:
        resp = await client.get(url, timeout=5)
        match = re.search(r'data-last-price="([^"]+)"', resp.text)
        if match:
            return symbol, float(match.group(1))
        return symbol, None

async def main():
    symbols = ['APOLLO', 'AETHER', 'ARKADE']
    tasks = [fetch_gfin(s) for s in symbols]
    res = await asyncio.gather(*tasks)
    print(res)

asyncio.run(main())
