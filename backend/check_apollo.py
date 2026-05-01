import asyncio
from backend.data.db import SessionLocal, PriceHistory
from sqlalchemy import select

async def main():
    db = SessionLocal()
    print("Fetching Apollo symbols...")
    stmt = select(PriceHistory.symbol, PriceHistory.date, PriceHistory.close).where(
        PriceHistory.symbol.like('APOLLO%')
    ).order_by(PriceHistory.date.desc()).limit(10)
    res = await db.execute(stmt)
    for r in res.all():
        print(r)
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())
