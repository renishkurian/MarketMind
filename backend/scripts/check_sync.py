import asyncio
from backend.data.db import SessionLocal, SyncLog
from sqlalchemy import select

async def check():
    async with SessionLocal() as s:
        res = await s.execute(select(SyncLog).order_by(SyncLog.completed_at.desc()).limit(5))
        logs = res.scalars().all()
        if not logs:
            print("No sync logs found.")
            return
        for l in logs:
            print(f"{l.exchange} | {l.target_date} | {l.status} | {l.completed_at}")

if __name__ == "__main__":
    asyncio.run(check())
