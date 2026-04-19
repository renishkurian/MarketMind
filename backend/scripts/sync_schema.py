import asyncio
import logging
from sqlalchemy import text
from backend.data.db import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync_schema")

async def sync_enums():
    async with engine.connect() as conn:
        logger.info("Syncing PriceHistory.source Enum...")
        # Add 'historical_import' to price_history
        await conn.execute(text("""
            ALTER TABLE price_history 
            MODIFY COLUMN source ENUM('bhavcopy', 'yfinance_fallback', 'eod_computed', 'historical_import')
        """))
        
        logger.info("Syncing FundamentalsCache.data_quality Enum...")
        # Keep FULL, PARTIAL, MISSING, AI_RESEARCHED and add VERIFIED
        await conn.execute(text("""
            ALTER TABLE fundamentals_cache 
            MODIFY COLUMN data_quality ENUM('FULL', 'PARTIAL', 'MISSING', 'AI_RESEARCHED', 'VERIFIED') DEFAULT 'FULL'
        """))
        
        await conn.commit()
        logger.info("Database schema sync complete.")

if __name__ == "__main__":
    asyncio.run(sync_enums())
