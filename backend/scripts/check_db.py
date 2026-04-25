import asyncio
import os
import sys

# Add the project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.data.db import engine, Base, PerformanceCache
from sqlalchemy import text

async def check_table():
    async with engine.begin() as conn:
        # This will create missing tables including PerformanceCache
        await conn.run_sync(Base.metadata.create_all)
        
        print("Ensuring PerformanceCache table exists...")
        
    async with engine.connect() as conn:
        result = await conn.execute(text("SHOW TABLES LIKE 'performance_cache'"))
        table_exists = result.scalar()
        if table_exists:
            print("SUCCESS: 'performance_cache' table is present in the database.")
            
            # Check columns to ensure user_id is there
            result = await conn.execute(text("DESCRIBE performance_cache"))
            columns = [row[0] for row in result.all()]
            print(f"Columns found: {columns}")
            if 'user_id' in columns:
                 print("VERIFIED: 'user_id' column exists for multi-user isolation.")
        else:
            print("FAILURE: Table could not be found or created.")

if __name__ == "__main__":
    asyncio.run(check_table())
