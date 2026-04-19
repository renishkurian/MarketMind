import os
import sys
import aiomysql
import asyncio
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'backend', '.env'))

DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DB", "marketmind_db")

async def migrate():
    print(f"Starting V2 migration on {DB_NAME}...")
    try:
        conn = await aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, db=DB_NAME)
        async with conn.cursor() as cur:
            # 1. SignalsCache updates
            print("Altering signals_cache...")
            try:
                await cur.execute("""
                ALTER TABLE signals_cache 
                ADD COLUMN composite_score DECIMAL(5,2),
                ADD COLUMN momentum_score DECIMAL(5,2),
                ADD COLUMN sector_rank_score DECIMAL(5,2),
                ADD COLUMN sector_percentile DECIMAL(5,2),
                ADD COLUMN data_confidence DECIMAL(4,3),
                ADD COLUMN promoter_pledge_warning BOOLEAN DEFAULT FALSE,
                ADD COLUMN score_profile VARCHAR(50),
                ADD COLUMN fa_breakdown JSON,
                ADD COLUMN ta_breakdown JSON,
                ADD COLUMN momentum_breakdown JSON
                """)
                print("SignalsCache updated.")
            except Exception as e:
                print(f"Note: signals_cache alter failed (might already have columns): {e}")

            # 2. FundamentalsCache updates
            print("Altering fundamentals_cache...")
            try:
                await cur.execute("""
                ALTER TABLE fundamentals_cache 
                ADD COLUMN promoter_holding DECIMAL(6,2),
                ADD COLUMN promoter_pledge_pct DECIMAL(6,2)
                """)
                print("FundamentalsCache updated.")
            except Exception as e:
                print(f"Note: fundamentals_cache alter failed: {e}")

            # 3. AIInsights updates
            print("Altering ai_insights...")
            try:
                await cur.execute("""
                ALTER TABLE ai_insights 
                ADD COLUMN skill_id VARCHAR(50),
                ADD COLUMN verdict VARCHAR(20)
                """)
                print("AIInsights updated.")
            except Exception as e:
                print(f"Note: ai_insights alter failed: {e}")

            await conn.commit()
            print("Migration V2 complete.")

    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    asyncio.run(migrate())
