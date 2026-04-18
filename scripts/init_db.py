import os
import sys
import aiomysql
import asyncio
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'backend', '.env'))

DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DB", "marketmind_db")

async def init_db():
    print(f"Connecting to MySQL at {DB_HOST}:{DB_PORT} as {DB_USER}...")
    try:
        # Connect to MySQL server without database first to create it
        conn = await aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            print(f"Database '{DB_NAME}' created or exists.")
        conn.close()

        # Connect to the created database
        conn = await aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, db=DB_NAME)
        async with conn.cursor() as cur:
            # TABLE 1: stocks_master
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks_master (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                exchange ENUM('NSE','BSE') NOT NULL DEFAULT 'NSE',
                company_name VARCHAR(100) NOT NULL,
                 sector VARCHAR(50),
                isin VARCHAR(12),
                market_cap_cat ENUM('LARGE','MID','SMALL','UNKNOWN'),
                quantity DECIMAL(14,4),
                avg_buy_price DECIMAL(18,6),
                buy_date DATE,
                type ENUM('PORTFOLIO','WATCHLIST') NOT NULL,
                added_date DATE NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                UNIQUE KEY(symbol, exchange),
                INDEX(symbol),
                INDEX(exchange),
                INDEX(type)
            )
            """)

            # TABLE 2: price_history
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                exchange ENUM('NSE','BSE') NOT NULL DEFAULT 'NSE',
                isin VARCHAR(12),
                date DATE NOT NULL,
                open DECIMAL(10,2),
                high DECIMAL(10,2),
                low DECIMAL(10,2),
                close DECIMAL(10,2) NOT NULL,
                volume BIGINT,
                no_of_trades INT,
                source ENUM('bhavcopy','yfinance_fallback','eod_computed'),
                UNIQUE KEY(symbol, date, exchange),
                INDEX(symbol),
                INDEX(date),
                INDEX(exchange)
            )
            """)

            # TABLE 3: intraday_ticks
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS intraday_ticks (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                exchange ENUM('NSE','BSE') NOT NULL DEFAULT 'NSE',
                timestamp DATETIME NOT NULL,
                open DECIMAL(10,2),
                high DECIMAL(10,2),
                low DECIMAL(10,2),
                close DECIMAL(10,2) NOT NULL,
                volume BIGINT,
                UNIQUE KEY(symbol, timestamp, exchange),
                INDEX(symbol),
                INDEX(timestamp),
                INDEX(exchange)
            )
            """)

            # TABLE 4: signals_cache
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS signals_cache (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                exchange ENUM('NSE','BSE') NOT NULL DEFAULT 'NSE',
                computed_at DATETIME NOT NULL,
                market_session ENUM('LIVE','EOD','CLOSED') NOT NULL,
                current_price DECIMAL(10,2),
                prev_close DECIMAL(10,2),
                change_pct DECIMAL(6,2),
                st_signal ENUM('BUY','HOLD','SELL'),
                st_score DECIMAL(5,2),
                lt_signal ENUM('BUY','HOLD','SELL'),
                lt_score DECIMAL(5,2),
                confidence_pct DECIMAL(5,2),
                data_quality ENUM('FULL','TECHNICALS_ONLY') DEFAULT 'FULL',
                flags JSON,
                indicator_breakdown JSON,
                UNIQUE KEY(symbol, exchange),
                INDEX(symbol),
                INDEX(st_signal),
                INDEX(lt_signal)
            )
            """)

            # TABLE 5: fundamentals_cache
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals_cache (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL UNIQUE,
                fetched_at DATETIME NOT NULL,
                pe_ratio DECIMAL(10,2),
                eps DECIMAL(10,2),
                roe DECIMAL(6,2),
                debt_equity DECIMAL(6,2),
                revenue_growth DECIMAL(6,2),
                sector_pe DECIMAL(10,2),
                market_cap BIGINT,
                data_quality ENUM('FULL','PARTIAL','MISSING') DEFAULT 'FULL',
                INDEX(symbol)
            )
            """)

            # TABLE 6: ai_insights
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS ai_insights (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                generated_at DATETIME NOT NULL,
                trigger_reason ENUM('WEEKLY','PRICE_SPIKE','MANUAL') NOT NULL,
                short_summary TEXT,
                long_summary TEXT,
                key_risks JSON,
                key_opportunities JSON,
                sentiment_score DECIMAL(4,2),
                INDEX(symbol),
                INDEX(generated_at)
            )
            """)
            # TABLE 7: sync_logs
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                target_date DATE NOT NULL,
                exchange ENUM('NSE','BSE') NOT NULL DEFAULT 'NSE',
                sync_type ENUM('MANUAL','SCHEDULED') NOT NULL,
                status ENUM('SUCCESS','FAILED','PARTIAL') NOT NULL,
                records_count INT DEFAULT 0,
                error_message VARCHAR(500),
                completed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX(target_date),
                INDEX(exchange)
            )
            """)

            # TABLE 8: system_config
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS system_config (
                `key` VARCHAR(50) PRIMARY KEY,
                `value` VARCHAR(500) NOT NULL,
                `description` VARCHAR(200),
                `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """)
            print("Created all tables.")

            # Load Portfolio Stocks
            print("Inserting default portfolio stocks...")
            insert_query = """
                INSERT IGNORE INTO stocks_master 
                (symbol, company_name, isin, sector, market_cap_cat, type, added_date)
                VALUES (%s, %s, %s, %s, %s, %s, CURDATE())
            """
            for symbol, details in PORTFOLIO_STOCKS.items():
                await cur.execute(insert_query, (
                    symbol, 
                    details["name"], 
                    details["isin"], 
                    details["sector"], 
                    details["mcap"], 
                    "PORTFOLIO"
                ))
            
            await conn.commit()
            print("Successfully populated stocks_master.")

            # Load Default Configs
            print("Inserting default system configurations...")
            config_query = "INSERT IGNORE INTO system_config (`key`, `value`, `description`) VALUES (%s, %s, %s)"
            configs = [
                ("NSE_SOURCE", "OFFICIAL", "Source for NSE Bhavcopy (OFFICIAL or SAMCO)"),
                ("BSE_SOURCE", "SAMCO", "Source for BSE Bhavcopy (OFFICIAL or SAMCO)"),
                ("SYNC_MODE", "ALL", "Whether to sync all symbols or just portfolio (ALL or PORTFOLIO)")
            ]
            for key, val, desc in configs:
                await cur.execute(config_query, (key, val, desc))

            await conn.commit()
            print("System configuration initialized.")

    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    asyncio.run(init_db())
