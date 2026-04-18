import asyncio
import aiomysql
import os
import sys
import re
import pandas as pd
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'backend', '.env'))

DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DB", "marketmind_db")

def categorize_mcap(mcap_inr):
    if not mcap_inr: return "UNKNOWN"
    cr = mcap_inr / 10**7
    if cr >= 20000: return "LARGE"
    if cr >= 5000: return "MID"
    return "SMALL"

async def extract_and_load(filepath):
    if not os.path.exists(filepath):
        print(f"File {filepath} not found!")
        return

    print("Fetching global ISIN to NSE Symbol mapping from legacy Bhavcopy...")
    from backend.data.nse_bhavcopy import download_nse_official, parse_nse
    
    # Use the target date we know works to get ISINs
    try:
        df = await download_nse_official(datetime(2026, 4, 17))
        df_parsed = parse_nse(df, datetime(2026, 4, 17))
        isin_to_nse = pd.Series(df_parsed['symbol'].values, index=df_parsed['isin']).to_dict()
    except Exception as e:
        print(f"Warning: Failed to fetch NSE ISIN map: {e}")
        isin_to_nse = {}

    with open(filepath, "r") as f:
        text = f.read()

    pattern = r'"scpName"\s*:\s*"([^"]+)".*?"isin"\s*:\s*"([^"]+)".*?"openQty"\s*:\s*([\d\.]+).*?"avgRate"\s*:\s*([\d\.]+).*?"symbol"\s*:\s*"([^"]+)".*?"buyDate"\s*:\s*"([^"]+)"'
    matches = re.finditer(pattern, text, re.DOTALL)
    
    portfolio = {}
    unique_txns = []
    seen = set()
    for m in matches:
        name = m.group(1).title().replace(" Ltd.", "").replace(" Ltd", "").replace(" Limited", "").strip()
        isin = m.group(2)
        qty = float(m.group(3))
        avg_rate = float(m.group(4))
        upstox_sym = m.group(5)
        # Use true NSE symbol via ISIN mapping!
        sym = isin_to_nse.get(isin, upstox_sym)
        buy_date_str = m.group(6) # DD-MM-YYYY
        
        sig = (sym, qty, avg_rate, buy_date_str)
        if sig in seen:
            continue
        seen.add(sig)
            
        dt = datetime.strptime(buy_date_str, "%d-%m-%Y").date()
        
        unique_txns.append({
            "sym": sym,
            "isin": isin,
            "qty": qty,
            "rate": avg_rate,
            "dt": dt
        })
        
        if sym not in portfolio:
            portfolio[sym] = {
                "name": name,
                "isin": isin,
                "total_qty": 0.0,
                "total_cost": 0.0,
                "earliest_date": dt
            }
            
        portfolio[sym]["total_qty"] += qty
        portfolio[sym]["total_cost"] += (qty * avg_rate)
        if dt < portfolio[sym]["earliest_date"]:
            portfolio[sym]["earliest_date"] = dt

    print(f"Extracted {len(portfolio)} unique holdings. Fetching latest categories from Yahoo Finance...")

    conn = await aiomysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, db=DB_NAME)
    async with conn.cursor() as cur:
        await cur.execute("UPDATE stocks_master SET is_active = 0 WHERE type = 'PORTFOLIO'")
        for sym, d in portfolio.items():
            avg_price = d["total_cost"] / d["total_qty"] if d["total_qty"] > 0 else 0
            
            ticker = yf.Ticker(f"{sym}.NS")
            try:
                info = ticker.info
                sector = info.get("sector", "Unknown") if info.get("sector") else "Unknown"
                mcap = info.get("marketCap", 0)
                mcap_cat = categorize_mcap(mcap)
            except Exception:
                sector = "Unknown"
                mcap_cat = "UNKNOWN"
                
            print(f"[{sym}] qty: {d['total_qty']}, avg: {avg_price:.2f}, sector: {sector}, mcap: {mcap_cat}")
            
            query = """
                INSERT INTO stocks_master (symbol, company_name, isin, sector, market_cap_cat, quantity, avg_buy_price, buy_date, type, added_date, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PORTFOLIO', CURDATE(), 1)
                ON DUPLICATE KEY UPDATE 
                    sector=VALUES(sector), 
                    market_cap_cat=VALUES(market_cap_cat), 
                    quantity=VALUES(quantity), 
                    avg_buy_price=VALUES(avg_buy_price), 
                    buy_date=VALUES(buy_date),
                    is_active=1
            """
            await cur.execute(query, (
                sym, d["name"], d["isin"], sector, mcap_cat, 
                d["total_qty"], avg_price, d["earliest_date"]
            ))
            
        print(f"Inserting {len(unique_txns)} individual transaction breakdowns into portfolio_transactions...")
        await cur.execute("TRUNCATE TABLE portfolio_transactions;")
        txn_query = """
            INSERT INTO portfolio_transactions (symbol, isin, quantity, buy_price, buy_date, status)
            VALUES (%s, %s, %s, %s, %s, 'OPEN')
        """
        for t in unique_txns:
            await cur.execute(txn_query, (t['sym'], t['isin'], t['qty'], t['rate'], t['dt']))
            
        await conn.commit()
    conn.close()
    print("Portfolio completely synced to Database!")

    print("Running historical price alignment...")
    try:
        from scripts.fix_historical_prices import fix_prices
        # Use existing event loop for the async call
        await fix_prices(lookback_days=5)
        print("P&L baseline alignment complete!")
    except Exception as e:
        print(f"Warning: Price history alignment failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_portfolio.py <path_to_uptock.txt>")
    else:
        asyncio.run(extract_and_load(sys.argv[1]))
