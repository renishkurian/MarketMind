import asyncio
import aiomysql
import os
import sys
import re
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
        sym = m.group(5)
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
                INSERT INTO stocks_master (symbol, company_name, isin, sector, market_cap_cat, quantity, avg_buy_price, buy_date, type, added_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PORTFOLIO', CURDATE())
                ON DUPLICATE KEY UPDATE 
                    sector=VALUES(sector), 
                    market_cap_cat=VALUES(market_cap_cat), 
                    quantity=VALUES(quantity), 
                    avg_buy_price=VALUES(avg_buy_price), 
                    buy_date=VALUES(buy_date)
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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_portfolio.py <path_to_uptock.txt>")
    else:
        asyncio.run(extract_and_load(sys.argv[1]))
