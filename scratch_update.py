import re
import sys

sys.path.append('/home/nicky/work/stockmarket/marketwatch/marketmind')
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

filepath = "/home/nicky/work/stockmarket/marketwatch/marketmind/backend/portfolio/uptock.txt"
with open(filepath, "r") as f:
    text = f.read()

new_stocks = {}

# Use regex to extract details
matches = re.finditer(r'"scpName"\s*:\s*"([^"]+)",\s*"isin"\s*:\s*"([^"]+)",(?:.*?)"symbol"\s*:\s*"([^"]+)"', text, re.DOTALL)
for match in matches:
    name = match.group(1).title().replace(" Ltd.", "").replace(" Ltd", "").replace(" Limited", "").strip()
    isin = match.group(2)
    sym = match.group(3)
    
    if sym not in new_stocks:
        existing = PORTFOLIO_STOCKS.get(sym, {})
        new_stocks[sym] = {
            "yf": existing.get("yf", f"{sym}.NS"),
            "isin": isin,
            "name": name,
            "sector": existing.get("sector", "Unknown"),
            "mcap": existing.get("mcap", "UNKNOWN")
        }

output = "PORTFOLIO_STOCKS = {\n"
for sym, details in sorted(new_stocks.items()):
    output += f'  "{sym}": {{"yf": "{details["yf"]}", "isin": "{details["isin"]}", "name": "{details["name"]}", "sector": "{details["sector"]}", "mcap": "{details["mcap"]}"}},\n'
output += "}\n"

with open("/home/nicky/work/stockmarket/marketwatch/marketmind/backend/utils/symbol_mapper.py", "w") as f:
    f.write(output)

print(f"Mapped {len(new_stocks)} unique stocks!")
