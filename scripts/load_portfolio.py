import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

def load_portfolio(filepath):
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        return
        
    print(f"Parsing portfolio holdings from {filepath}...")
    try:
        df = pd.read_excel(filepath)
        print("Data loaded successfully. Simulating ISIN mapping...")
        matched = 0
        unmatched = 0
        
        # Simplified scanning
        for _, row in df.iterrows():
            # Assume row contains an ISIN column or we scan all for "INE"
            row_str = " ".join([str(v) for v in row.values])
            if "INE" in row_str:
                # Mock match finding
                matched += 1
                
        print(f"Matched {matched} records against stocks_master (simulated).")
    except Exception as e:
        print(f"Failed to process portfolio file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_portfolio.py <path_to_holdings.xlsx>")
    else:
        load_portfolio(sys.argv[1])
