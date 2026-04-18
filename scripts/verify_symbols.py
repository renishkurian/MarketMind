import yfinance as yf
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

def verify():
    print(f"Verifying {len(PORTFOLIO_STOCKS)} portfolio symbols via Yahoo Finance...")
    successful = []
    failed = []
    
    for sym, data in PORTFOLIO_STOCKS.items():
        yf_sym = data['yf']
        try:
            df = yf.download(yf_sym, period="1d", progress=False)
            if not df.empty:
                successful.append(sym)
                print(f"SUCCESS: {sym} -> {yf_sym}")
            else:
                failed.append(sym)
                print(f"FAILED (No Data): {sym} -> {yf_sym}")
        except Exception as e:
            failed.append(sym)
            print(f"FAILED (Error): {sym} -> {yf_sym} ({e})")
            
    print("\n--- Summary ---")
    print(f"Total Successful: {len(successful)}")
    print(f"Total Failed: {len(failed)}")
    
    if failed:
        print("\nSuggestions for failed symbols: verify the symbol suffix (.NS or .BO) on Yahoo Finance.")

if __name__ == "__main__":
    verify()
