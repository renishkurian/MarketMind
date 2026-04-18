import asyncio
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.bhavcopy import load_historical_bhavcopy

async def main():
    print("Loading historical Bhavcopy data...")
    # Get last 3 months ~ 90 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    print(f"Fetching from {start_date.date()} to {end_date.date()}")
    await load_historical_bhavcopy(start_date, end_date)
    print("Finished loading historical Bhavcopy data.")
    
    # Optional fallback to yfinance max history could be triggered here if Bhavcopy fails completely

if __name__ == "__main__":
    asyncio.run(main())
