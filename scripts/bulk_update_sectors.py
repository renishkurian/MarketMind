import asyncio
import os
import sys
from sqlalchemy import update

# Add parent dir to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import SessionLocal, StockMaster

# researched mapping
SECTOR_MAP = {
    # Financial Services
    'CDSL': 'Financial Services',
    'IFCI': 'Financial Services',
    'IRFC': 'Financial Services',
    'IREDA': 'Financial Services',
    'PAYTM': 'Financial Services',
    'POLICYBZR': 'Financial Services',
    'SHRIRAMFIN': 'Financial Services',
    'UJJIVANSFB': 'Financial Services',
    'BAJAJHFL': 'Financial Services',
    'CARERATING': 'Financial Services',
    
    # IT / Technology
    'COFORGE': 'Information Technology',
    'DATAMATICS': 'Information Technology',
    'FSL': 'Information Technology',
    'LATENTVIEW': 'Information Technology',
    'NEWGEN': 'Information Technology',
    'FIRSTCRY': 'Retail',
    'ETERNAL': 'Services', # Zomato
    
    # Metals & Mining
    'COALINDIA': 'Metals & Mining',
    'JSL': 'Metals & Mining',
    'SHYAMMETL': 'Metals & Mining',
    'SAIL': 'Metals & Mining',
    'TATASTEEL': 'Metals & Mining',
    'VEDL': 'Metals & Mining',
    
    # Energy & Utilities
    'GAIL': 'Energy',
    'HINDOILEXP': 'Energy',
    'HINDPETRO': 'Energy',
    'BPCL': 'Energy',
    'NTPC': 'Utilities',
    'NTPCGREEN': 'Utilities',
    'ONGC': 'Energy',
    'IEX': 'Energy',
    
    # Automobile
    'OLAELEC': 'Automobile',
    'OLECTRA': 'Automobile',
    'TMPV': 'Automobile',
    'TMCV': 'Automobile',
    'GREAVESCOT': 'Automobile',
    'ASHOKLEY': 'Automobile',
    
    # Chemicals
    'AETHER': 'Chemicals',
    'PCBL': 'Chemicals',
    'TATVA': 'Chemicals',
    'BCLIND': 'Chemicals',
    
    # Pharma
    'DRREDDY': 'Pharmaceuticals',
    'GLAND': 'Pharmaceuticals',
    
    # Defense/Industrial
    'APOLLO': 'Defense & Aerospace',
    'COCHINSHIP': 'Defense & Aerospace',
    'SALASAR': 'Capital Goods',
    'GENSOL': 'Renewable Energy',
    'BHEL': 'Capital Goods',
    
    # Consumer / Retail / Real Estate
    'CCL': 'Consumer Goods',
    'TI': 'Consumer Staples',
    'NYKAA': 'Retail',
    'ARKADE': 'Real Estate',
    'SOBHA': 'Real Estate',
    'SCI': 'Shipping',
    'NITINSPIN': 'Textiles',
    'WELSPUNLIV': 'Textiles',
    
    # ETFs
    'GOLDBEES': 'ETF & Mutual Funds',
    'HDFCGOLD': 'ETF & Mutual Funds',
    'NIFTYBEES': 'ETF & Mutual Funds',
    'SILVERBEES': 'ETF & Mutual Funds',
    'TATSILV': 'ETF & Mutual Funds'
}

async def bulk_update_sectors():
    print(f"Starting bulk sector update for {len(SECTOR_MAP)} symbols...")
    curr = 0
    total = len(SECTOR_MAP)
    
    async with SessionLocal() as db:
        for symbol, sector in SECTOR_MAP.items():
            res = await db.execute(
                update(StockMaster)
                .where(StockMaster.symbol == symbol)
                .values(sector=sector)
            )
            curr += 1
            if curr % 10 == 0:
                print(f"  Processed {curr}/{total}")
        
        await db.commit()
    
    print("Bulk update complete!")

if __name__ == "__main__":
    asyncio.run(bulk_update_sectors())
