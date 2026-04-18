import io
import zipfile
import httpx
import pandas as pd
import asyncio
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

async def download_bse_samco(target_date: datetime) -> pd.DataFrame:
    """Download from Samco API (Mirror)."""
    url = "https://www.samco.in/bse_nse_mcx/getBhavcopy"
    date_str = target_date.strftime("%Y-%m-%d")
    
    payload = {
        "start_date": date_str,
        "end_date": date_str,
        "bhavcopy_data[]": ["BSE"]
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.samco.in/"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, data=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Samco BSE download failed: {response.status_code}")
            
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # File name pattern: 20260417_BSE.csv
            filename = f"{target_date.strftime('%Y%m%d')}_BSE.csv"
            if filename not in z.namelist():
                filename = z.namelist()[0]
            
            with z.open(filename) as f:
                df = pd.read_csv(f)
                return df

async def download_bse_official(target_date: datetime) -> pd.DataFrame:
    """Fallback to official (might still be blocked, but structure is here)."""
    # ... placeholder or actual logic if needed ...
    raise Exception("BSE Official source is currently restricted. Please use SAMCO.")

def parse_bse(df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
    """Parse BSE data from Samco format."""
    # Samco headers: SC_CODE,SC_NAME,SC_GROUP,SC_TYPE,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,NO_TRADES,NO_OF_SHRS,NET_TURNOV,TDCLOINDI
    df = df.rename(columns={
        'SC_CODE': 'symbol', # Numeric ID
        'OPEN': 'open',
        'HIGH': 'high',
        'LOW': 'low',
        'CLOSE': 'close',
        'NO_OF_SHRS': 'volume',
        'SC_TYPE': 'series'
    })
    
    df['date'] = target_date.date()
    df['symbol'] = df['symbol'].astype(str).str.strip()
    
    # Filter for standard STK/Equities if applicable, or just keep all from Equity file
    if 'series' in df.columns:
        df = df[df['series'].astype(str).str.strip() == 'STK'].copy()
        
    df['exchange'] = 'BSE'
    return df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'exchange']]
