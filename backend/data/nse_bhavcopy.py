import io
import zipfile
import httpx
import pandas as pd
import asyncio
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

async def download_nse_official(target_date: datetime) -> pd.DataFrame:
    """Download from official NSE archives (UDiFF/Legacy)."""
    date_str = target_date.strftime('%Y%m%d')
    year = target_date.strftime('%Y')
    mon = target_date.strftime('%b').upper()
    dd_mon_yyyy = target_date.strftime('%d%b%Y').upper()
    
    udiff_url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    legacy_url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{mon}/cm{dd_mon_yyyy}bhav.csv.zip"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.nseindia.com/'
    }
    
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        await client.get('https://www.nseindia.com', headers=headers)
        
        primary_url = udiff_url if int(year) >= 2026 else legacy_url
        secondary_url = legacy_url if int(year) >= 2026 else udiff_url
        
        response = await client.get(primary_url, headers=headers)
        if response.status_code != 200:
            logger.info(f"Primary NSE URL failed ({response.status_code}), trying secondary...")
            response = await client.get(secondary_url, headers=headers)
            
        if response.status_code != 200:
            raise Exception(f"Failed to download NSE Bhavcopy from both sources.")
            
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            filename = z.namelist()[0]
            with z.open(filename) as f:
                use_header = None if "BhavCopy_NSE_CM" in response.url.path else 'infer'
                df = pd.read_csv(f, header=use_header)
                return df

def parse_nse(df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
    """Parse NSE data from Official or Samco formats."""
    # Official Source Detection
    if 'SYMBOL' in df.columns and 'SERIES' in df.columns:
        # Legacy Format
        df = df.rename(columns={
            'SYMBOL': 'symbol', 'SERIES': 'series', 'OPEN': 'open', 'HIGH': 'high',
            'LOW': 'low', 'CLOSE': 'close', 'TOTTRDQTY': 'volume', 'TIMESTAMP': 'date',
            'ISIN': 'isin', 'TOTALTRADES': 'no_of_trades'
        })
        if isinstance(df['date'].iloc[0], str):
            df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y').dt.date
    elif df.columns.dtype == 'int64' or list(df.columns) == list(range(len(df.columns))):
        # 6:ISIN, 7:Symbol, 8:Series, 14:Open, 15:High, 16:Low, 17:Close, 24:Volume, 26:TotalTrades
        df = df[[6, 7, 8, 14, 15, 16, 17, 24, 26]].copy()
        df.columns = ['isin', 'symbol', 'series', 'open', 'high', 'low', 'close', 'volume', 'no_of_trades']
        df['date'] = target_date.date()
    else:
        # Samco/Fallback Format
        df = df.rename(columns={
            'SYMBOL': 'symbol', 'SERIES': 'series', 'OPEN': 'open', 'HIGH': 'high',
            'LOW': 'low', 'CLOSE': 'close', 'TOTTRDQTY': 'volume', 'ISIN': 'isin'
        })
        df['date'] = target_date.date()

    df['symbol'] = df['symbol'].astype(str).str.strip()
    df['isin'] = df['isin'].astype(str).str.strip()
    df = df[df['series'].astype(str).str.strip() == 'EQ'].copy()
    df['exchange'] = 'NSE'
    if 'no_of_trades' not in df.columns:
        df['no_of_trades'] = None
    return df[['symbol', 'isin', 'date', 'open', 'high', 'low', 'close', 'volume', 'no_of_trades', 'exchange']]
