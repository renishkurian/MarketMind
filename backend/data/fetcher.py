import yfinance as yf
import pandas as pd
import asyncio
import logging
from typing import List, Dict, Any

from backend.utils.symbol_mapper import PORTFOLIO_STOCKS

logger = logging.getLogger(__name__)

def _get_yf_symbol(symbol: str) -> str:
    # Attempt to use symbol mapper if available, else append .NS
    if symbol in PORTFOLIO_STOCKS:
        return PORTFOLIO_STOCKS[symbol]['yf']
    return f"{symbol}.NS"

async def fetch_live_prices(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch live ticker data in batches of 10."""
    results = {}
    batch_size = 10
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        yf_symbols = [_get_yf_symbol(s) for s in batch]
        try:
            # yfinance doesn't easily support async, so we wrap it or just use it sync
            # download period=1d will fetch the latest daily candle, showing current intraday price
            df = yf.download(yf_symbols, period='1d', interval='1d', progress=False, auto_adjust=False)
            
            if not df.empty:
                for idx, yf_sym in enumerate(yf_symbols):
                    sym = batch[idx]
                    # Handle single stock vs multi stock return
                    if len(yf_symbols) == 1:
                        stock_data = df.iloc[-1]
                    else:
                        try:
                            # Handle multi-stock download
                            stock_data = df.xs(yf_sym, level=1, axis=1).iloc[-1]
                        except (KeyError, ValueError):
                            continue

                    close_val = stock_data.get('Close')
                    # If it's still a series (shouldn't be after .iloc[-1] but let's be safe)
                    if isinstance(close_val, pd.Series):
                        close_val = close_val.iloc[0]

                    if pd.notna(close_val):
                        results[sym] = {
                            "open": float(stock_data.get('Open', 0).iloc[0] if isinstance(stock_data.get('Open'), pd.Series) else stock_data.get('Open', 0)),
                            "high": float(stock_data.get('High', 0).iloc[0] if isinstance(stock_data.get('High'), pd.Series) else stock_data.get('High', 0)),
                            "low": float(stock_data.get('Low', 0).iloc[0] if isinstance(stock_data.get('Low'), pd.Series) else stock_data.get('Low', 0)),
                            "close": float(close_val),
                            "volume": int(stock_data.get('Volume', 0).iloc[0] if isinstance(stock_data.get('Volume'), pd.Series) else stock_data.get('Volume', 0)),
                            "timestamp": df.index[-1].to_pydatetime()
                        }
        except Exception as e:
            logger.error(f"Error fetching live prices for batch {batch}: {e}")
            
        await asyncio.sleep(0.5)
        
    return results

async def fetch_5min_candles(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch the latest 5-min candle for a list of symbols."""
    results = {}
    batch_size = 10
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        yf_symbols = [_get_yf_symbol(s) for s in batch]
        try:
            df = yf.download(yf_symbols, period='1d', interval='5m', progress=False, auto_adjust=False)
            
            if not df.empty:
                for idx, yf_sym in enumerate(yf_symbols):
                    sym = batch[idx]
                    if len(yf_symbols) == 1:
                        stock_data = df.iloc[-1]
                    else:
                        try:
                            # Handle multi-stock download
                            stock_data = df.xs(yf_sym, level=1, axis=1).iloc[-1]
                        except (KeyError, ValueError):
                            continue

                    close_val = stock_data.get('Close')
                    # If it's still a series
                    if isinstance(close_val, pd.Series):
                        close_val = close_val.iloc[0]

                    if pd.notna(close_val):
                        results[sym] = {
                            "open": float(stock_data.get('Open', 0).iloc[0] if isinstance(stock_data.get('Open'), pd.Series) else stock_data.get('Open', 0)),
                            "high": float(stock_data.get('High', 0).iloc[0] if isinstance(stock_data.get('High'), pd.Series) else stock_data.get('High', 0)),
                            "low": float(stock_data.get('Low', 0).iloc[0] if isinstance(stock_data.get('Low'), pd.Series) else stock_data.get('Low', 0)),
                            "close": float(close_val),
                            "volume": int(stock_data.get('Volume', 0).iloc[0] if isinstance(stock_data.get('Volume'), pd.Series) else stock_data.get('Volume', 0)),
                            "timestamp": df.index[-1].to_pydatetime()
                        }
        except Exception as e:
            logger.error(f"Error fetching 5-min candles for batch {batch}: {e}")
            
        await asyncio.sleep(0.5)
        
    return results

async def fetch_fundamentals(symbol: str) -> Dict[str, Any]:
    """Fetch fundamentals for a single symbol using yfinance info."""
    yf_sym = _get_yf_symbol(symbol)
    try:
        # Run in executor since yf.Ticker() does blocking HTTP calls
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        
        result = {
            "pe_ratio": info.get('trailingPE'),
            "eps": info.get('trailingEps'),
            "roe": info.get('returnOnEquity'),
            "debt_equity": info.get('debtToEquity'),
            "revenue_growth": info.get('revenueGrowth'),
            "market_cap": info.get('marketCap'),
            "sector": info.get('sector'),
            "sector_pe": None, # yfinance info doesn't typically provide sector PE directly
            "promoter_holding": (info.get('heldPercentInsiders', 0) * 100) if info.get('heldPercentInsiders') else None,
            "promoter_pledge_pct": (info.get('pledgedPercent', 0) * 100) if info.get('pledgedPercent') else None
        }
        
        # Determine data quality
        # if all non-sector values are None, MISSING
        required_keys = ["pe_ratio", "eps", "roe", "debt_equity", "revenue_growth"]
        missing_count = sum(1 for k in required_keys if result[k] is None)
        
        if missing_count == len(required_keys):
            result["data_quality"] = "MISSING"
        elif missing_count > 0:
            result["data_quality"] = "PARTIAL"
        else:
            result["data_quality"] = "FULL"
            
        return result
        
    except Exception as e:
        logger.error(f"Error fetching fundamentals for {symbol}: {e}")
        return {
            "pe_ratio": None, "eps": None, "roe": None,
            "debt_equity": None, "revenue_growth": None,
            "market_cap": None, "sector": None, "sector_pe": None,
            "data_quality": "MISSING"
        }

async def fetch_max_history(symbol: str) -> pd.DataFrame:
    """Fetch maximum available daily history for a symbol via yfinance."""
    yf_sym = _get_yf_symbol(symbol)
    try:
        df = yf.download(yf_sym, period='max', interval='1d', progress=False, auto_adjust=False)
        return df
    except Exception as e:
        logger.error(f"Error fetching max history for {symbol}: {e}")
        return pd.DataFrame()
