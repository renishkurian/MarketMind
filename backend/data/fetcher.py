import yfinance as yf
import pandas as pd
import asyncio
import logging
from typing import List, Dict, Any, Union

logger = logging.getLogger(__name__)

def _get_yf_symbol(symbol: str) -> str:
    """Legacy helper. Callers should transition to using DB-defined yahoo_symbol."""
    return f"{symbol}.NS"

async def fetch_live_prices(symbols: Union[List[str], Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch live ticker data in batches of 10.
    Accepts:
      - List[str]: Legacy behavior (appends .NS)
      - Dict[str, str]: Map of {internal_symbol: yahoo_symbol}
    """
    results = {}
    batch_size = 10
    
    # Normalize to dict mapping
    if isinstance(symbols, list):
        symbols_map = {s: _get_yf_symbol(s) for s in symbols}
    else:
        symbols_map = symbols

    internal_symbols = list(symbols_map.keys())
    
    for i in range(0, len(internal_symbols), batch_size):
        batch_internal = internal_symbols[i : i + batch_size]
        batch_yf = [symbols_map[s] for s in batch_internal]
        
        try:
            df = yf.download(batch_yf, period='1d', interval='1d', progress=False, auto_adjust=False)
            
            if not df.empty:
                for sym_int in batch_internal:
                    yf_sym = symbols_map[sym_int]
                    
                    try:
                        if len(batch_yf) == 1:
                            stock_data = df.iloc[-1]
                        else:
                            stock_data = df.xs(yf_sym, level=1, axis=1).iloc[-1]
                    except (KeyError, ValueError):
                        continue

                    close_val = stock_data.get('Close')
                    if isinstance(close_val, pd.Series):
                        close_val = close_val.iloc[0]

                    if pd.notna(close_val):
                        results[sym_int] = {
                            "open": float(stock_data.get('Open', 0).iloc[0] if isinstance(stock_data.get('Open'), pd.Series) else stock_data.get('Open', 0)),
                            "high": float(stock_data.get('High', 0).iloc[0] if isinstance(stock_data.get('High'), pd.Series) else stock_data.get('High', 0)),
                            "low": float(stock_data.get('Low', 0).iloc[0] if isinstance(stock_data.get('Low'), pd.Series) else stock_data.get('Low', 0)),
                            "close": float(close_val),
                            "volume": int(stock_data.get('Volume', 0).iloc[0] if isinstance(stock_data.get('Volume'), pd.Series) else stock_data.get('Volume', 0)),
                            "timestamp": df.index[-1].to_pydatetime()
                        }
        except Exception as e:
            logger.error(f"Error fetching live prices for batch {batch_internal}: {e}")
            
        await asyncio.sleep(0.5)
        
    return results

async def fetch_5min_candles(symbols: Union[List[str], Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    """Fetch the latest 5-min candle. Accepts List[str] or Dict[internal:yf]."""
    results = {}
    batch_size = 10
    
    if isinstance(symbols, list):
        symbols_map = {s: _get_yf_symbol(s) for s in symbols}
    else:
        symbols_map = symbols

    internal_symbols = list(symbols_map.keys())
    
    for i in range(0, len(internal_symbols), batch_size):
        batch_internal = internal_symbols[i : i + batch_size]
        batch_yf = [symbols_map[s] for s in batch_internal]
        
        try:
            df = yf.download(batch_yf, period='1d', interval='5m', progress=False, auto_adjust=False)
            
            if not df.empty:
                for sym_int in batch_internal:
                    yf_sym = symbols_map[sym_int]
                    try:
                        if len(batch_yf) == 1:
                            stock_data = df.iloc[-1]
                        else:
                            stock_data = df.xs(yf_sym, level=1, axis=1).iloc[-1]
                    except (KeyError, ValueError):
                        continue

                    close_val = stock_data.get('Close')
                    if isinstance(close_val, pd.Series):
                        close_val = close_val.iloc[0]

                    if pd.notna(close_val):
                        results[sym_int] = {
                            "open": float(stock_data.get('Open', 0).iloc[0] if isinstance(stock_data.get('Open'), pd.Series) else stock_data.get('Open', 0)),
                            "high": float(stock_data.get('High', 0).iloc[0] if isinstance(stock_data.get('High'), pd.Series) else stock_data.get('High', 0)),
                            "low": float(stock_data.get('Low', 0).iloc[0] if isinstance(stock_data.get('Low'), pd.Series) else stock_data.get('Low', 0)),
                            "close": float(close_val),
                            "volume": int(stock_data.get('Volume', 0).iloc[0] if isinstance(stock_data.get('Volume'), pd.Series) else stock_data.get('Volume', 0)),
                            "timestamp": df.index[-1].to_pydatetime()
                        }
        except Exception as e:
            logger.error(f"Error fetching 5-min candles for batch {batch_internal}: {e}")
            
        await asyncio.sleep(0.5)
        
    return results

async def fetch_fundamentals(symbol: str, yahoo_symbol: str = None) -> Dict[str, Any]:
    """Fetch fundamentals using yfinance info. yahoo_symbol is preferred."""
    yf_sym = yahoo_symbol or _get_yf_symbol(symbol)
    try:
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
            "sector_pe": None,
            "promoter_holding": (info.get('heldPercentInsiders', 0) * 100) if info.get('heldPercentInsiders') else None,
            "promoter_pledge_pct": (info.get('pledgedPercent', 0) * 100) if info.get('pledgedPercent') else None
        }
        
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
        logger.error(f"Error fetching fundamentals for {symbol} (ticker: {yf_sym}): {e}")
        return {
            "pe_ratio": None, "eps": None, "roe": None,
            "debt_equity": None, "revenue_growth": None,
            "market_cap": None, "sector": None, "sector_pe": None,
            "data_quality": "MISSING"
        }

async def fetch_max_history(symbol: str, yahoo_symbol: str = None) -> pd.DataFrame:
    """Fetch maximum history. yahoo_symbol is preferred."""
    yf_sym = yahoo_symbol or _get_yf_symbol(symbol)
    try:
        df = yf.download(yf_sym, period='max', interval='1d', progress=False, auto_adjust=False)
        return df
    except Exception as e:
        logger.error(f"Error fetching max history for {symbol} (ticker: {yf_sym}): {e}")
        return pd.DataFrame()
