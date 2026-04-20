import yfinance as yf
import pandas as pd
import asyncio
import httpx
import logging
import urllib.request
import re
from typing import List, Dict, Any, Union
from datetime import datetime

from backend.utils.market_hours import get_current_ist_time

logger = logging.getLogger(__name__)

def _get_yf_symbol(symbol: str) -> str:
    """Legacy helper. Callers should transition to using DB-defined yahoo_symbol."""
    return f"{symbol}.NS"

def _get_live_ltp_google_sync(symbol: str) -> Union[float, None]:
    """Synchronous web scraper for zero-delay Indian stock prices using Google Finance."""
    try:
        req = urllib.request.Request(
            f'https://www.google.com/finance/quote/{symbol}:NSE',
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            html = resp.read().decode('utf-8')
            match = re.search(r'data-last-price=\"([^\"]+)\"', html)
            if match:
                return float(match.group(1).replace(',', ''))
    except Exception as e:
        logger.debug(f"Google Finance scrape failed for {symbol}: {e}")
    return None

async def _fetch_hybrid_overrides(internal_symbols: List[str]) -> Dict[str, float]:
    """Concurrently scrapes Google Finance for a batch of symbols."""
    overrides = {}
    
    async def fetch_one(sym: str):
        val = await asyncio.to_thread(_get_live_ltp_google_sync, sym)
        if val is not None:
            overrides[sym] = val
            
    await asyncio.gather(*(fetch_one(s) for s in internal_symbols))
    return overrides

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
            df = yf.download(batch_yf, period='1d', interval='1m', progress=False, auto_adjust=False)
            
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
                            "timestamp": get_current_ist_time()
                        }
        except Exception as e:
            logger.error(f"Error fetching live prices for batch {batch_internal}: {e}")
        
        # --- HYBRID OVERRIDE ---
        # Overwrite the delayed Yahoo close with the zero-delay Google LTP
        gfin_overrides = await _fetch_hybrid_overrides(batch_internal)
        for sym_int, true_live_price in gfin_overrides.items():
            if sym_int in results:
                # Set the close to the true live price
                results[sym_int]["close"] = true_live_price
                
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

async def fetch_valuation_timeseries(symbol: str, yahoo_symbol: str) -> Dict[str, Any]:
    """
    Fetch trailing ratios from the direct TimeSeries API.
    Returns: {peg_ratio, ps_ratio, pb_ratio, ev_ebitda}
    """
    url = f"https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{yahoo_symbol}"
    params = {
        "merge": "false",
        "padTimeSeries": "true",
        "period1": int(datetime(2020, 1, 1).timestamp()),
        "period2": int(datetime.now().timestamp()),
        "type": "trailingPegRatio,trailingPsRatio,trailingPbRatio,trailingEnterprisesValueEBITDARatio",
        "lang": "en-US",
        "region": "US"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    defaults = {"peg_ratio": None, "ps_ratio": None, "pb_ratio": None, "ev_ebitda": None}
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code != 200:
                logger.warning(f"Timeseries API error {resp.status_code} for {yahoo_symbol}")
                return defaults
            
            data = resp.json()
            results = data.get("timeseries", {}).get("result", [])
            
            extracted = {}
            for res in results:
                meta_type = res.get("meta", {}).get("type", [None])[0]
                # Look for the latest data point in the array for this type
                # The metric name in the JSON matches the meta_type
                points = res.get(meta_type, [])
                if points:
                    # Get the most recent one (last in list)
                    latest = points[-1].get("reportedValue", {}).get("raw")
                    if meta_type == "trailingPegRatio": extracted["peg_ratio"] = latest
                    elif meta_type == "trailingPsRatio": extracted["ps_ratio"] = latest
                    elif meta_type == "trailingPbRatio": extracted["pb_ratio"] = latest
                    elif meta_type == "trailingEnterprisesValueEBITDARatio": extracted["ev_ebitda"] = latest
            
            return {**defaults, **extracted}

    except Exception as e:
        logger.error(f"Error in fetch_valuation_timeseries for {yahoo_symbol}: {e}")
        return defaults

async def fetch_fundamentals(symbol: str, yahoo_symbol: str = None) -> Dict[str, Any]:
    """Fetch fundamentals using yfinance + direct TimeSeries API."""
    yf_sym = yahoo_symbol or _get_yf_symbol(symbol)
    try:
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        
        # 1. Base data from yfinance
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
            "promoter_pledge_pct": (info.get('pledgedPercent', 0) * 100) if info.get('pledgedPercent') else None,
            
            # Key Statistics (Added from User Discovery)
            "book_value": info.get('bookValue'),
            "ebitda": info.get('ebitda'),
            "held_percent_institutions": (info.get('heldPercentInstitutions', 0) * 100) if info.get('heldPercentInstitutions') else None,
            "shares_outstanding": info.get('sharesOutstanding'),
            
            # -- Phase 3: Health & Sentiment --
            "analyst_rating": info.get('recommendationMean'),
            "recommendation_key": info.get('recommendationKey'),
            "total_cash": info.get('totalCash'),
            "total_debt": info.get('totalDebt'),
            "current_ratio": info.get('currentRatio'),
            
            # -- Phase 3: Price Action Statistics --
            "fifty_two_week_high": info.get('fiftyTwoWeekHigh'),
            "fifty_two_week_low": info.get('fiftyTwoWeekLow'),
            "fifty_two_week_change": info.get('52WeekChange'),
            "beta": info.get('beta')
        }
        
        # 2. Advanced Valuation from direct TimeSeries API
        adv_val = await fetch_valuation_timeseries(symbol, yf_sym)
        result.update(adv_val)
        
        # Determine data quality
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
