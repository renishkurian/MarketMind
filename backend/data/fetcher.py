import yfinance as yf
import pandas as pd
import asyncio
import httpx
import logging
import urllib.request
import re
from typing import List, Dict, Any, Union, Optional
from datetime import datetime

from backend.utils.market_hours import get_current_ist_time

logger = logging.getLogger(__name__)

def _get_yf_symbol(symbol: str) -> str:
    """Legacy helper. Callers should transition to using DB-defined yahoo_symbol."""
    return f"{symbol}.NS"

async def _get_live_ltp_google_async(symbol: str) -> Optional[float]:
    """Asynchronous scraper for zero-delay Indian stock prices using Google Finance."""
    url = f"https://www.google.com/finance/quote/{symbol}:NSE"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                logger.debug(f"Google Finance {symbol} returned {resp.status_code}")
                return None
                
            html = resp.text
            
            # Pattern 1: data-last-price attribute (Modern)
            match = re.search(r'data-last-price="([^"]+)"', html)
            if match:
                return float(match.group(1).replace(',', ''))
                
            # Pattern 2: JSON-LD or script context (Fallback)
            # Google often embeds current price in a JSON structure
            match = re.search(r'\["(\d+\.\d+)",\d+,\d+,"INR"\]', html)
            if match:
                return float(match.group(1))

            # Pattern 3: Simple currency pattern (Last resort)
            # Look for ₹ symbol or specific class names often used for price
            match = re.search(r'₹(\d+,?\d*\.?\d*)', html)
            if match:
                return float(match.group(1).replace(',', ''))

    except Exception as e:
        logger.debug(f"Google scraper error for {symbol}: {e}")
    return None

async def _fetch_hybrid_overrides(internal_symbols: List[str]) -> Dict[str, float]:
    """Concurrently scrapes Google Finance for a batch of symbols."""
    overrides = {}
    
    async def fetch_one(sym: str):
        val = await _get_live_ltp_google_async(sym)
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
            df = yf.download(batch_yf, period='1d', interval='1m', progress=False, auto_adjust=False, threads=False)
            
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
            df = yf.download(batch_yf, period='1d', interval='5m', progress=False, auto_adjust=False, threads=False)
            
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
            "roe": (info.get('returnOnEquity') * 100) if info.get('returnOnEquity') else None,
            "debt_equity": info.get('debtToEquity'),
            "revenue_growth": (info.get('revenueGrowth') * 100) if info.get('revenueGrowth') else None,
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
        df = yf.download(yf_sym, period='max', interval='1d', progress=False, auto_adjust=False, threads=False)
        return df
    except Exception as e:
        logger.error(f"Error fetching max history for {symbol} (ticker: {yf_sym}): {e}")
        return pd.DataFrame()


_SCREENER_SLUG_MAP: Dict[str, str] = {
    "AARTIIND": "aarti-industries", "ABBOTINDIA": "abbott-india", "ABCAPITAL": "aditya-birla-capital",
    "ABFRL": "aditya-birla-fashion-and-retail", "ABB": "abb-india", "ACC": "acc",
    "ADANIENT": "adani-enterprises", "ADANIGREEN": "adani-green-energy",
    "ADANIPORTS": "adani-ports-and-special-economic-zone", "ADANIPOWER": "adani-power",
    "ADANITRANS": "adani-transmission", "AFFLE": "affle-india", "AIAENG": "aia-engineering",
    "AJANTPHARM": "ajanta-pharma", "ALKEM": "alkem-laboratories",
    "ALKYLAMINE": "alkyl-amines-chemicals", "AMARAJABAT": "amara-raja-batteries",
    "AMBUJACEM": "ambuja-cements", "ANGELONE": "angel-one", "APLAPOLLO": "apl-apollo-tubes",
    "APOLLOHOSP": "apollo-hospitals-enterprise", "APOLLOTYRE": "apollo-tyres",
    "ASHOKLEY": "ashok-leyland", "ASIANPAINT": "asian-paints", "ASTRAL": "astral",
    "ATUL": "atul", "AUBANK": "au-small-finance-bank", "AUROPHARMA": "aurobindo-pharma",
    "AVANTIFEED": "avanti-feeds", "AXISBANK": "axis-bank",
    "BAJAJ-AUTO": "bajaj-auto", "BAJAJFINSV": "bajaj-finserv", "BAJFINANCE": "bajaj-finance",
    "BALKRISIND": "balkrishna-industries", "BANDHANBNK": "bandhan-bank",
    "BANKBARODA": "bank-of-baroda", "BANKINDIA": "bank-of-india", "BATAINDIA": "bata-india",
    "BEL": "bharat-electronics", "BERGEPAINT": "berger-paints-india",
    "BHARATFORG": "bharat-forge", "BHARTIARTL": "bharti-airtel",
    "BHEL": "bharat-heavy-electricals", "BIOCON": "biocon", "BOSCHLTD": "bosch",
    "BPCL": "bharat-petroleum-corporation", "BRITANNIA": "britannia-industries",
    "CANBK": "canara-bank", "CASTROLIND": "castrol-india",
    "CDSL": "central-depository-services-india", "CESC": "cesc",
    "CGPOWER": "cg-power-and-industrial-solutions",
    "CHAMBLFERT": "chambal-fertilisers-and-chemicals",
    "CHOLAFIN": "cholamandalam-investment-and-finance-company", "CIPLA": "cipla",
    "COALINDIA": "coal-india", "COFORGE": "coforge", "COLPAL": "colgate-palmolive-india",
    "CONCOR": "container-corporation-of-india", "COROMANDEL": "coromandel-international",
    "CUMMINSIND": "cummins-india", "DABUR": "dabur-india", "DALBHARAT": "dalmia-bharat",
    "DEEPAKFERT": "deepak-fertilisers-and-petrochemicals", "DEEPAKNTR": "deepak-nitrite",
    "DELHIVERY": "delhivery", "DIVISLAB": "divi-s-laboratories",
    "DIXON": "dixon-technologies-india", "DLF": "dlf", "DRREDDY": "dr-reddys-laboratories",
    "EICHERMOT": "eicher-motors", "ELGIEQUIP": "elgi-equipments", "EMAMILTD": "emami",
    "ENGINERSIN": "engineers-india", "ESCORTS": "escorts-kubota", "EXIDEIND": "exide-industries",
    "FEDERALBNK": "federal-bank", "FINEORG": "fine-organic-industries",
    "FINPIPE": "finolex-industries", "FLUOROCHEM": "gujarat-fluorochemicals",
    "FORTIS": "fortis-healthcare", "GAIL": "gail-india",
    "GLENMARK": "glenmark-pharmaceuticals", "GMRINFRA": "gmr-airports-infrastructure",
    "GODREJCP": "godrej-consumer-products", "GODREJIND": "godrej-industries",
    "GODREJPROP": "godrej-properties", "GRANULES": "granules-india", "GRASIM": "grasim-industries",
    "GSPL": "gujarat-state-petronet", "GUJGASLTD": "gujarat-gas",
    "HAPPSTMNDS": "happiest-minds-technologies", "HCLTECH": "hcl-technologies",
    "HDFCAMC": "hdfc-asset-management-company", "HDFCBANK": "hdfc-bank",
    "HDFCLIFE": "hdfc-life-insurance-company", "HEROMOTOCO": "hero-motocorp", "HFCL": "hfcl",
    "HINDALCO": "hindalco-industries", "HINDCOPPER": "hindustan-copper",
    "HINDPETRO": "hindustan-petroleum-corporation", "HINDUNILVR": "hindustan-unilever",
    "HONAUT": "honeywell-automation-india", "ICICIBANK": "icici-bank",
    "ICICIGI": "icici-lombard-general-insurance-company",
    "ICICIPRULI": "icici-prudential-life-insurance-company", "IDFCFIRSTB": "idfc-first-bank",
    "IEX": "indian-energy-exchange", "IGL": "indraprastha-gas",
    "INDHOTEL": "indian-hotels-company", "INDIAMART": "indiamart-intermesh",
    "INDIGO": "interglobe-aviation", "INDUSINDBK": "indusind-bank",
    "INDUSTOWER": "indus-towers", "INFY": "infosys", "IOC": "indian-oil-corporation",
    "IPCALAB": "ipca-laboratories",
    "IRCTC": "indian-railway-catering-and-tourism-corporation",
    "IRFC": "indian-railway-finance-corporation", "ITC": "itc",
    "JINDALSTEL": "jindal-steel-and-power", "JKCEMENT": "jk-cement",
    "JSWENERGY": "jsw-energy", "JSWSTEEL": "jsw-steel", "JUBLFOOD": "jubilant-foodworks",
    "KAJARIACER": "kajaria-ceramics", "KANSAINER": "kansai-nerolac-paints",
    "KOTAKBANK": "kotak-mahindra-bank", "KPITTECH": "kpit-technologies",
    "L&TFH": "l-t-finance", "LAURUSLABS": "laurus-labs", "LICHSGFIN": "lic-housing-finance",
    "LICI": "life-insurance-corporation-of-india", "LINDEINDIA": "linde-india",
    "LT": "larsen-and-toubro", "LTIM": "ltimindtree", "LTTS": "l-t-technology-services",
    "LUPIN": "lupin", "M&M": "mahindra-and-mahindra",
    "M&MFIN": "mahindra-and-mahindra-financial-services", "MANAPPURAM": "manappuram-finance",
    "MARICO": "marico", "MARUTI": "maruti-suzuki-india", "MAXHEALTH": "max-healthcare-institute",
    "MCX": "multi-commodity-exchange-of-india", "METROPOLIS": "metropolis-healthcare",
    "MFSL": "max-financial-services", "MPHASIS": "mphasis", "MRF": "mrf",
    "MUTHOOTFIN": "muthoot-finance", "NATIONALUM": "national-aluminium-company",
    "NAUKRI": "info-edge-india", "NAVINFLUOR": "navin-fluorine-international",
    "NESTLEIND": "nestle-india", "NMDC": "nmdc", "NTPC": "ntpc",
    "OFSS": "oracle-financial-services-software", "OIL": "oil-india",
    "ONGC": "oil-and-natural-gas-corporation", "PAGEIND": "page-industries",
    "PATANJALI": "patanjali-foods", "PERSISTENT": "persistent-systems",
    "PETRONET": "petronet-lng", "PFIZER": "pfizer", "PHOENIXLTD": "phoenix-mills",
    "PIDILITIND": "pidilite-industries", "PIIND": "pi-industries", "PNB": "punjab-national-bank",
    "POLYCAB": "polycab-india", "POWERGRID": "power-grid-corporation-of-india",
    "PRESTIGE": "prestige-estates-projects", "RAIN": "rain-industries",
    "RAJESHEXPO": "rajesh-exports", "RAMCOCEM": "the-ramco-cements", "RECLTD": "rec",
    "RELIANCE": "reliance-industries", "RITES": "rites",
    "SAIL": "steel-authority-of-india", "SBICARD": "sbi-cards-and-payment-services",
    "SBILIFE": "sbi-life-insurance-company", "SBIN": "state-bank-of-india",
    "SCHAEFFLER": "schaeffler-india", "SHREECEM": "shree-cement",
    "SHRIRAMFIN": "shriram-finance", "SIEMENS": "siemens", "SOBHA": "sobha",
    "SOLARINDS": "solar-industries-india", "SONACOMS": "sona-blw-precision-forgings",
    "SUNPHARMA": "sun-pharmaceutical-industries", "SUNTV": "sun-tv-network",
    "SUPREMEIND": "supreme-industries", "SUZLON": "suzlon-energy",
    "TANLA": "tanla-platforms", "TATACHEM": "tata-chemicals",
    "TATACOMM": "tata-communications", "TATACONSUM": "tata-consumer-products",
    "TATAELXSI": "tata-elxsi", "TATAMOTORS": "tata-motors",
    "TATAPOWER": "tata-power-company", "TATASTEEL": "tata-steel",
    "TCS": "tata-consultancy-services", "TECHM": "tech-mahindra", "THERMAX": "thermax",
    "TITAN": "titan-company", "TORNTPHARM": "torrent-pharmaceuticals",
    "TORNTPOWER": "torrent-power", "TRENT": "trent", "TRIDENT": "trident",
    "TVSMOTOR": "tvs-motor-company", "UBL": "united-breweries",
    "ULTRACEMCO": "ultratech-cement", "UNIONBANK": "union-bank-of-india",
    "UNITDSPR": "united-spirits", "VBL": "varun-beverages", "VEDL": "vedanta",
    "VOLTAS": "voltas", "WHIRLPOOL": "whirlpool-of-india", "WIPRO": "wipro",
    "ZOMATO": "zomato", "ZYDUSLIFE": "zydus-lifesciences",
}


async def fetch_screener_fundamentals(symbol: str, screener_symbol: str = None) -> Dict[str, Any]:
    """
    Scrape Screener.in for Indian-specific fundamental data missing from Yahoo.
    Robust multi-strategy extraction covering all ratio and growth fields.
    Slug resolution priority:
      1. screener_symbol arg (set via Edit Data / DB)
      2. _SCREENER_SLUG_MAP built-in lookup
      3. lowercase symbol as fallback
    """
    result: Dict[str, Any] = {}
    slug = screener_symbol or _SCREENER_SLUG_MAP.get(symbol.upper()) or symbol.lower()
    urls = [
        f"https://www.screener.in/company/{slug}/consolidated/",
        f"https://www.screener.in/company/{slug}/",
    ]

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.screener.in/",
            "DNT": "1",
        }
        html = ""
        fetched_url = ""
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            for url in urls:
                resp = await client.get(url, headers=headers)
                fetched_url = str(resp.url)
                if resp.status_code == 200:
                    html = resp.text
                    break
        if not html:
            logger.warning(f"Screener.in returned no usable response for {symbol} (slug={slug})")
            return result
        # Detect login wall / redirect to login page
        if "login" in fetched_url or 'id="login-form"' in html or "Please login" in html:
            logger.warning(f"Screener.in requires login for {symbol} — got redirected to: {fetched_url}")
            return result
        logger.info(f"[Screener debug] {symbol} slug={slug} url={fetched_url} HTML len={len(html)}, sample={html[200:600]!r}")

        # ── Strategy 1: Extract all <li> ratio items (handles any class structure) ──
        # Screener structure: <li ...><span ...>Label</span><span ...>Value</span></li>
        # Also handles: <li><span class="name">...</span><span class="nowrap">...</span>
        ratios: Dict[str, str] = {}

        # Extract all text from ratio section between id="company-ratios" and next section
        ratios_section = re.search(
            r'id=["\'](?:top-ratios|company-ratios)["\'][^>]*>(.*?)</(?:ul|section|div)',
            html, re.DOTALL | re.IGNORECASE
        )
        search_html = ratios_section.group(1) if ratios_section else html

        # Match any <li> with two consecutive spans
        li_pattern = re.compile(
            r'<li[^>]*>\s*<span[^>]*>(.*?)</span>\s*<span[^>]*>(.*?)</span>',
            re.DOTALL | re.IGNORECASE
        )
        for m in li_pattern.finditer(search_html):
            key = re.sub(r'<[^>]+>', '', m.group(1)).strip().lower()
            val = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            val = val.replace(',', '').replace('%', '').replace('₹', '').replace(' ', '').strip()
            if key and val:
                ratios[key] = val

        # Also try dl/dt/dd structure that some Screener pages use
        dl_pattern = re.compile(r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', re.DOTALL | re.IGNORECASE)
        for m in dl_pattern.finditer(html):
            key = re.sub(r'<[^>]+>', '', m.group(1)).strip().lower()
            val = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            val = val.replace(',', '').replace('%', '').replace('₹', '').strip()
            if key and val:
                ratios[key] = val

        logger.debug(f"Screener ratio keys for {symbol}: {list(ratios.keys())}")

        def _f(*fragments) -> Optional[float]:
            for frag in fragments:
                frag_l = frag.lower()
                for k, v in ratios.items():
                    if frag_l in k:
                        try:
                            f = float(v)
                            if f != 0:
                                return f
                        except (ValueError, TypeError):
                            pass
            return None

        result["pe_ratio"]         = _f("stock p/e", "p/e", "price to earning")
        result["pb_ratio"]         = _f("price to book", "p/b value", "p/b")
        result["ev_ebitda"]        = _f("ev/ebitda", "ev / ebitda", "ebitda")
        result["roe"]              = _f("return on equity", "roe")
        result["debt_equity"]      = _f("debt to equity", "d/e ratio", "d/e")
        result["operating_margin"] = _f("opm", "operating profit margin", "operating margin")
        result["current_ratio"]    = _f("current ratio")

        # ── Strategy 2: Regex directly on full HTML for fields not in ratio list ──
        def _re_val(pattern: str) -> Optional[float]:
            m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if m:
                try:
                    return float(m.group(1).replace(',', '').replace('%', '').strip())
                except (ValueError, TypeError):
                    pass
            return None

        # Fallbacks for ratio fields using direct HTML patterns
        if result.get("debt_equity") is None:
            result["debt_equity"] = _re_val(r'[Dd]ebt to [Ee]quity[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("operating_margin") is None:
            result["operating_margin"] = _re_val(r'OPM[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("pb_ratio") is None:
            result["pb_ratio"] = _re_val(r'Price to Book[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("ev_ebitda") is None:
            result["ev_ebitda"] = _re_val(r'EV\s*/\s*EBITDA[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("current_ratio") is None:
            result["current_ratio"] = _re_val(r'Current [Rr]atio[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')

        # ── Strategy 3: Promoter holdings ────────────────────────────────
        # Find the latest promoter holding from the shareholding table
        # Screener format: Promoters button inside td, then td cells with percentages (newest last)
        prom_row = re.search(
            r'Promoters.*?</tr>',
            html, re.DOTALL | re.IGNORECASE
        )
        if prom_row:
            prom_vals = re.findall(r'<td[^>]*>([\d.]+)%?</td>', prom_row.group(0))
            if prom_vals:
                try:
                    v = float(prom_vals[-1])
                    if 0 < v <= 100:
                        result["promoter_holding"] = v
                except (ValueError, TypeError):
                    pass
        if not result.get("promoter_holding"):
            prom_patterns = [
                r'Promoters\s*</td>\s*(?:<td[^>]*>[\d.]*%?\s*</td>\s*)*<td[^>]*>([\d.]+)\s*%?',
                r'"Promoters"\s*[,:{]\s*"?([\d.]+)',
            ]
            for pat in prom_patterns:
                m = re.search(pat, html, re.IGNORECASE)
                if m:
                    try:
                        v = float(m.group(1))
                        if 0 < v <= 100:
                            result["promoter_holding"] = v
                            break
                    except (ValueError, TypeError):
                        pass

        # Pledged %
        pledge_patterns = [
            r'[Pp]ledged\s+[Pp]ercentage[^<]*</td>\s*<td[^>]*>([\d.]+)',
            r'[Pp]ledged[^<]*</td>\s*(?:<td[^>]*>[\d.]*\s*</td>\s*)*<td[^>]*>([\d.]+)',
            r'[Pp]ledge[^<]*:\s*([\d.]+)\s*%',
        ]
        for pat in pledge_patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                try:
                    result["promoter_pledge_pct"] = float(m.group(1))
                    break
                except (ValueError, TypeError):
                    pass

        # ── Strategy 4: CAGR growth from compounded tables ───────────────
        def _extract_cagr(label_re: str) -> Optional[float]:
            # Try: "3 Years: X %" inline format
            m = re.search(label_re + r'.*?3\s+Years?\s*[:\-]\s*([\d.]+)\s*%', html, re.DOTALL | re.IGNORECASE)
            if m:
                try: return float(m.group(1))
                except: pass
            # Try: table format with "3 Years:" cell (colon inside td — actual Screener format)
            m = re.search(label_re + r'.*?<td[^>]*>\s*3\s+[Yy]ears?:?\s*</td>\s*<td[^>]*>\s*([\d.]+)', html, re.DOTALL | re.IGNORECASE)
            if m:
                try: return float(m.group(1))
                except: pass
            # Try: "3 Yrs" variant
            m = re.search(label_re + r'.*?3\s+[Yy]rs?\s*[:\-]?\s*([\d.]+)\s*%', html, re.DOTALL | re.IGNORECASE)
            if m:
                try: return float(m.group(1))
                except: pass
            return None

        if not result.get("revenue_growth_3yr"):
            result["revenue_growth_3yr"] = _extract_cagr(r'[Cc]ompounded\s+[Ss]ales\s+[Gg]rowth')
        if not result.get("pat_growth_3yr"):
            result["pat_growth_3yr"] = _extract_cagr(r'[Cc]ompounded\s+[Pp]rofit\s+[Gg]rowth')

        # ── Strategy 5: ROE 3yr avg from annual table ─────────────────────
        if not result.get("roe_3yr_avg"):
            # Find ROE values in the annual P&L or ratios section
            roe_vals = re.findall(
                r'[Rr]eturn on [Ee]quity[^%]*?(\d{4})[^%\d]*([\d.]+)\s*%[^%\d]*(\d{4})[^%\d]*([\d.]+)\s*%[^%\d]*(\d{4})[^%\d]*([\d.]+)\s*%',
                html, re.DOTALL
            )
            if roe_vals:
                try:
                    vals = [float(roe_vals[0][i]) for i in (1, 3, 5)]
                    result["roe_3yr_avg"] = round(sum(vals) / 3, 2)
                except: pass
            if not result.get("roe_3yr_avg"):
                # Simpler: find 3+ numbers in ROE row
                roe_row = re.search(r'[Rr]eturn on [Ee]quity.*?(<tr.*?</tr>)', html, re.DOTALL)
                if roe_row:
                    nums = re.findall(r'<td[^>]*>\s*([\d.]+)\s*</td>', roe_row.group(1))
                    if len(nums) >= 3:
                        try:
                            vals = [float(x) for x in nums[-3:]]
                            result["roe_3yr_avg"] = round(sum(vals) / 3, 2)
                        except: pass

        # ── Strategy 6: PE 5yr avg ────────────────────────────────────────
        if not result.get("pe_5yr_avg"):
            pe_hist = re.findall(r'Price to [Ee]arn(?:ing|ings?).*?<td[^>]*>([\d.]+)</td>', html, re.DOTALL)
            if len(pe_hist) >= 5:
                try:
                    result["pe_5yr_avg"] = round(sum(float(x) for x in pe_hist[-5:]) / 5, 2)
                except: pass

        # Remove None/zero values
        result = {k: v for k, v in result.items() if v is not None}
        logger.info(f"Screener.in filled {len(result)} fields for {symbol}: {list(result.keys())}")
        return result

    except Exception as e:
        logger.warning(f"Screener.in fetch failed for {symbol}: {e!r}", exc_info=True)
        return result
