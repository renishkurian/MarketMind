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
    slug = screener_symbol or symbol.upper()
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
            r'<li[^>]*>\s*<span[^>]*>(.*?)</span>\s*<span[^>]*>(.*?)</span\s*>',
            re.DOTALL | re.IGNORECASE
        )
        for m in li_pattern.finditer(search_html):
            key = re.sub(r'<[^>]+>', '', m.group(1)).strip().lower()
            raw = re.sub(r'<[^>]+>', ' ', m.group(2))
            num_m = re.search(r'([\d,]+(?:\.\d+)?)', raw)
            val = num_m.group(1).replace(',', '') if num_m else ''
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
        result["roce"]             = _f("roce")
        result["book_value"]       = _f("book value")
        result["market_cap"]       = _f("market cap")
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
            # OPM % is in the P&L table as <td>OPM %</td> followed by td cells; grab last annual value
            opm_row = re.search(r'OPM\s*%.*?</tr>', html, re.DOTALL | re.IGNORECASE)
            if opm_row:
                opm_vals = re.findall(r'<td[^>]*>\s*([\d.]+)%?\s*</td>', opm_row.group(0))
                if opm_vals:
                    try: result["operating_margin"] = float(opm_vals[-2] if len(opm_vals) > 1 else opm_vals[-1])  # skip TTM, use last annual
                    except: pass
            if result.get("operating_margin") is None:
                result["operating_margin"] = _re_val(r'OPM[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("pb_ratio") is None:
            result["pb_ratio"] = _re_val(r'Price to Book[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("ev_ebitda") is None:
            result["ev_ebitda"] = _re_val(r'EV\s*/\s*EBITDA[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')
        if result.get("current_ratio") is None:
            result["current_ratio"] = _re_val(r'Current [Rr]atio[^<]*</span>\s*<span[^>]*>\s*([\d.]+)')

        # ── Strategy 3: Shareholding Pattern (Promoter, FII, DII, Public) ──
        def _extract_shareholding(label_re: str) -> Optional[float]:
            # Search for the label followed by the rest of the row
            # Use non-greedy match to stay within one row
            row_match = re.search(r'<tr[^>]*>.*?' + label_re + r'.*?</tr>', html, re.DOTALL | re.IGNORECASE)
            if row_match:
                # Find all percentages in that row
                row_html = row_match.group(0)
                vals = re.findall(r'<td[^>]*>\s*([\d.]+)\s*%?\s*</td>', row_html)
                if vals:
                    try: return float(vals[-1])
                    except: pass
            return None

        result["promoter_holding"] = _extract_shareholding(r'Promoters?')
        result["fii_holding"]      = _extract_shareholding(r'FIIs?')
        result["dii_holding"]      = _extract_shareholding(r'DIIs?')
        result["public_holding"]   = _extract_shareholding(r'Public')

        # Pledged %
        pledge_patterns = [
            r'[Pp]ledged\s+[Pp]ercentage[^<]*</td>\s*<td[^>]*>([\d.]+)',
            r'[Pp]ledged[^<]*</td>\s*(?:<td[^>]*>[\d.]*\s*</td>\s*)*<td[^>]*>([\d.]+)',
            r'[Pp]ledge[^<]*:\s*([\d.]+)\s*%',
            r'>\s*Pledged\s*percentage.*?<td[^>]*>\s*([\d.]+)',
        ]
        for pat in pledge_patterns:
            m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
            if m:
                try:
                    result["promoter_pledge_pct"] = float(m.group(1))
                    break
                except (ValueError, TypeError):
                    pass

        # ── Strategy 4: CAGR growth from compounded tables ───────────────
        def _extract_cagr(label_re: str, years_list: List[int]) -> Dict[str, float]:
            extracted = {}
            for yr in years_list:
                # Try various patterns for each year
                patterns = [
                    label_re + r'.*?' + str(yr) + r'\s+Years?\s*[:\-]\s*([\d.]+)\s*%',
                    label_re + r'.*?<td[^>]*>\s*' + str(yr) + r'\s+[Yy]ears?:?\s*</td>\s*<td[^>]*>\s*([\d.]+)',
                    label_re + r'.*?' + str(yr) + r'\s+[Yy]rs?\s*[:\-]?\s*([\d.]+)\s*%',
                ]
                for pat in patterns:
                    m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
                    if m:
                        try:
                            extracted[yr] = float(m.group(1))
                            break
                        except: pass
            return extracted

        sales_cagr = _extract_cagr(r'[Cc]ompounded\s+[Ss]ales\s+[Gg]rowth', [3, 5, 10])
        result["revenue_growth_3yr"] = sales_cagr.get(3)
        result["revenue_cagr_3yr"]   = sales_cagr.get(3)
        result["revenue_cagr_5yr"]   = sales_cagr.get(5)
        result["revenue_cagr_10yr"]  = sales_cagr.get(10)

        profit_cagr = _extract_cagr(r'[Cc]ompounded\s+[Pp]rofit\s+[Gg]rowth', [3, 5, 10])
        result["pat_growth_3yr"]  = profit_cagr.get(3)
        result["profit_cagr_3yr"] = profit_cagr.get(3)
        result["profit_cagr_5yr"] = profit_cagr.get(5)
        result["profit_cagr_10yr"] = profit_cagr.get(10)

        price_cagr = _extract_cagr(r'[Ss]tock\s+[Pp]rice\s+CAGR', [1, 3, 5, 10])
        result["price_cagr_1yr"]  = price_cagr.get(1)
        result["price_cagr_3yr"]  = price_cagr.get(3)
        result["price_cagr_5yr"]  = price_cagr.get(5)
        result["price_cagr_10yr"] = price_cagr.get(10)

        roe_cagr = _extract_cagr(r'[Rr]eturn\s+on\s+[Ee]quity', [3, 5, 10])
        result["roe_3yr_avg"]  = roe_cagr.get(3)
        result["roe_avg_3yr"]  = roe_cagr.get(3)
        result["roe_avg_5yr"]  = roe_cagr.get(5)
        result["roe_avg_10yr"] = roe_cagr.get(10)

        # ── Strategy 5: Enhanced Ratios (Dividend, Face Value, Efficiency) ──
        result["dividend_yield"]  = _f("dividend yield", "div yield")
        result["dividend_payout"] = _f("dividend payout", "div payout")
        result["face_value"]      = _f("face value")
        result["debtor_days"]     = _f("debtor days")
        result["inventory_days"]  = _f("inventory days")
        result["days_payable"]    = _f("days payable", "payable days")
        result["working_capital_days"] = _f("working capital days", "wc days")
        result["cash_conversion_cycle"] = _f("cash conversion cycle", "ccc")

        # Fallback from ANY table if missing from top summary
        def _extract_from_any_table(label_re: str) -> Optional[float]:
            # Search for the label in any table row
            row_match = re.search(r'<tr[^>]*>.*?>(?:' + label_re + r').*?</tr>', html, re.DOTALL | re.IGNORECASE)
            if row_match:
                # Find all numbers in that row (usually percentages or days)
                vals = re.findall(r'<td[^>]*>\s*([\d.]+)\s*%?\s*</td>', row_match.group(0))
                if vals:
                    # Usually the last value is the most recent (TTM or last FY)
                    try: return float(vals[-1])
                    except: pass
            return None

        if result.get("debtor_days") is None:
            result["debtor_days"] = _extract_from_any_table(r'Debtor\s+Days')
        if result.get("inventory_days") is None:
            result["inventory_days"] = _extract_from_any_table(r'Inventory\s+Days')
        if result.get("working_capital_days") is None:
            result["working_capital_days"] = _extract_from_any_table(r'Working\s+Capital\s+Days')
        if result.get("dividend_payout") is None:
            result["dividend_payout"] = _extract_from_any_table(r'Dividend\s+Payout\s*%')

        # ── Strategy 6: PE 5yr avg fallback (if not in ratios) ─────────────
        if not result.get("pe_5yr_avg"):
            pe_hist = re.findall(r'Price to [Ee]arn(?:ing|ings?).*?<td[^>]*>([\d.]+)</td>', html, re.DOTALL)
            if len(pe_hist) >= 5:
                try:
                    result["pe_5yr_avg"] = round(sum(float(x) for x in pe_hist[-5:]) / 5, 2)
                except: pass

        # ── Strategy 7: Extract Rich Data Tables ──────────────────────────
        def _extract_rich_table(section_id: str) -> List[Dict[str, Any]]:
            # Match section with data-result-table
            sect = re.search(f'id=["\']{section_id}["\'].*?data-result-table>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
            if not sect: return []
            tbl = sect.group(1)
            # Headers
            headers = []
            th_matches = re.findall(r'<th[^>]*>(.*?)</th>', tbl, re.DOTALL)
            for th in th_matches:
                txt = re.sub(r'<[^>]+>', '', th).strip()
                if txt: headers.append(txt)
            # Rows
            rows = []
            tr_matches = re.findall(r'<tr[^>]*>(.*?)</tr>', tbl, re.DOTALL)
            for tr in tr_matches:
                tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
                if not tds: continue
                metric = re.sub(r'<[^>]+>', '', tds[0]).replace('&nbsp;', ' ').strip()
                if not metric: continue
                row_obj = {"Report Date": metric}
                for i, td in enumerate(tds[1:]):
                    if i < len(headers):
                        val = re.sub(r'<[^>]+>', '', td).replace(',', '').replace('%', '').strip()
                        row_obj[headers[i]] = val
                rows.append(row_obj)
            return rows

        result["quarterly_results"]    = _extract_rich_table("quarters")
        result["annual_pnl"]           = _extract_rich_table("profit-loss")
        result["annual_balance_sheet"] = _extract_rich_table("balance-sheet")
        result["annual_cashflows"]      = _extract_rich_table("cash-flow")
        result["annual_ratios"]         = _extract_rich_table("ratios")
        result["shareholding_history"]  = _extract_rich_table("shareholding")

        # ── Strategy 8: Pros & Cons ──────────────────────────────────────
        pros = re.search(r'class=["\']eight columns pro["\'].*?<ul>(.*?)</ul>', html, re.DOTALL | re.IGNORECASE)
        if pros:
            result["screener_pros"] = [re.sub(r'<[^>]+>', '', li).strip() for li in re.findall(r'<li>(.*?)</li>', pros.group(1), re.DOTALL)]
        cons = re.search(r'class=["\']eight columns con["\'].*?<ul>(.*?)</ul>', html, re.DOTALL | re.IGNORECASE)
        if cons:
            result["screener_cons"] = [re.sub(r'<[^>]+>', '', li).strip() for li in re.findall(r'<li>(.*?)</li>', cons.group(1), re.DOTALL)]

        # ── Strategy 9: Metadata (About, Sector, Industry) ────────────────
        about = re.search(r'class=["\']about["\'][^>]*>(.*?)</div>', html, re.DOTALL | re.IGNORECASE)
        if about:
            result["about_text"] = re.sub(r'<[^>]+>', '', about.group(1)).strip()
        
        intro = re.search(r'class=["\']introduction["\'][^>]*>(.*?)</div>', html, re.DOTALL | re.IGNORECASE)
        if intro:
            links = re.findall(r'<a[^>]*>(.*?)</a>', intro.group(1))
            if len(links) >= 2:
                result["sector"] = links[0].strip()
                result["industry"] = links[1].strip()

        # Adjust units
        if result.get("market_cap"):
            # If the value is in Cr (typical Screener), and we want it in Absolute for FundamentalsCache
            # We also store a market_cap_cr for ScreenerCache specifically
            result["market_cap_cr"] = result["market_cap"]
            result["market_cap"]    = int(result["market_cap"] * 10_000_000)

        # Remove None/empty values
        result = {k: v for k, v in result.items() if v is not None}
        logger.info(f"Screener.in filled {len(result)} fields for {symbol}: {list(result.keys())}")

    except Exception as e:
        logger.warning(f"Screener.in fetch failed for {symbol}: {e!r}", exc_info=True)

    # ── yfinance fallback: fill missing computed fields ──────────────────────
    missing_computed = [f for f in (
        "revenue_growth_3yr", "pat_growth_3yr", "operating_margin",
        "roe_3yr_avg", "pe_5yr_avg", "debt_equity", "current_ratio", "roe"
    ) if not result.get(f)]

    if missing_computed:
        try:
            yf_sym = _get_yf_symbol(symbol)
            ticker = yf.Ticker(yf_sym)
            fin = ticker.financials
            bs  = ticker.balance_sheet
            cf  = ticker.cashflow

            def _yf_row(df, *names):
                if df is None or df.empty:
                    return []
                for name in names:
                    for idx in df.index:
                        if name.lower() in str(idx).lower():
                            vals = df.loc[idx].dropna().tolist()
                            return [float(v) for v in vals if v != 0]
                return []

            if "revenue_growth_3yr" in missing_computed:
                rev = _yf_row(fin, "Total Revenue", "Revenue")
                if len(rev) >= 4:
                    try:
                        result["revenue_growth_3yr"] = round(((rev[0] / rev[3]) ** (1/3) - 1) * 100, 2)
                    except Exception:
                        pass

            if "pat_growth_3yr" in missing_computed:
                pat = _yf_row(fin, "Net Income", "Net Income Common Stockholders")
                if len(pat) >= 4 and pat[3] > 0 and pat[0] > 0:
                    try:
                        result["pat_growth_3yr"] = round(((pat[0] / pat[3]) ** (1/3) - 1) * 100, 2)
                    except Exception:
                        pass

            if "operating_margin" in missing_computed:
                ebit = _yf_row(fin, "EBIT", "Operating Income")
                rev  = _yf_row(fin, "Total Revenue", "Revenue")
                if ebit and rev:
                    try:
                        result["operating_margin"] = round((ebit[0] / rev[0]) * 100, 2)
                    except Exception:
                        pass

            if "roe" in missing_computed:
                ni = _yf_row(fin, "Net Income")
                eq = _yf_row(bs, "Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity")
                if ni and eq:
                    try:
                        result["roe"] = round((ni[0] / eq[0]) * 100, 2)
                    except Exception:
                        pass

            if "roe_3yr_avg" in missing_computed:
                ni_list = _yf_row(fin, "Net Income")
                eq_list = _yf_row(bs, "Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity")
                pairs = min(len(ni_list), len(eq_list), 3)
                if pairs >= 2:
                    try:
                        roe_vals = [(ni_list[i] / eq_list[i]) * 100 for i in range(pairs) if eq_list[i] != 0]
                        if roe_vals:
                            result["roe_3yr_avg"] = round(sum(roe_vals) / len(roe_vals), 2)
                    except Exception:
                        pass

            if "debt_equity" in missing_computed:
                debt = _yf_row(bs, "Total Debt", "Long Term Debt")
                eq   = _yf_row(bs, "Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity")
                if debt and eq and eq[0] != 0:
                    try:
                        result["debt_equity"] = round(debt[0] / eq[0], 2)
                    except Exception:
                        pass

            if "current_ratio" in missing_computed:
                ca = _yf_row(bs, "Current Assets", "Total Current Assets")
                cl = _yf_row(bs, "Current Liabilities", "Total Current Liabilities")
                if ca and cl and cl[0] != 0:
                    try:
                        result["current_ratio"] = round(ca[0] / cl[0], 2)
                    except Exception:
                        pass

            yf_filled = [f for f in missing_computed if result.get(f) is not None]
            if yf_filled:
                logger.info(f"yfinance fallback filled {len(yf_filled)} fields for {symbol}: {yf_filled}")

        except Exception as e:
            logger.warning(f"yfinance fallback failed for {symbol}: {e!r}")

    result = {k: v for k, v in result.items() if v is not None}
    return result


# ── NSE Corporate Actions (Layer 0) ──────────────────────────────────────────
import time as _time
import re as _re

# In-memory cache: { "data": [...], "fetched_at": float }
_NSE_CORP_CACHE: Dict[str, Any] = {}
_NSE_CORP_CACHE_TTL = 6 * 3600  # 6 hours

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}


async def _fetch_nse_corp_actions_raw() -> List[Dict[str, Any]]:
    """
    Fetch the full NSE corporate actions calendar (all equities).
    Requires a two-step request: seed session cookies via homepage, then
    call the API endpoint. Results are cached for _NSE_CORP_CACHE_TTL seconds.
    """
    now = _time.monotonic()
    cached = _NSE_CORP_CACHE.get("data")
    fetched_at = _NSE_CORP_CACHE.get("fetched_at", 0)
    if cached is not None and (now - fetched_at) < _NSE_CORP_CACHE_TTL:
        return cached

    api_url = "https://www.nseindia.com/api/corporates-corporateActions?index=equities"
    try:
        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers=_NSE_HEADERS,
        ) as client:
            # Step 1: seed session cookies
            await client.get("https://www.nseindia.com", timeout=10.0)
            # Step 2: fetch corporate actions
            resp = await client.get(api_url)
            if resp.status_code != 200:
                logger.warning(f"NSE corporate actions API returned {resp.status_code}")
                return cached or []
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("NSE corporate actions: unexpected response format")
                return cached or []

        _NSE_CORP_CACHE["data"] = data
        _NSE_CORP_CACHE["fetched_at"] = now
        logger.info(f"NSE corporate actions: fetched {len(data)} records, cached for {_NSE_CORP_CACHE_TTL//3600}h")
        return data

    except Exception as e:
        logger.warning(f"NSE corporate actions fetch failed: {e!r}")
        return cached or []


def _parse_nse_date(date_str: Optional[str]) -> Optional[str]:
    """Convert NSE date strings like '10-Apr-2026' to ISO '2026-04-10'."""
    if not date_str or date_str.strip() in ("-", ""):
        return None
    try:
        from datetime import datetime as _dt
        for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return _dt.strptime(date_str.strip(), fmt).date().isoformat()
            except ValueError:
                continue
    except Exception:
        pass
    return date_str.strip()


def _classify_nse_action(purpose: str) -> str:
    """Classify a free-text NSE purpose string into a canonical action type."""
    p = (purpose or "").lower()
    if "dividend" in p or "interim dividend" in p or "final dividend" in p:
        return "DIVIDEND"
    if "bonus" in p:
        return "BONUS"
    if "split" in p or "sub-division" in p or "sub division" in p:
        return "SPLIT"
    if "buyback" in p or "buy-back" in p or "buy back" in p:
        return "BUYBACK"
    if "rights" in p:
        return "RIGHTS"
    return "OTHER"


def _extract_dividend_amount(purpose: str) -> Optional[float]:
    """Extract rupee amount from strings like 'Dividend - Rs 10 Per Share'."""
    if not purpose:
        return None
    m = _re.search(r'(?:rs\.?|re\.?|₹)\s*([\d,]+(?:\.\d+)?)', purpose, _re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    # Try plain number after dash  e.g. "Dividend - 5.00"
    m = _re.search(r'-\s*([\d]+(?:\.\d+)?)', purpose)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _extract_bonus_ratio(purpose: str) -> Optional[str]:
    """Extract bonus ratio from strings like 'Bonus Issue  - 1:1' or '2:3'."""
    if not purpose:
        return None
    m = _re.search(r'(\d+)\s*:\s*(\d+)', purpose)
    if m:
        return f"{m.group(1)}:{m.group(2)}"
    return None


async def fetch_nse_corporate_actions(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetch and filter NSE corporate actions for a specific symbol using the 'nse' library.

    Returns a dict with:
      - upcoming_dividend  : closest future dividend (ex_date, amount, purpose, source)
      - upcoming_bonus     : closest future bonus issue (ex_date, ratio, purpose, source)
      - upcoming_split     : closest future split (ex_date, ratio, purpose, source)
      - upcoming_buyback   : closest future buyback (ex_date, purpose, source)
      - recent_actions     : last 10 actions of any type (sorted newest first)
      - raw_nse_actions    : full filtered list for the symbol

    Returns None on total failure.
    """
    from datetime import date as _date, timedelta
    from nse import NSE
    import os

    try:
        sym_upper = symbol.upper().strip()
        
        # We fetch for a wide range: 1 year back to 6 months forward to cover recent and upcoming
        today_obj = _date.today()
        d_from = today_obj - timedelta(days=365)
        d_to = today_obj + timedelta(days=180)

        # Use a local cache directory for the nse library
        nse_cache_dir = os.path.join(os.getcwd(), ".nse_cache")
        os.makedirs(nse_cache_dir, exist_ok=True)

        with NSE(download_folder=nse_cache_dir) as nse:
            # We fetch specifically for this symbol
            raw_actions = nse.actions(
                segment='equities', 
                symbol=sym_upper, 
                from_date=d_from, 
                to_date=d_to
            )

        if not raw_actions:
            logger.debug(f"NSE corporate actions: no records found for {sym_upper}")
            return None

        today_iso = today_obj.isoformat()
        upcoming_dividend = None
        upcoming_bonus    = None
        upcoming_split    = None
        upcoming_buyback  = None
        all_parsed        = []

        for a in raw_actions:
            # Field mapping from nse library:
            # 'subject' = description
            # 'exDate'  = ex-date
            # 'recDate' = record date
            purpose   = a.get("subject") or ""
            ex_date   = _parse_nse_date(a.get("exDate"))
            rec_date  = _parse_nse_date(a.get("recDate"))
            bc_start  = _parse_nse_date(a.get("bcStartDate"))
            bc_end    = _parse_nse_date(a.get("bcEndDate"))
            action_type = _classify_nse_action(purpose)

            parsed = {
                "symbol":      sym_upper,
                "company":     a.get("comp", ""),
                "purpose":     purpose,
                "action_type": action_type,
                "ex_date":     ex_date,
                "record_date": rec_date,
                "bc_start":    bc_start,
                "bc_end":      bc_end,
                "source":      "NSE",
            }

            # Enrich by type
            if action_type == "DIVIDEND":
                parsed["amount"] = _extract_dividend_amount(purpose)
            elif action_type in ("BONUS", "SPLIT"):
                parsed["ratio"] = _extract_bonus_ratio(purpose)

            all_parsed.append(parsed)

        # Sort all actions: future first (ascending ex_date), then past (descending)
        def _sort_key(a):
            d = a.get("ex_date") or "0000-00-00"
            is_future = d >= today_iso
            return (0 if is_future else 1, d if is_future else "9999" + d)

        all_parsed.sort(key=_sort_key)

        # Pick closest upcoming for each type
        for a in all_parsed:
            d = a.get("ex_date") or ""
            if d < today_iso:
                break  # We've passed future items, no point continuing
            at = a["action_type"]
            if at == "DIVIDEND" and upcoming_dividend is None:
                upcoming_dividend = {k: a[k] for k in ("ex_date", "record_date", "amount", "purpose", "source") if k in a}
                upcoming_dividend["confirmed"] = True
            elif at == "BONUS" and upcoming_bonus is None:
                upcoming_bonus = {k: a[k] for k in ("ex_date", "record_date", "ratio", "purpose", "source") if k in a}
                upcoming_bonus["confirmed"] = True
            elif at == "SPLIT" and upcoming_split is None:
                upcoming_split = {k: a[k] for k in ("ex_date", "record_date", "ratio", "purpose", "source") if k in a}
                upcoming_split["confirmed"] = True
            elif at == "BUYBACK" and upcoming_buyback is None:
                upcoming_buyback = {k: a[k] for k in ("ex_date", "record_date", "purpose", "source") if k in a}
                upcoming_buyback["confirmed"] = True

        # Recent actions — last 10 by ex_date descending (past events)
        past = [a for a in all_parsed if (a.get("ex_date") or "") < today_iso]
        past.sort(key=lambda a: a.get("ex_date") or "", reverse=True)
        recent_actions = past[:10]

        return {
            "upcoming_dividend": upcoming_dividend,
            "upcoming_bonus":    upcoming_bonus,
            "upcoming_split":    upcoming_split,
            "upcoming_buyback":  upcoming_buyback,
            "recent_actions":    recent_actions,
            "raw_nse_actions":   all_parsed,
        }

    except Exception as e:
        logger.warning(f"fetch_nse_corporate_actions failed for {symbol}: {e!r}")
        return None
