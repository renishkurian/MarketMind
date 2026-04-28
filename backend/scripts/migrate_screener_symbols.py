"""
migrate_screener_symbols.py
────────────────────────────────────────────────────────────────────────────────
One-time migration: populate stocks_master.screener_symbol for all known stocks.

Strategy (in order):
  1. Exact match in the built-in KNOWN_MAP (covers popular / tricky slugs).
  2. Auto-derive: lowercase the symbol, replace common suffixes, join with hyphens.
     e.g. ASHOKLEY → ashok-leyland  (via KNOWN_MAP),  RELIANCE → reliance
  3. Any symbol with no mapping is left NULL — can be set manually via Edit Data.

Run:
    cd /path/to/MarketMind
    python -m backend.scripts.migrate_screener_symbols

Or with dry-run to preview:
    python -m backend.scripts.migrate_screener_symbols --dry-run
"""

import asyncio
import argparse
import logging
from sqlalchemy import select, update, text
from backend.data.db import engine, SessionLocal, StockMaster

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Known mappings: NSE symbol → screener.in slug ────────────────────────────
# Add any symbol where the slug differs from just lowercasing the NSE ticker.
KNOWN_MAP: dict[str, str] = {
    # A
    "AARTIIND": "aarti-industries",
    "ABBOTINDIA": "abbott-india",
    "ABCAPITAL": "aditya-birla-capital",
    "ABFRL": "aditya-birla-fashion-and-retail",
    "ABB": "abb-india",
    "ACC": "acc",
    "ADANIENT": "adani-enterprises",
    "ADANIGREEN": "adani-green-energy",
    "ADANIPORTS": "adani-ports-and-special-economic-zone",
    "ADANIPOWER": "adani-power",
    "ADANITRANS": "adani-transmission",
    "AFFLE": "affle-india",
    "AIAENG": "aia-engineering",
    "AJANTPHARM": "ajanta-pharma",
    "ALKEM": "alkem-laboratories",
    "ALKYLAMINE": "alkyl-amines-chemicals",
    "AMARAJABAT": "amara-raja-batteries",
    "AMBUJACEM": "ambuja-cements",
    "ANGELONE": "angel-one",
    "APLAPOLLO": "apl-apollo-tubes",
    "APOLLOHOSP": "apollo-hospitals-enterprise",
    "APOLLOTYRE": "apollo-tyres",
    "ASHOKLEY": "ashok-leyland",
    "ASIANPAINT": "asian-paints",
    "ASTRAL": "astral",
    "ATUL": "atul",
    "AUBANK": "au-small-finance-bank",
    "AUROPHARMA": "aurobindo-pharma",
    "AVANTIFEED": "avanti-feeds",
    "AXISBANK": "axis-bank",
    # B
    "BAJAJ-AUTO": "bajaj-auto",
    "BAJAJFINSV": "bajaj-finserv",
    "BAJFINANCE": "bajaj-finance",
    "BALKRISIND": "balkrishna-industries",
    "BANDHANBNK": "bandhan-bank",
    "BANKBARODA": "bank-of-baroda",
    "BANKINDIA": "bank-of-india",
    "BATAINDIA": "bata-india",
    "BEL": "bharat-electronics",
    "BERGEPAINT": "berger-paints-india",
    "BHARATFORG": "bharat-forge",
    "BHARTIARTL": "bharti-airtel",
    "BHEL": "bharat-heavy-electricals",
    "BIOCON": "biocon",
    "BOSCHLTD": "bosch",
    "BPCL": "bharat-petroleum-corporation",
    "BRITANNIA": "britannia-industries",
    # C
    "CANBK": "canara-bank",
    "CASTROLIND": "castrol-india",
    "CDSL": "central-depository-services-india",
    "CESC": "cesc",
    "CGPOWER": "cg-power-and-industrial-solutions",
    "CHAMBLFERT": "chambal-fertilisers-and-chemicals",
    "CHOLAFIN": "cholamandalam-investment-and-finance-company",
    "CIPLA": "cipla",
    "COALINDIA": "coal-india",
    "COFORGE": "coforge",
    "COLPAL": "colgate-palmolive-india",
    "CONCOR": "container-corporation-of-india",
    "COROMANDEL": "coromandel-international",
    "CUMMINSIND": "cummins-india",
    # D
    "DABUR": "dabur-india",
    "DALBHARAT": "dalmia-bharat",
    "DEEPAKFERT": "deepak-fertilisers-and-petrochemicals",
    "DEEPAKNTR": "deepak-nitrite",
    "DELHIVERY": "delhivery",
    "DIVISLAB": "divi-s-laboratories",
    "DIXON": "dixon-technologies-india",
    "DLF": "dlf",
    "DRREDDY": "dr-reddys-laboratories",
    # E
    "EICHERMOT": "eicher-motors",
    "ELGIEQUIP": "elgi-equipments",
    "EMAMILTD": "emami",
    "ENGINERSIN": "engineers-india",
    "ESCORTS": "escorts-kubota",
    "EXIDEIND": "exide-industries",
    # F
    "FEDERALBNK": "federal-bank",
    "FINEORG": "fine-organic-industries",
    "FINPIPE": "finolex-industries",
    "FLUOROCHEM": "gujarat-fluorochemicals",
    "FORTIS": "fortis-healthcare",
    # G
    "GAIL": "gail-india",
    "GLENMARK": "glenmark-pharmaceuticals",
    "GMRINFRA": "gmr-airports-infrastructure",
    "GODREJCP": "godrej-consumer-products",
    "GODREJIND": "godrej-industries",
    "GODREJPROP": "godrej-properties",
    "GRANULES": "granules-india",
    "GRASIM": "grasim-industries",
    "GSPL": "gujarat-state-petronet",
    "GUJGASLTD": "gujarat-gas",
    # H
    "HAPPSTMNDS": "happiest-minds-technologies",
    "HCLTECH": "hcl-technologies",
    "HDFCAMC": "hdfc-asset-management-company",
    "HDFCBANK": "hdfc-bank",
    "HDFCLIFE": "hdfc-life-insurance-company",
    "HEROMOTOCO": "hero-motocorp",
    "HFCL": "hfcl",
    "HINDALCO": "hindalco-industries",
    "HINDCOPPER": "hindustan-copper",
    "HINDPETRO": "hindustan-petroleum-corporation",
    "HINDUNILVR": "hindustan-unilever",
    "HONAUT": "honeywell-automation-india",
    # I
    "ICICIBANK": "icici-bank",
    "ICICIGI": "icici-lombard-general-insurance-company",
    "ICICIPRULI": "icici-prudential-life-insurance-company",
    "IDFCFIRSTB": "idfc-first-bank",
    "IEX": "indian-energy-exchange",
    "IGL": "indraprastha-gas",
    "INDHOTEL": "indian-hotels-company",
    "INDIAMART": "indiamart-intermesh",
    "INDIGO": "interglobe-aviation",
    "INDUSINDBK": "indusind-bank",
    "INDUSTOWER": "indus-towers",
    "INFY": "infosys",
    "IOC": "indian-oil-corporation",
    "IPCALAB": "ipca-laboratories",
    "IRCTC": "indian-railway-catering-and-tourism-corporation",
    "IRFC": "indian-railway-finance-corporation",
    "ITC": "itc",
    # J
    "JINDALSTEL": "jindal-steel-and-power",
    "JKCEMENT": "jk-cement",
    "JSWENERGY": "jsw-energy",
    "JSWSTEEL": "jsw-steel",
    "JUBLFOOD": "jubilant-foodworks",
    # K
    "KAJARIACER": "kajaria-ceramics",
    "KANSAINER": "kansai-nerolac-paints",
    "KOTAKBANK": "kotak-mahindra-bank",
    "KPITTECH": "kpit-technologies",
    # L
    "L&TFH": "l-t-finance",
    "LAURUSLABS": "laurus-labs",
    "LICHSGFIN": "lic-housing-finance",
    "LICI": "life-insurance-corporation-of-india",
    "LINDEINDIA": "linde-india",
    "LT": "larsen-and-toubro",
    "LTIM": "ltimindtree",
    "LTTS": "l-t-technology-services",
    "LUPIN": "lupin",
    # M
    "M&M": "mahindra-and-mahindra",
    "M&MFIN": "mahindra-and-mahindra-financial-services",
    "MANAPPURAM": "manappuram-finance",
    "MARICO": "marico",
    "MARUTI": "maruti-suzuki-india",
    "MAXHEALTH": "max-healthcare-institute",
    "MCX": "multi-commodity-exchange-of-india",
    "METROPOLIS": "metropolis-healthcare",
    "MFSL": "max-financial-services",
    "MPHASIS": "mphasis",
    "MRF": "mrf",
    "MUTHOOTFIN": "muthoot-finance",
    # N
    "NATIONALUM": "national-aluminium-company",
    "NAUKRI": "info-edge-india",
    "NAVINFLUOR": "navin-fluorine-international",
    "NESTLEIND": "nestle-india",
    "NMDC": "nmdc",
    "NTPC": "ntpc",
    # O
    "OFSS": "oracle-financial-services-software",
    "OIL": "oil-india",
    "ONGC": "oil-and-natural-gas-corporation",
    # P
    "PAGEIND": "page-industries",
    "PATANJALI": "patanjali-foods",
    "PERSISTENT": "persistent-systems",
    "PETRONET": "petronet-lng",
    "PFIZER": "pfizer",
    "PHOENIXLTD": "phoenix-mills",
    "PIDILITIND": "pidilite-industries",
    "PIIND": "pi-industries",
    "PNB": "punjab-national-bank",
    "POLYCAB": "polycab-india",
    "POWERGRID": "power-grid-corporation-of-india",
    "PRESTIGE": "prestige-estates-projects",
    # R
    "RAIN": "rain-industries",
    "RAJESHEXPO": "rajesh-exports",
    "RAMCOCEM": "the-ramco-cements",
    "RECLTD": "rec",
    "RELIANCE": "reliance-industries",
    "RITES": "rites",
    # S
    "SAIL": "steel-authority-of-india",
    "SBICARD": "sbi-cards-and-payment-services",
    "SBILIFE": "sbi-life-insurance-company",
    "SBIN": "state-bank-of-india",
    "SCHAEFFLER": "schaeffler-india",
    "SHREECEM": "shree-cement",
    "SHRIRAMFIN": "shriram-finance",
    "SIEMENS": "siemens",
    "SOBHA": "sobha",
    "SOLARINDS": "solar-industries-india",
    "SONACOMS": "sona-blw-precision-forgings",
    "SUNPHARMA": "sun-pharmaceutical-industries",
    "SUNTV": "sun-tv-network",
    "SUPREMEIND": "supreme-industries",
    "SUZLON": "suzlon-energy",
    # T
    "TANLA": "tanla-platforms",
    "TATACHEM": "tata-chemicals",
    "TATACOMM": "tata-communications",
    "TATACONSUM": "tata-consumer-products",
    "TATAELXSI": "tata-elxsi",
    "TATAMOTORS": "tata-motors",
    "TATAPOWER": "tata-power-company",
    "TATASTEEL": "tata-steel",
    "TCS": "tata-consultancy-services",
    "TECHM": "tech-mahindra",
    "THERMAX": "thermax",
    "TITAN": "titan-company",
    "TORNTPHARM": "torrent-pharmaceuticals",
    "TORNTPOWER": "torrent-power",
    "TRENT": "trent",
    "TRIDENT": "trident",
    "TVSMOTOR": "tvs-motor-company",
    # U
    "UBL": "united-breweries",
    "ULTRACEMCO": "ultratech-cement",
    "UNIONBANK": "union-bank-of-india",
    "UNITDSPR": "united-spirits",
    # V
    "VBL": "varun-beverages",
    "VEDL": "vedanta",
    "VOLTAS": "voltas",
    # W
    "WHIRLPOOL": "whirlpool-of-india",
    "WIPRO": "wipro",
    # Z
    "ZOMATO": "zomato",
    "ZYDUSLIFE": "zydus-lifesciences",
}


def derive_slug(symbol: str) -> str | None:
    """
    Auto-derive a screener.in slug from an NSE symbol.
    Returns None when derivation is not reliable (symbol has digits, special chars, etc.).
    """
    # Strip common exchange suffixes
    s = symbol.upper().replace("-BE", "").replace("-BL", "").replace("-SM", "")
    # If it's all alphabetic with no unusual chars, lowercase is usually the slug
    if s.isalpha() and len(s) <= 10:
        return s.lower()
    return None


async def run(dry_run: bool = False):
    async with SessionLocal() as db:
        result = await db.execute(
            select(StockMaster.id, StockMaster.symbol, StockMaster.screener_symbol)
        )
        rows = result.fetchall()

    updated, skipped, already_set, no_mapping = 0, 0, 0, []

    async with SessionLocal() as db:
        for row_id, symbol, current_slug in rows:
            if current_slug:
                already_set += 1
                log.info(f"  SKIP  {symbol:<20} already set → {current_slug}")
                continue

            slug = KNOWN_MAP.get(symbol) or derive_slug(symbol)

            if not slug:
                no_mapping.append(symbol)
                skipped += 1
                log.warning(f"  MISS  {symbol:<20} no mapping found — set manually")
                continue

            log.info(f"  {'DRY ' if dry_run else 'SET '}  {symbol:<20} → {slug}")
            if not dry_run:
                await db.execute(
                    update(StockMaster)
                    .where(StockMaster.id == row_id)
                    .values(screener_symbol=slug)
                )
            updated += 1

        if not dry_run:
            await db.commit()

    print()
    print("─" * 60)
    print(f"  Total stocks   : {len(rows)}")
    print(f"  Already set    : {already_set}")
    print(f"  {'Would update' if dry_run else 'Updated'}     : {updated}")
    print(f"  No mapping     : {skipped}")
    if no_mapping:
        print(f"\n  Symbols needing manual slug:")
        for s in sorted(no_mapping):
            print(f"    {s}")
    print("─" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no DB writes")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))
