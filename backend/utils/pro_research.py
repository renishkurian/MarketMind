import urllib.request
import urllib.parse
import json
import logging
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

class ProResearchUtility:
    """
    Advanced intelligence gathering for the AI War Room.
    Simulates real-time search and financial data aggregation.
    """
    @staticmethod
    async def get_market_pulse(symbol: str, company_name: str = None) -> dict:
        """
        Fetches 'Pro' tier news and trending themes for a symbol.
        """
        try:
            # 1. Fetch News via RSS (existing logic refined)
            search_query = f"{symbol} {company_name}" if company_name else symbol
            query = urllib.parse.quote(f'{search_query} stock market trends India')
            url = f'https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en'
            
            # Using a custom User-Agent to avoid blocks
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'})
            
            # Use to_thread for blocking urllib
            content = await asyncio.to_thread(ProResearchUtility._fetch_url, req)
            
            # Simplistic regex to find headlines without full XML parser
            import re
            headlines = re.findall(r'<title>(.*?)</title>', content)
            
            # Filter out the first title which is the RSS channel title
            actual_news = headlines[1:10] if len(headlines) > 1 else []
            
            return {
                "headlines": actual_news,
                "scanned_at": datetime.utcnow().isoformat(),
                "status": "PRO_INTEL_READY"
            }
        except Exception as e:
            logger.error(f"Pro Intel Fetch Failed for {symbol}: {e}")
            return {"headlines": [], "status": "LIMITED_INTEL"}

    @staticmethod
    def _fetch_url(req):
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.read().decode('utf-8')
        except:
            return ""
