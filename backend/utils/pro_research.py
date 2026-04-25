import urllib.request
import urllib.parse
import json
import logging
import asyncio
from datetime import datetime
import xml.etree.ElementTree as ET
import html

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
            
            root = ET.fromstring(content)
            actual_news = []
            for item in root.findall('.//item')[:9]:
                title_el = item.find('title')
                if title_el is not None and title_el.text:
                    actual_news.append(html.unescape(title_el.text))
            return {
                "headlines": actual_news,
                "fetch_failed": len(actual_news) == 0,
                "scanned_at": datetime.utcnow().isoformat(),
                "status": "PRO_INTEL_READY"
            }
        except Exception as e:
            logger.error(f"Pro Intel Fetch Failed for {symbol}: {e}")
            return {"headlines": [], "fetch_failed": True, "status": "LIMITED_INTEL"}

    @staticmethod
    def _fetch_url(req):
        for timeout in (5, 10):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.read().decode('utf-8')
            except Exception:
                continue
        return ""
