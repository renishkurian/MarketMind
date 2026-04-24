import pandas as pd
import numpy as np
from sqlalchemy import select
from backend.data.db import PriceHistory, StockMaster, FundamentalsCache, AICallLog
from backend.engine import ai_engine
import logging
from typing import Dict, List
import datetime
import asyncio

logger = logging.getLogger(__name__)

class WarRoomEngine:
    """
    The 'Super Feature' - Market Intelligence War Room.
    Combines Technical ML, Fundamental Quality, and Generative AI Sentiment
    to provide an Institutional-grade Verdict.
    """
    def __init__(self, db):
        self.db = db

    async def get_deep_research(self, symbol: str) -> Dict:
        """
        Runs a Multi-Model Deep Research on a stock combining ML and Real-time AI reasoning.
        """
        # 1. Fetch Basic Data
        stmt = select(StockMaster).where(StockMaster.symbol == symbol)
        res = await self.db.execute(stmt)
        stock = res.scalars().first()
        if not stock:
            return {"error": "Stock not found"}

        # 2. Get the 'Oracle' ML Score
        from backend.features.oracle.oracle_engine import OracleEngine
        oracle = OracleEngine(self.db)
        ml_analysis = await oracle.get_conviction_prediction(symbol)
        
        # 3. Fetch Latest News (Real-time AI Search Capabilities)
        news_headlines = await ai_engine._fetch_symbol_news(symbol)
        news_str = "\n".join([f"- {h}" for h in news_headlines]) if news_headlines else "No recent headlines found."
        
        # 4. Synthesize with LLM
        prompt = f"""
        TRANSFORM INTO: Elite Indian Institutional Trader (Buffett-style discipline + Modern HFT speed).
        SYMBOL: {symbol}
        ML SIGNAL: {ml_analysis.get('conviction_score')}% Conviction | {ml_analysis.get('projected_30d_return')}% target.
        FUNDAMENTALS: {", ".join(ml_analysis.get('buffett_insights', []))}
        RECENT NEWS HEADLINES:
        {news_str}
        
        MISSION:
        Synthesize the Technical-Fundamental-Sentimental data. 
        Detect if the News confirms the ML trend or contradicts it (e.g. ML is bullish but News shows a regulatory crisis).
        
        FORMAT (STRICT JSON):
        {{
          "market_sentiment": "Hyper-Bullish/Cautious/Bearish/Neutral",
          "bull_case": ["Strategic point 1", "Strategic point 2"],
          "bear_case": ["Risk factor 1", "Risk factor 2"],
          "pro_verdict": "One-line internal fund verdict",
          "institutional_action": "BUY_HEAVY / TRIM / HOLD / AVOID"
        }}
        """
        
        try:
            ai_raw = await ai_engine.generate_insight(
                symbol=symbol,
                trigger_reason="WAR_ROOM_DEEP_RESEARCH",
                messages=[{"role": "user", "content": prompt}],
                system_prompt="You are a Pro-Tier Quant+Fundamental Synthesizer. Speak like a multi-million dollar fund manager."
            )
            
            # The generate_insight returns a dict with 'reply'. We need it to be JSON.
            # But the backend engine might return a full object. 
            # I'll try to extract JSON from the AI reply.
            
            import re
            content = ai_raw.get('reply', '')
            match = re.search(r'\{.*\}', content, re.DOTALL)
            intelligence = {}
            if match:
                try:
                    import json
                    intelligence = json.loads(match.group())
                except:
                    intelligence = {"pro_verdict": content[:200]}
            else:
                intelligence = {"pro_verdict": content[:200]}

            return {
                "symbol": symbol,
                "ml_data": ml_analysis,
                "ai_intelligence": intelligence,
                "news_analyzed": news_headlines[:5],
                "generated_at": datetime.datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"War Room Intelligence Error: {e}")
            return {
                "symbol": symbol,
                "ml_data": ml_analysis,
                "ai_intelligence": {"pro_verdict": "Neural network desynchronized. Fallback to ML Signal."},
                "generated_at": datetime.datetime.utcnow().isoformat()
            }
