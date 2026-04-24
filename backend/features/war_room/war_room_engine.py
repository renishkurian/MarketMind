import pandas as pd
import numpy as np
from sqlalchemy import select
from backend.data.db import PriceHistory, StockMaster, FundamentalsCache, AICallLog
from backend.engine import ai_engine
import logging
from typing import Dict, List
import datetime
import asyncio
import re
import json

logger = logging.getLogger(__name__)

class WarRoomEngine:
    """
    The 'Super Feature' - Market Intelligence War Room.
    Combines Technical ML, Fundamental Quality, and Generative AI Sentiment
    to provide an Institutional-grade Verdict.
    """
    def __init__(self, db):
        self.db = db

    async def get_deep_research(self, symbol: str, user_id: int = None) -> Dict:
        """
        Runs a Multi-Model Deep Research on a stock combining ML and Real-time AI reasoning.
        """
        try:
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
            
            # 3. Fetch Latest News (Pro Intel)
            from backend.utils.pro_research import ProResearchUtility
            intel = await ProResearchUtility.get_market_pulse(symbol)
            news_headlines = intel.get('headlines', [])
            news_str = "\n".join([f"- {h}" for h in news_headlines]) if news_headlines else "No recent headlines found."
            
            # 4. Synthesize with LLM
            prompt = f"""
            TRANSFORM INTO: Elite Indian Institutional Trader.
            SYMBOL: {symbol}
            ML SIGNAL: {ml_analysis.get('conviction_score', 50)}% Conviction | {ml_analysis.get('projected_30d_return', 0)}% target.
            FUNDAMENTALS: {", ".join(ml_analysis.get('buffett_insights', ['No fundamental anomaliesDetected']))}
            RECENT NEWS HEADLINES:
            {news_str}
            
            MISSION:
            Produce a sharp institutional synthesis. 
            Does current news validate the ML conviction or suggest a pivot?
            
            FORMAT (STRICT JSON ONLY - NO PREAMBLE):
            {{
              "market_sentiment": "Hyper-Bullish | Cautious | Bearish | Neutral",
              "bull_case": ["Point 1", "Point 2"],
              "bear_case": ["Point 1", "Point 2"],
              "pro_verdict": "One-line pro decision",
              "institutional_action": "BUY_HEAVY | TRIM | HOLD | AVOID"
            }}
            """
            
            ai_raw = await ai_engine.generate_pro_research(
                symbol=symbol,
                trigger_reason="WAR_ROOM_DEEP_RESEARCH",
                messages=[{"role": "user", "content": prompt}],
                system_prompt="You are a Pro-Tier Quant+Fundamental Synthesizer. Return ONLY JSON.",
                user_id=user_id
            )
            
            content = ai_raw.get('reply', '')
            intelligence = {}
            
            # Robust JSON extraction
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                try:
                    intelligence = json.loads(match.group())
                except Exception as je:
                    logger.error(f"JSON Parse Error for {symbol}: {je}")
                    intelligence = {"pro_verdict": content[:250], "market_sentiment": "Analyzing..."}
            else:
                intelligence = {"pro_verdict": content[:250], "market_sentiment": "Analyzing..."}

            # Ensure lists exist for UI mapping
            if "bull_case" not in intelligence or not isinstance(intelligence["bull_case"], list):
                intelligence["bull_case"] = ["Technical alpha confirmed"]
            if "bear_case" not in intelligence or not isinstance(intelligence["bear_case"], list):
                intelligence["bear_case"] = ["Market noise/volatility"]

            return {
                "symbol": symbol,
                "ml_data": ml_analysis,
                "ai_intelligence": intelligence,
                "news_analyzed": news_headlines[:8],
                "generated_at": datetime.datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            import traceback
            err_msg = traceback.format_exc()
            logger.error(f"War Room Intelligence Error for {symbol}: {err_msg}")
            return {
                "symbol": symbol,
                "ml_data": {"conviction_score": 0, "projected_30d_return": 0},
                "ai_intelligence": {
                    "pro_verdict": f"CRITICAL: {str(e)}",
                    "bull_case": ["ML signals offline"],
                    "bear_case": ["Incomplete data stream"],
                    "market_sentiment": "ERROR"
                },
                "generated_at": datetime.datetime.utcnow().isoformat()
            }
