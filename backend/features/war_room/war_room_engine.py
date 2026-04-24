import pandas as pd
import numpy as np
from sqlalchemy import select
from backend.data.db import PriceHistory, StockMaster, FundamentalsCache, AICallLog, WarRoomSnapshot
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
            print(f"DEBUG: Starting War Room Research for {symbol}")
            # 1. Fetch Basic Data
            stmt = select(StockMaster).where(StockMaster.symbol == symbol)
            res = await self.db.execute(stmt)
            stock = res.scalars().first()
            if not stock:
                print(f"DEBUG: Symbol {symbol} not found in DB")
                return {"error": "Stock not found"}

            # 2. Get the 'Oracle' ML Score
            print(f"DEBUG: Running Oracle ML for {symbol}...")
            from backend.features.oracle.oracle_engine import OracleEngine
            oracle = OracleEngine(self.db)
            ml_analysis = await oracle.get_conviction_prediction(symbol)
            print(f"DEBUG: Oracle ML Score for {symbol}: {ml_analysis.get('conviction_score')}%")
            
            # 3. Fetch Latest News (Pro Intel)
            print(f"DEBUG: Fetching News Intel for {symbol}...")
            from backend.utils.pro_research import ProResearchUtility
            intel = await ProResearchUtility.get_market_pulse(symbol)
            news_headlines = intel.get('headlines', [])
            print(f"DEBUG: Fetched {len(news_headlines)} headlines for {symbol}")
            news_str = "\n".join([f"- {h}" for h in news_headlines]) if news_headlines else "No recent headlines found."
            
            # 4. Synthesize with LLM
            print(f"DEBUG: Synthesizing with AI for {symbol}...")
            
            f_raw = ml_analysis.get('fundamentals_raw') or {}
            f_str = f"""
            - ROE: {f_raw.get('roe', 'N/A')}%
            - D/E: {f_raw.get('debt_equity', 'N/A')}
            - PEG: {f_raw.get('peg_ratio', 'N/A')}
            - Margins: {f_raw.get('operating_margin', 'N/A')}%
            - P/E: {f_raw.get('pe_ratio', 'N/A')}
            """ if f_raw else "Fundamental data stream unavailable."

            prompt = f"""
            TRANSFORM INTO: Elite Indian Institutional Trader.
            SYMBOL: {symbol}
            ML SIGNAL: {ml_analysis.get('conviction_score', 50)}% Conviction | {ml_analysis.get('projected_30d_return', 0)}% target.
            
            FUNDAMENTAL METRICS:
            {f_str}
            
            BUFFETT INSIGHTS:
            {", ".join(ml_analysis.get('buffett_insights', ['Neutral visibility']))}
            
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
            
            intelligence = {}
            if isinstance(ai_raw, dict) and "pro_verdict" in ai_raw:
                print(f"DEBUG: AI returned structured JSON for {symbol}")
                intelligence = ai_raw
            else:
                content = ai_raw.get('reply', '') if isinstance(ai_raw, dict) else str(ai_raw)
                print(f"DEBUG: AI Response for {symbol} received (Length: {len(content)})")
                # Robust JSON extraction for string fallbacks
                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    try:
                        intelligence = json.loads(match.group())
                    except Exception as je:
                        logger.error(f"JSON Parse Error for {symbol}: {je}")
                        intelligence = {"pro_verdict": content[:500], "market_sentiment": "Analyzing..."}
                else:
                    intelligence = {"pro_verdict": content[:500], "market_sentiment": "Analyzing..."}

            # Ensure lists exist for UI mapping
            if "bull_case" not in intelligence or not isinstance(intelligence["bull_case"], list):
                intelligence["bull_case"] = ["Technical alpha confirmed"]
            if "bear_case" not in intelligence or not isinstance(intelligence["bear_case"], list):
                intelligence["bear_case"] = ["Market noise/volatility"]

            # 5. Build Result
            result = {
                "symbol": symbol,
                "ml_data": ml_analysis,
                "ai_intelligence": intelligence,
                "news_analyzed": news_headlines[:8],
                "generated_at": datetime.datetime.utcnow().isoformat()
            }

            # 6. Save Snapshot
            try:
                new_snap = WarRoomSnapshot(
                    user_id=user_id,
                    symbol=symbol,
                    intel_score=ml_analysis.get('conviction_score', 0),
                    snapshot_data=result
                )
                self.db.add(new_snap)
                await self.db.commit()
            except Exception as se:
                logger.error(f"Failed to save War Room Snapshot: {se}")

            return result
            
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
    async def get_latest_snapshot(self, symbol: str, user_id: int) -> Dict:
        """
        Retrieves the most recent research snapshot for this user/symbol.
        """
        stmt = (
            select(WarRoomSnapshot)
            .where(WarRoomSnapshot.user_id == user_id, WarRoomSnapshot.symbol == symbol)
            .order_by(WarRoomSnapshot.created_at.desc())
        )
        res = await self.db.execute(stmt)
        snap = res.scalars().first()
        if snap:
            data = snap.snapshot_data
            data["from_cache"] = True
            data["created_at"] = snap.created_at.isoformat()
            return data
        return None
