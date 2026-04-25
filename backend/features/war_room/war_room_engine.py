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
            logger.debug(f"Starting War Room Research for {symbol}")
            # 1. Fetch Basic Data
            stmt = select(StockMaster).where(StockMaster.symbol == symbol)
            res = await self.db.execute(stmt)
            stock = res.scalars().first()
            if not stock:
                logger.debug(f"Symbol {symbol} not found in DB")
                return {"error": "Stock not found"}

            # 2. Get the 'Oracle' ML Score
            logger.debug(f"Running Oracle ML for {symbol}...")
            from backend.features.oracle.oracle_engine import OracleEngine
            oracle = OracleEngine(self.db)
            ml_analysis = await oracle.get_conviction_prediction(symbol, user_id)
            if "error" in ml_analysis:
                return ml_analysis
            logger.debug(f"Oracle ML Score for {symbol}: {ml_analysis.get('conviction_score')}%")
            
            # 3. Fetch Latest News (Pro Intel)
            company_name = stock.company_name if stock else symbol
            logger.debug(f"Fetching News Intel for {company_name} ({symbol})...")
            from backend.utils.pro_research import ProResearchUtility
            intel = await ProResearchUtility.get_market_pulse(symbol, company_name=company_name)
            news_headlines = intel.get('headlines', [])
            logger.debug(f"Fetched {len(news_headlines)} headlines for {symbol}")
            news_str = "\n".join([f"- {h}" for h in news_headlines]) if news_headlines else "No recent headlines found."
            
            # 4. Synthesize with LLM
            logger.debug(f"Synthesizing with AI for {symbol}...")
            
            from backend.data.db import SignalsCache
            sig_res = await self.db.execute(select(SignalsCache).where(SignalsCache.symbol == symbol))
            sig = sig_res.scalars().first()
            
            f_raw = ml_analysis.get('fundamentals_raw') or {}
            f_str = f"""
            - ROE: {f_raw.get('roe') if f_raw.get('roe') is not None else 'N/A'}%
            - D/E: {f_raw.get('debt_equity') if f_raw.get('debt_equity') is not None else 'N/A'}
            - PEG: {f_raw.get('peg_ratio') if f_raw.get('peg_ratio') is not None else 'N/A'}
            - Margins: {f_raw.get('operating_margin') if f_raw.get('operating_margin') is not None else 'N/A'}%
            - P/E: {f_raw.get('pe_ratio') if f_raw.get('pe_ratio') is not None else 'N/A'}
            """ if f_raw else "Fundamental data stream unavailable."

            prompt = f"""
            TRANSFORM INTO: Elite Indian Institutional Trader.
            COMPANY: {company_name}
            SYMBOL: {symbol}
            ML SIGNAL: {ml_analysis.get('conviction_score', 50)}% Conviction | {ml_analysis.get('projected_30d_return', 0)}% target.
            
            FUNDAMENTAL METRICS:
            {f_str}
            
            MARKETMIND SIGNAL INTELLIGENCE:
            - Composite Score: {sig.composite_score if sig else 'N/A'}/100
            - Sector Percentile: {sig.sector_percentile if sig else 'N/A'}%
            - Short-Term Signal: {sig.st_signal if sig else 'N/A'}
            - Long-Term Signal: {sig.lt_signal if sig else 'N/A'}

            
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
            # Normalize: handle both direct dict (all providers) and error wrapper {'reply': '...'}
            if isinstance(ai_raw, dict) and "pro_verdict" in ai_raw:
                intelligence = ai_raw
            elif isinstance(ai_raw, dict) and "reply" in ai_raw:
                # Error wrapper from generate_pro_research exception handler
                raw_text = ai_raw["reply"]
                import re
                import json
                match = re.search(r'\{.*\}', raw_text, re.DOTALL)
                if match:
                    try:
                        intelligence = json.loads(match.group())
                    except Exception:
                        pass
                if not intelligence.get("pro_verdict"):
                    intelligence = {
                        "pro_verdict": "Analysis temporarily unavailable. Please rebuild.",
                        "market_sentiment": "Unavailable",
                        "bull_case": ["Data pipeline error — please retry"],
                        "bear_case": ["Could not complete synthesis"],
                        "institutional_action": "HOLD"
                    }
            else:
                intelligence = {"pro_verdict": "Unexpected response format.", "market_sentiment": "Error"}

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
            from datetime import timedelta
            import datetime
            age = datetime.datetime.utcnow() - snap.created_at
            
            if age > timedelta(hours=24):
                snap_dict = snap.snapshot_data.copy()
                snap_dict["from_cache"] = True
                snap_dict["created_at"] = snap.created_at.isoformat()
                snap_dict["cache_stale"] = True   # new field
                return snap_dict
                
            data = snap.snapshot_data
            data["from_cache"] = True
            data["created_at"] = snap.created_at.isoformat()
            data["cache_stale"] = False
            return data
        return None
