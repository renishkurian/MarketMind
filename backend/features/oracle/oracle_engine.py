import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sqlalchemy import select
from backend.data.db import PriceHistory, StockMaster, FundamentalsCache
import logging
from typing import Dict, List
import datetime

logger = logging.getLogger(__name__)

class OracleEngine:
    """
    The 'Indian Warren Buffett' Engine.
    Combines deep fundamental analysis with multi-year momentum tracking
    to predict high-alpha opportunities.
    """
    def __init__(self, db):
        self.db = db

    async def get_conviction_prediction(self, symbol: str) -> Dict:
        # 1. Fetch 10 years of price history
        stmt = (
            select(PriceHistory.close, PriceHistory.volume, PriceHistory.date)
            .where(PriceHistory.symbol == symbol)
            .order_by(PriceHistory.date.asc())
        )
        res = await self.db.execute(stmt)
        history = res.all()

        if len(history) < 200:
            return {"error": "Insufficient history for Oracle analysis"}

        df = pd.DataFrame(history, columns=['close', 'volume', 'date'])
        df['close'] = df['close'].astype(float)
        
        # 2. Fetch Fundamentals ( Buffett Style )
        f_stmt = select(FundamentalsCache).where(FundamentalsCache.symbol == symbol)
        f_res = await self.db.execute(f_stmt)
        fund = f_res.scalars().first()
        
        # 3. Feature Engineering
        # Technicals
        df['returns'] = df['close'].pct_change()
        df['sma_200'] = df['close'].rolling(200).mean()
        df['dist_sma_200'] = (df['close'] - df['sma_200']) / df['sma_200']
        df['volatility'] = df['returns'].rolling(30).std()
        
        # Target: Forward 30-day return
        df['target'] = df['close'].shift(-30) / df['close'] - 1
        
        train_df = df.dropna().tail(2500) # Last 10 years approx
        if train_df.empty:
            return {"error": "Training set empty"}

        X = train_df[['dist_sma_200', 'volatility']].values
        y = train_df['target'].values
        
        # 4. XGBoost Training (Oracle Model)
        model = XGBRegressor(n_estimators=100, max_depth=6, learning_rate=0.05, random_state=42)
        model.fit(X, y)
        
        # 5. Inference
        latest_X = df[['dist_sma_200', 'volatility']].tail(1).values
        prediction = model.predict(latest_X)[0]
        
        # 6. Fundamental Scoring (Quality Filter)
        quality_score = 50 # Default
        reasons = []
        
        if fund:
            if fund.roe and fund.roe > 20: 
                quality_score += 20
                reasons.append("High ROE (>20%) - Efficient capital user")
            if fund.debt_equity and fund.debt_equity < 0.5:
                quality_score += 15
                reasons.append("Low Debt-to-Equity - Strong balance sheet")
            if fund.peg_ratio and fund.peg_ratio < 1.2:
                quality_score += 15
                reasons.append("Attractive PEG Ratio - Growth at reasonable price")
            if fund.operating_margin and fund.operating_margin > 25:
                quality_score += 10
                reasons.append("Superior Operating Margins - Strong moat")
        else:
            reasons.append("Limited fundamental visibility")

        # Conviction = ML Prediction + Fundamental Quality
        conviction_raw = (prediction * 100) + (quality_score / 2)
        conviction = max(0, min(100, conviction_raw))

        return {
            "symbol": symbol,
            "conviction_score": round(float(conviction), 1),
            "projected_30d_return": round(float(prediction) * 100, 2),
            "quality_grade": "Strong" if quality_score > 70 else "Neutral",
            "buffett_insights": reasons,
            "analyzed_at": datetime.datetime.utcnow().isoformat()
        }
