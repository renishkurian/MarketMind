import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from datetime import date, timedelta
from typing import List, Dict, Optional
import logging

from backend.data.db import PriceHistory
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class AlphaDiscoveryEngine:
    """
    ML-based Alpha Discovery.
    Predicts the 5-day forward return using Random Forest on technical features.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_alpha_prediction(self, symbol: str, isin: str) -> Dict:
        """
        Fetches history, builds features, trains a quick model, and returns prediction.
        """
        # 1. Fetch History
        stmt = (
            select(PriceHistory)
            .where(PriceHistory.isin == isin)
            .order_by(PriceHistory.date.desc())
            .limit(500) # Last 2 years approx
        )
        res = await self.db.execute(stmt)
        history = res.scalars().all()
        
        if len(history) < 100:
            return {"error": "Insufficient history for ML (min 100 bars)"}

        # 2. Build DataFrame
        df = pd.DataFrame([
            {"date": h.date, "close": float(h.close), "volume": int(h.volume or 0)} 
            for h in reversed(history)
        ])

        # 3. Feature Engineering
        # Moving Averages
        df['sma_20'] = df['close'].rolling(window=20).mean()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['dist_sma_20'] = (df['close'] - df['sma_20']) / df['sma_20']
        df['dist_sma_50'] = (df['close'] - df['sma_50']) / df['sma_50']
        
        # Volatility
        df['volatility'] = df['close'].rolling(window=20).std() / df['sma_20']
        
        # Target: 5-day forward return
        df['target'] = df['close'].shift(-5) / df['close'] - 1
        
        # Clean up
        df = df.dropna()
        if df.empty:
            return {"error": "Cleanup resulted in empty dataset"}

        # 4. Train Model
        features = ['dist_sma_20', 'dist_sma_50', 'volatility']
        X = df[features].values[:-5] # Hide last 5 bars they don't have targets
        y = df['target'].values[:-5]
        
        if len(X) < 50:
            return {"error": "Insufficient training samples"}

        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X, y)

        # 5. Predict for TODAY
        latest_features = df[features].values[-1].reshape(1, -1)
        prediction = model.predict(latest_features)[0]

        # 6. Interpret result
        confidence = float(np.mean([tree.predict(latest_features)[0] for tree in model.estimators_]) / prediction) if prediction != 0 else 0
        
        return {
            "symbol": symbol,
            "prediction_5d_return": round(prediction * 100, 2),
            "confidence_score": round(min(1.0, abs(confidence)), 2),
            "model_type": "RandomForestRegressor",
            "features_used": features,
            "train_size": len(X),
            "last_price": df['close'].iloc[-1],
            "projected_price": round(df['close'].iloc[-1] * (1 + prediction), 2)
        }
