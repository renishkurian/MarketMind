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
        import asyncio
        logger.info(f"ML Alpha: Starting prediction for {symbol}")
        
        # 1. Fetch History
        stmt = (
            select(PriceHistory)
            .where(PriceHistory.symbol == symbol)  # Use symbol as it's more reliable
            .order_by(PriceHistory.date.desc())
            .limit(500)
        )
        res = await self.db.execute(stmt)
        history = res.scalars().all()
        
        if len(history) < 60:
            logger.warning(f"ML Alpha: Insufficient history for {symbol} ({len(history)} bars)")
            return {"error": "Insufficient history (min 60 bars)"}

        # 2. Build DataFrame
        df = pd.DataFrame([
            {"close": float(h.close), "volume": int(h.volume or 0)} 
            for h in reversed(history)
        ])

        # 3. Use ThreadPool for ML to avoid blocking event loop
        return await asyncio.to_thread(self._train_and_predict, symbol, df)

    def _train_and_predict(self, symbol: str, df: pd.DataFrame) -> Dict:
        try:
            # Feature Engineering
            df['sma_20'] = df['close'].rolling(window=20).mean()
            df['sma_50'] = df['close'].rolling(window=50).mean()
            df['dist_sma_20'] = (df['close'] - df['sma_20']) / df['sma_20'].replace(0, np.nan)
            df['dist_sma_50'] = (df['close'] - df['sma_50']) / df['sma_50'].replace(0, np.nan)
            df['volatility'] = df['close'].rolling(window=20).std() / df['sma_20'].replace(0, np.nan)
            
            # Target: 5-day forward return
            df['target'] = df['close'].shift(-5) / df['close'] - 1
            
            df = df.dropna()
            if len(df) < 30:
                return {"error": "Insufficient clean data samples"}

            features = ['dist_sma_20', 'dist_sma_50', 'volatility']
            X = df[features].values[:-5]
            y = df['target'].values[:-5]
            
            model = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
            model.fit(X, y)

            latest_features = df[features].values[-1].reshape(1, -1)
            prediction = model.predict(latest_features)[0]

            # Better confidence: Std Dev of tree predictions (normalized)
            tree_preds = [tree.predict(latest_features)[0] for tree in model.estimators_]
            std_dev = np.std(tree_preds)
            confidence = 1.0 / (1.0 + std_dev * 10) # Simple inverse mapping
            
            return {
                "symbol": symbol,
                "prediction_5d_return": round(float(prediction) * 100, 2),
                "confidence_score": round(float(confidence), 2),
                "model_type": "RandomForestRegressor",
                "features_used": features,
                "train_size": len(X),
                "last_price": round(float(df['close'].iloc[-1]), 2),
                "projected_price": round(float(df['close'].iloc[-1] * (1 + prediction)), 2)
            }
        except Exception as e:
            logger.error(f"ML Error for {symbol}: {e}")
            return {"error": f"Model error: {str(e)}"}
