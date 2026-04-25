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

    async def get_alpha_prediction(self, symbol: str) -> Dict:
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
            {
                "date": pd.to_datetime(h.date),
                "open": float(h.open) if h.open is not None else float(h.close),
                "high": float(h.high) if h.high is not None else float(h.close),
                "low": float(h.low) if h.low is not None else float(h.close),
                "close": float(h.close),
                "volume": int(h.volume or 0)
            } 
            for h in reversed(history)
        ])
        df.set_index('date', inplace=True)

        # 3. Use ThreadPool for ML to avoid blocking event loop
        return await asyncio.to_thread(self._train_and_predict, symbol, df)

    def _train_and_predict(self, symbol: str, df: pd.DataFrame) -> Dict:
        try:
            import pandas_ta as ta
            
            # Use pandas-ta for bulk feature engineering
            df.ta.ema(length=20, append=True)
            df.ta.ema(length=50, append=True)
            
            df['dist_ema_20'] = (df['close'] - df['EMA_20']) / df['EMA_20'].replace(0, np.nan)
            df['dist_ema_50'] = (df['close'] - df['EMA_50']) / df['EMA_50'].replace(0, np.nan)
            
            df.ta.rsi(length=14, append=True)
            df['rsi_norm'] = (df['RSI_14'] - 50) / 50
            
            df.ta.macd(append=True)
            df.ta.bbands(append=True)
            df.ta.adx(append=True)
            df.ta.atr(length=14, append=True)
            df.ta.supertrend(append=True)
            
            # Target: 5-day forward return
            df['target'] = df['close'].shift(-5) / df['close'] - 1
            
            features = [
                'dist_ema_20', 'dist_ema_50', 
                'rsi_norm', 'vol_ratio', 'momentum_3d', 
                'MACD_12_26_9', 'ADX_14', 'ATRr_14', 'SUPERT_7_3.0'
            ]
            
            # Fallback zeros incase TA drops columns due to short history
            for col in features:
                if col not in df.columns:
                    df[col] = 0.0

            df['vol_ratio'] = (df['volume'] / df['volume'].rolling(20).mean().replace(0, 1)).fillna(1).clip(0, 5)
            df['momentum_3d'] = df['close'].pct_change(3).fillna(0)
            
            # Forward-fill feature NaNs, then fill 0
            df[features] = df[features].ffill().fillna(0)
            
            # Extract latest BEFORE dropping target NaNs!
            latest_features = df[features].iloc[-1].values.reshape(1, -1)
            
            # Train DF drops rows where target is NaN (the last 5 days)
            train_df = df.dropna(subset=['target'])
            
            if len(train_df) < 30:
                return {"error": "Insufficient clean data samples"}

            X = train_df[features].values
            y = train_df['target'].values
            
            model = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
            model.fit(X, y)
            
            prediction = model.predict(latest_features)[0]

            # Better confidence: Std Dev of tree predictions (normalized)
            tree_preds = [tree.predict(latest_features)[0] for tree in model.estimators_]
            std_dev = np.std(tree_preds)
            confidence = float(np.clip(1.0 - (std_dev / (std_dev + 0.01)), 0.0, 1.0))
            
            # SHAP Explainability
            feature_impact = {}
            try:
                import shap
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(latest_features)
                # Map SHAP values to their feature names
                feature_impact = {features[i]: round(float(shap_values[0][i]), 4) for i in range(len(features))}
            except Exception as e:
                logger.warning(f"SHAP Error for {symbol}: {e}")

            return {
                "symbol": symbol,
                "prediction_5d_return": round(float(prediction) * 100, 2),
                "confidence_score": round(float(confidence), 2),
                "model_type": "RandomForestRegressor",
                "features_used": features,
                "feature_impact": feature_impact,
                "train_size": len(X),
                "last_price": round(float(df['close'].iloc[-1]), 2),
                "projected_price": round(float(df['close'].iloc[-1] * (1 + prediction)), 2)
            }
        except Exception as e:
            logger.error(f"ML Error for {symbol}: {e}")
            return {"error": f"Model error: {str(e)}"}
