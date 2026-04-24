import pandas as pd
import numpy as np
from pypfopt import EfficientFrontier, risk_models, expected_returns
from sqlalchemy import select
from backend.data.db import PriceHistory, StockMaster
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class PortfolioOptEngine:
    def __init__(self, db):
        self.db = db

    async def optimize_portfolio(self, symbols: List[str]) -> Dict:
        """
        Calculates optimal weights for a given set of symbols using Max Sharpe Ratio optimization.
        """
        if not symbols:
            return {"error": "No symbols provided for optimization"}

        # 1. Fetch historical data (Pivot into a price matrix)
        stmt = (
            select(PriceHistory.date, PriceHistory.symbol, PriceHistory.close)
            .where(PriceHistory.symbol.in_(symbols))
            .order_by(PriceHistory.date.desc())
            .limit(2000) # Enough for many stocks
        )
        res = await self.db.execute(stmt)
        data = res.all()

        if not data:
            return {"error": "No historical data found for these symbols"}

        df = pd.DataFrame(data, columns=['date', 'symbol', 'close'])
        df['close'] = df['close'].astype(float)
        
        # Pivot: Dates as index, Symbols as columns
        prices_df = df.pivot(index='date', columns='symbol', values='close').sort_index()
        prices_df = prices_df.dropna(axis=1, thresh=len(prices_df) * 0.8) # Keep columns with 80% data
        prices_df = prices_df.ffill().dropna()

        if prices_df.empty or len(prices_df.columns) < 2:
            return {"error": "Insufficient history or too few stocks for optimization"}

        # 2. PyPortfolioOpt Magic
        try:
            # Expected Returns and Sample Covariance
            mu = expected_returns.mean_historical_return(prices_df)
            S = risk_models.sample_cov(prices_df)

            # Efficient Frontier
            ef = EfficientFrontier(mu, S)
            weights = ef.max_sharpe()
            cleaned_weights = ef.clean_weights()
            
            # Performance metrics
            perf = ef.portfolio_performance(verbose=False)
            
            return {
                "weights": cleaned_weights,
                "metrics": {
                    "expected_annual_return": round(perf[0] * 100, 2),
                    "annual_volatility": round(perf[1] * 100, 2),
                    "sharpe_ratio": round(perf[2], 2)
                },
                "symbols_analyzed": list(prices_df.columns)
            }
        except Exception as e:
            logger.error(f"Optimization Error: {e}")
            return {"error": f"Mathematical optimization failed: {str(e)}"}
