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
            from pypfopt import objective_functions
            # Expected Returns (CAPM is more stable than mean historical)
            mu = expected_returns.capm_return(prices_df)
            S = risk_models.CovarianceShrinkage(prices_df).ledoit_wolf()

            # Efficient Frontier with dynamic weight constraints
            # For 61 assets, a 2% floor is impossible (61*2% > 100%).
            # We use a safe floor of (1/N) * 0.5 to ensure feasibility.
            num_assets = len(prices_df.columns)
            min_weight = 1.0 / (num_assets * 2) if num_assets > 0 else 0
            max_weight = max(0.1, 2.0 / num_assets) if num_assets > 0 else 0.25
            max_weight = min(max_weight, 0.40) # Never more than 40%
            
            ef = EfficientFrontier(mu, S, weight_bounds=(min_weight, max_weight))
            
            # Add L2 Regularization to favor many non-zero weights
            # Gamma of 0.1 is strong enough to spread weights
            ef.add_objective(objective_functions.L2_reg, gamma=0.1)
            
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
