import pandas as pd
import numpy as np
from pypfopt import EfficientFrontier, risk_models, expected_returns
from sqlalchemy import select
from backend.data.db import PriceHistory, StockMaster
import logging
from datetime import date, timedelta
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
            .where(
                PriceHistory.symbol.in_(symbols),
                PriceHistory.date >= (date.today() - timedelta(days=1095))
            )
            .order_by(PriceHistory.date.desc())
        )
        res = await self.db.execute(stmt)
        data = res.all()

        if not data:
            return {"error": "No historical data found for these symbols"}

        df = pd.DataFrame(data, columns=['date', 'symbol', 'close'])
        df['close'] = df['close'].astype(float)
        
        # Pivot: Dates as index, Symbols as columns
        prices_df = df.pivot(index='date', columns='symbol', values='close').sort_index()
        prices_df = prices_df.dropna(axis=1, thresh=len(prices_df) * 0.5) # Keep columns with 50% data
        prices_df = prices_df.ffill().dropna()

        if prices_df.empty or len(prices_df.columns) < 2:
            return {"error": "Insufficient history or too few stocks for optimization"}

        # 1.5 Machine Learning Regime & Mean-Reversion Detection
        current_state = "Unknown"
        mean_reverting_symbols = []
        try:
            # A. HMM Regime Detection
            from hmmlearn.hmm import GaussianHMM
            # Use average portfolio return to estimate overall regime
            mean_returns = prices_df.pct_change().mean(axis=1).dropna().values.reshape(-1, 1)
            if len(mean_returns) > 100:
                hmm_model = GaussianHMM(n_components=3, covariance_type="full", n_iter=100, random_state=42)
                hmm_model.fit(mean_returns)
                regime = hmm_model.predict(mean_returns)[-1]
                
                means = hmm_model.means_.flatten()
                sorted_idx = np.argsort(means) # Low = Bear, Mid = Sideways, High = Bull
                state_mapping = {sorted_idx[0]: "Bear", sorted_idx[1]: "Sideways", sorted_idx[2]: "Bull"}
                current_state = state_mapping[regime]

            # B. ADF Mean Reversion Testing
            from statsmodels.tsa.stattools import adfuller
            for symbol in prices_df.columns:
                ret = prices_df[symbol].pct_change().dropna()
                if len(ret) > 50:
                    result = adfuller(ret)
                    # p-value < 5% means it is stationary (mean reverting)
                    if result[1] < 0.05:
                        mean_reverting_symbols.append(symbol)
        except Exception as e:
            logger.warning(f"ML Regime/Reversion detection failed: {e}")

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
            
            # Risk-averse allocation based on regime
            if current_state == "Bear":
                logger.info("Bear regime detected. Switching to Defensive Minimum Volatility Optimization.")
                weights = ef.min_volatility()
            else:
                # Use a 0% risk-free rate to ensure the optimization problem is always solvable 
                # even if market returns have been poor recently.
                try:
                    weights = ef.max_sharpe(risk_free_rate=0.00)
                except ValueError:
                    logger.warning("Max Sharpe failed, falling back to Min Volatility")
                    weights = ef.min_volatility()

            cleaned_weights = ef.clean_weights()
            
            # Performance metrics
            perf = ef.portfolio_performance(verbose=False, risk_free_rate=0.00)
            
            return {
                "weights": cleaned_weights,
                "metrics": {
                    "expected_annual_return": float(round(perf[0] * 100, 2)),
                    "annual_volatility": float(round(perf[1] * 100, 2)),
                    "sharpe_ratio": float(round(perf[2], 2))
                },
                "ml_insights": {
                    "market_regime": current_state,
                    "mean_reverting_assets": mean_reverting_symbols
                },
                "symbols_analyzed": list(prices_df.columns)
            }
        except Exception as e:
            logger.error(f"Optimization Error: {e}")
            return {"error": f"Mathematical optimization failed: {str(e)}"}
