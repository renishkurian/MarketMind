import numpy as np
import pandas as pd
import logging
from pypfopt import (
    EfficientFrontier,
    EfficientCVaR,
    EfficientCDaR,
    EfficientSemivariance,
    risk_models,
    expected_returns,
    objective_functions,
    BlackLittermanModel,
    HRPOpt,
    DiscreteAllocation,
    CovarianceShrinkage,
)
from pypfopt import black_litterman

logger = logging.getLogger(__name__)

# Minimum stocks required for various mathematical models to be stable
STRATEGY_MIN_STOCKS = {
    "AI_PULSE": 1,
    "HRP": 3,
    "MVO": 3,
    "BLACK_LITTERMAN": 3,
    "ERC": 2,
    "CVAR": 3,
    "CDAR": 3,
    "SEMIVARIANCE": 3,
}


def _get_robust_cov(returns_df: pd.DataFrame) -> pd.DataFrame:
    """
    Auto-selects the best covariance estimator based on portfolio size.
    - ≤10 stocks: Ledoit-Wolf shrinkage (stable on small NSE portfolios)
    - >10 stocks: Oracle Approximating Shrinkage (more accurate for larger matrices)
    Replaces noisy sample_cov which is ill-conditioned on small portfolios.
    """
    n = len(returns_df.columns)
    shrink = CovarianceShrinkage(returns_df)
    if n <= 10:
        return shrink.ledoit_wolf()
    else:
        return shrink.oracle_approximating()


def calculate_allocation(
    strategy: str,
    amount: float,
    returns_df: pd.DataFrame,
    ai_scores: dict = None,
    current_prices: dict = None,
    market_caps: dict = None,
    sector_map: dict = None,
    prev_weights: dict = None,
    nifty_prices: pd.Series = None,
    target_volatility: float = None,
    target_return: float = None,
) -> dict:
    """
    Main entry point for specialized portfolio allocation math.
    strategies: AI_PULSE, HRP, MVO, BLACK_LITTERMAN, ERC, CVAR, CDAR, SEMIVARIANCE,
                EFFICIENT_RISK, EFFICIENT_RETURN

    Args:
        strategy: Allocation strategy name
        amount: Total capital in INR
        returns_df: Daily returns DataFrame (rows=dates, cols=symbols)
        ai_scores: Dict of {symbol: composite_score}
        current_prices: Dict of {symbol: price}
        market_caps: Dict of {symbol: market_cap_in_INR}  — for BL prior
        sector_map: Dict of {symbol: sector_name}  — for sector constraints
        prev_weights: Dict of {symbol: weight 0-1}  — for transaction cost penalty
        nifty_prices: pd.Series of Nifty50 prices  — for CAPM-based BL prior
        target_volatility: float (0-1)  — for EFFICIENT_RISK strategy
        target_return: float (0-1)      — for EFFICIENT_RETURN strategy
    """
    strategy = strategy.upper()

    # Derive symbol list
    if not returns_df.empty:
        symbols = returns_df.columns.tolist()
    else:
        symbols = list(ai_scores.keys()) if ai_scores else []

    n_stocks = len(symbols)

    if n_stocks < STRATEGY_MIN_STOCKS.get(strategy, 1):
        if strategy != "AI_PULSE":
            logger.warning(
                f"Portfolio too small for {strategy} ({n_stocks} stocks). Falling back to AI_PULSE."
            )
        strategy = "AI_PULSE"

    try:
        # ── Pre-process: Scrub returns_df ────────────────────────────────────
        if not returns_df.empty:
            min_obs = 10
            valid_cols = returns_df.columns[returns_df.notna().sum() >= min_obs]
            dropped = set(returns_df.columns) - set(valid_cols)
            if dropped:
                logger.warning(
                    f"Dropping symbols with insufficient history for {strategy}: {dropped}"
                )
                returns_df = returns_df[valid_cols]

            returns_df = returns_df.replace([np.inf, -np.inf], np.nan).fillna(0)

            if len(returns_df.columns) < STRATEGY_MIN_STOCKS.get(strategy, 1):
                logger.warning(
                    f"Remaining stocks ({len(returns_df.columns)}) insufficient for {strategy}. Falling back to AI_PULSE."
                )
                strategy = "AI_PULSE"

        ai_scores = ai_scores or {}
        current_prices = current_prices or {}
        market_caps = market_caps or {}
        sector_map = sector_map or {}
        prev_weights = prev_weights or {}

        if strategy == "HRP":
            return _execute_hrp(amount, returns_df, current_prices, prev_weights)
        elif strategy == "MVO":
            return _execute_mvo(
                amount, returns_df, current_prices, sector_map, prev_weights
            )
        elif strategy == "BLACK_LITTERMAN":
            return _execute_black_litterman(
                amount, returns_df, ai_scores, current_prices, market_caps, sector_map,
                prev_weights, nifty_prices
            )
        elif strategy == "ERC":
            return _execute_erc(amount, returns_df, current_prices, prev_weights)
        elif strategy == "CVAR":
            return _execute_cvar(amount, returns_df, current_prices, prev_weights)
        elif strategy == "CDAR":
            return _execute_cdar(amount, returns_df, current_prices, prev_weights)
        elif strategy == "SEMIVARIANCE":
            return _execute_semivariance(amount, returns_df, current_prices, prev_weights)
        elif strategy == "EFFICIENT_RISK":
            return _execute_efficient_risk(
                amount, returns_df, current_prices, sector_map, prev_weights, target_volatility
            )
        elif strategy == "EFFICIENT_RETURN":
            return _execute_efficient_return(
                amount, returns_df, current_prices, sector_map, prev_weights, target_return
            )
        else:  # AI_PULSE / Fallback
            return _execute_proportional(amount, symbols, ai_scores, current_prices)

    except Exception as e:
        logger.error(f"Allocation strategy {strategy} failed: {e}", exc_info=True)
        return _execute_proportional(
            amount, symbols, ai_scores, current_prices, fallback=True
        )


# ── Discrete Allocation Helper ────────────────────────────────────────────────

def _apply_discrete_allocation(weights: dict, current_prices: dict, amount: float):
    """
    Converts fractional weights to exact integer share counts using LP-solving.
    NSE requires whole shares, so this makes allocations actually tradeable.
    Returns (da_weights, leftover_cash).
    """
    # Only keep symbols for which we have prices
    prices_series = pd.Series({s: current_prices.get(s, 1.0) for s in weights if weights[s] > 0.001})
    filtered_weights = {s: w for s, w in weights.items() if s in prices_series.index}

    if not filtered_weights or amount <= 0:
        return filtered_weights, 0.0

    try:
        da = DiscreteAllocation(filtered_weights, prices_series, total_portfolio_value=amount)
        alloc, leftover = da.lp_portfolio()
        # Convert share counts back to weights for consistent downstream handling
        total_invested = sum(alloc[s] * prices_series[s] for s in alloc)
        da_weights = {s: (alloc[s] * prices_series[s]) / amount for s in alloc}
        return da_weights, leftover
    except Exception as e:
        logger.warning(f"DiscreteAllocation LP failed ({e}), falling back to greedy.")
        try:
            da = DiscreteAllocation(filtered_weights, prices_series, total_portfolio_value=amount)
            alloc, leftover = da.greedy_portfolio()
            da_weights = {s: (alloc[s] * prices_series[s]) / amount for s in alloc}
            return da_weights, leftover
        except Exception as e2:
            logger.warning(f"DiscreteAllocation greedy also failed ({e2}). Using fractional weights.")
            return filtered_weights, 0.0


# ── Sector Constraints Helper ─────────────────────────────────────────────────

def _add_sector_constraints(ef: EfficientFrontier, symbols: list, sector_map: dict, max_sector_pct: float = 0.40):
    """Enforces that no single sector can exceed `max_sector_pct` of the portfolio."""
    if not sector_map:
        return
    sectors = list(set(sector_map.get(s, "Unknown") for s in symbols))
    sector_lower = {s: 0.0 for s in sectors}
    sector_upper = {s: max_sector_pct for s in sectors}
    mapper = {sym: sector_map.get(sym, "Unknown") for sym in symbols}
    try:
        ef.add_sector_constraints(mapper, sector_lower, sector_upper)
    except Exception as e:
        logger.warning(f"Sector constraints could not be applied: {e}")


# ── Strategy Implementations ──────────────────────────────────────────────────

def _execute_proportional(amount, symbols, ai_scores, current_prices, fallback=False):
    """Proportional allocation based on AI composite scores."""
    total_score = sum([ai_scores.get(s, 50) for s in symbols])
    if total_score == 0:
        total_score = len(symbols) * 50

    allocations = []
    for s in symbols:
        score = ai_scores.get(s, 50)
        weight = score / total_score
        alloc_amt = amount * weight
        price = current_prices.get(s, 1.0)
        qty = int(alloc_amt // price) if price else 0
        leftover = alloc_amt - qty * price

        allocations.append(
            {
                "symbol": s,
                "allocated_amount": round(alloc_amt, 2),
                "shares": qty,
                "leftover_cash": round(leftover, 2),
                "weight_pct": round(weight * 100, 2),
                "reason": f"Proportional allocation based on AI score of {score}"
                + (" (Fallback)" if fallback else ""),
            }
        )

    return {
        "strategy": "AI_PULSE",
        "rationale": "Direct distribution based on AI conviction levels.",
        "allocations": allocations,
        "metrics": {"expected_return": 0, "expected_volatility": 0, "expected_sharpe": 0},
    }


def _execute_hrp(amount, returns_df, current_prices, prev_weights):
    """
    Hierarchical Risk Parity: Clusters stocks to ensure diversification.
    Uses exp_cov (time-decay covariance) so recent correlations matter more —
    better for trending/momentum-sensitive NSE portfolios (#14).
    """
    # #14: exp_cov gives more weight to recent correlations (span=180 trading days ≈ 9 months)
    exp_cov_matrix = risk_models.exp_cov(returns_df, span=180)
    hrp = HRPOpt(returns_df, cov_matrix=exp_cov_matrix)
    hrp.optimize()
    cleaned_weights = hrp.clean_weights()
    ret, vol, sharpe = hrp.portfolio_performance()

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "HRP",
        "Machine-learning clustering with time-decay covariance (exp_cov) — recent correlations weighted more heavily for NSE momentum portfolios.",
        amount,
        da_weights,
        current_prices,
        {"expected_return": ret, "expected_volatility": vol, "expected_sharpe": sharpe},
        leftover_cash=leftover,
    )


def _execute_mvo(amount, returns_df, current_prices, sector_map, prev_weights):
    """
    Mean-Variance Optimization with:
    - EMA historical returns (exponential weighting, better for trend-sensitive NSE)
    - Ledoit-Wolf / OAS shrinkage covariance (stable on small portfolios)
    - L2 regularisation to prevent over-concentration into 1-2 stocks
    - Sector constraints (no sector > 40%)
    - Optional transaction cost penalty when re-balancing
    """
    # FIX #5: Use EMA returns instead of simple mean
    mu = expected_returns.ema_historical_return(returns_df, span=252)
    # FIX #3: Use shrinkage covariance instead of noisy sample_cov
    S = _get_robust_cov(returns_df)

    ef = EfficientFrontier(mu, S)

    # FIX #8: Sector constraints
    _add_sector_constraints(ef, returns_df.columns.tolist(), sector_map)

    # FIX #6: L2 regularisation to avoid over-concentration
    ef.add_objective(objective_functions.L2_reg, gamma=0.1)

    # FIX #7: Transaction cost penalty for rebalancing
    if prev_weights:
        w_prev = np.array([prev_weights.get(s, 0.0) for s in returns_df.columns])
        ef.add_objective(objective_functions.transaction_cost, w_prev=w_prev, k=0.001)

    weights = ef.max_sharpe()
    cleaned_weights = ef.clean_weights()
    ret, vol, sharpe = ef.portfolio_performance()

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "MVO",
        "Modern Portfolio Theory: Maximises risk-adjusted return using exponential weighting of recent data and shrinkage covariance.",
        amount,
        da_weights,
        current_prices,
        {"expected_return": ret, "expected_volatility": vol, "expected_sharpe": sharpe},
        leftover_cash=leftover,
    )


def _execute_black_litterman(amount, returns_df, ai_scores, current_prices, market_caps, sector_map,
                             prev_weights, nifty_prices=None):
    """
    Black-Litterman with:
    - #4: Market-cap-weighted equilibrium prior (correct BL prior)
    - #13: CAPM-based expected returns as views when Nifty prices are available
    - #3: Shrinkage covariance
    - #8: Sector constraints applied via post-BL EfficientFrontier
    """
    symbols = returns_df.columns.tolist()

    # #3: Shrinkage covariance
    S = _get_robust_cov(returns_df)

    # #4: Proper market-cap-weighted equilibrium prior
    if market_caps:
        mkt_caps_series = pd.Series({s: market_caps.get(s, 1e9) for s in symbols})
        pi = black_litterman.market_implied_prior_returns(
            mkt_caps_series, risk_aversion=2.5, cov_matrix=S
        )
    else:
        logger.warning("BL: No market cap data available. Using equal-weight prior.")
        pi = np.array([1 / len(symbols)] * len(symbols))

    # #13: Use CAPM-based views when Nifty prices are available; otherwise fall back to AI score views
    if nifty_prices is not None and not nifty_prices.empty:
        try:
            # Reconstruct price matrix from returns_df for capm_return
            prices_df = (1 + returns_df).cumprod()
            capm_mu = expected_returns.capm_return(
                prices_df,
                market_prices=nifty_prices,
                risk_free_rate=0.065  # RBI repo rate approx
            )
            views = capm_mu.to_dict()
            logger.info("BL: Using CAPM-based expected return views.")
        except Exception as e:
            logger.warning(f"BL: CAPM views failed ({e}), falling back to AI score views.")
            views = {s: (ai_scores.get(s, 50) - 50) / 500.0 for s in symbols}
    else:
        # AI score views: 50=neutral(0%), 100=+10%, 0=-10%
        views = {s: (ai_scores.get(s, 50) - 50) / 500.0 for s in symbols}

    bl = BlackLittermanModel(S, pi=pi, absolute_views=views)
    bl_returns = bl.bl_returns()
    bl_cov = bl.bl_cov()

    # #8: Apply sector constraints via a post-BL EfficientFrontier
    ef = EfficientFrontier(bl_returns, bl_cov)
    _add_sector_constraints(ef, symbols, sector_map)
    if prev_weights:
        w_prev = np.array([prev_weights.get(s, 0.0) for s in symbols])
        ef.add_objective(objective_functions.transaction_cost, w_prev=w_prev, k=0.001)
    ef.max_sharpe()
    cleaned_weights = ef.clean_weights()
    ret, vol, sharpe = ef.portfolio_performance()

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    prior_type = "CAPM" if (nifty_prices is not None and not nifty_prices.empty) else "AI-score"
    return _build_result(
        "BLACK_LITTERMAN",
        f"Market-cap equilibrium prior + {prior_type} views + sector caps. Theoretically grounded tilt-based allocation.",
        amount,
        da_weights,
        current_prices,
        {"expected_return": ret, "expected_volatility": vol, "expected_sharpe": sharpe},
        leftover_cash=leftover,
    )


def _execute_erc(amount, returns_df, current_prices, prev_weights):
    """
    Real Equal Risk Contribution (ERC) via convex custom objective.
    Each asset's marginal risk contribution is equalised, not merely
    minimising total volatility (which would concentrate into lowest-vol stock).
    """
    # FIX #2: Real ERC via convex_objective
    S = _get_robust_cov(returns_df)
    n = len(returns_df.columns)

    ef = EfficientFrontier(None, S)

    def _erc_objective(w, S):
        """
        Minimises sum of squared differences between each asset's risk contribution.
        When all risk contributions are equal the portfolio achieves true ERC.
        """
        w = np.array(w)
        portfolio_vol = np.sqrt(w @ S @ w)
        marginal_risk = S @ w
        risk_contrib = w * marginal_risk / (portfolio_vol + 1e-8)
        target = portfolio_vol / n
        return np.sum((risk_contrib - target) ** 2)

    ef.convex_objective(_erc_objective, S=S.values)
    cleaned_weights = ef.clean_weights()

    # Compute performance metrics manually since convex_objective doesn't set mu
    w_arr = np.array([cleaned_weights.get(s, 0.0) for s in returns_df.columns])
    vol = float(np.sqrt(w_arr @ S.values @ w_arr))

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "ERC",
        "True Equal Risk Contribution: Each asset contributes exactly the same amount of portfolio risk, ensuring no single stock dominates volatility.",
        amount,
        da_weights,
        current_prices,
        {"expected_return": 0, "expected_volatility": vol, "expected_sharpe": 0},
        leftover_cash=leftover,
    )


def _execute_cvar(amount, returns_df, current_prices, prev_weights):
    """
    FIX #1: Real CVaR using EfficientCVaR (not EfficientSemivariance).
    Minimises expected shortfall — the average loss in the worst 5% of scenarios.
    """
    mu = expected_returns.ema_historical_return(returns_df, span=252)

    # EfficientCVaR takes (mu, historical_rets)
    es = EfficientCVaR(mu, returns_df)
    es.min_cvar()
    cleaned_weights = es.clean_weights()
    ret, cvar = es.portfolio_performance()

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "CVAR",
        "Survival-focused: Minimises the expected loss in the worst 5% of market scenarios (Conditional Value at Risk).",
        amount,
        da_weights,
        current_prices,
        {"expected_return": ret, "expected_volatility": cvar, "expected_sharpe": 0},
        leftover_cash=leftover,
    )


def _execute_cdar(amount, returns_df, current_prices, prev_weights):
    """
    NEW — EfficientCDaR: Conditional Drawdown at Risk.
    Minimises the expected drawdown during the worst market periods.
    Intuitive for long-term Indian equity investors (sequence-of-returns risk).
    """
    mu = expected_returns.ema_historical_return(returns_df, span=252)

    es = EfficientCDaR(mu, returns_df)
    es.min_cdar()
    cleaned_weights = es.clean_weights()
    ret, cdar = es.portfolio_performance()

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "CDAR",
        "Drawdown-aware: Minimises the expected drawdown during the worst market periods — ideal for long-term NSE investors focused on sequence-of-returns risk.",
        amount,
        da_weights,
        current_prices,
        {"expected_return": ret, "expected_volatility": cdar, "expected_sharpe": 0},
        leftover_cash=leftover,
    )


def _execute_semivariance(amount, returns_df, current_prices, prev_weights):
    """
    #15: Downside-only risk model.
    EfficientSemivariance internally uses semicovariance — only penalises
    returns below zero (downside), treating upside volatility as good.
    """
    mu = expected_returns.ema_historical_return(returns_df, span=252)

    # EfficientSemivariance already uses semicovariance internally via its returns matrix.
    # Passing returns_df directly is the correct interface (not a pre-computed cov matrix).
    es = EfficientSemivariance(mu, returns_df)
    es.min_semivariance()
    cleaned_weights = es.clean_weights()
    ret, semi_vol, sharpe = es.portfolio_performance()

    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "SEMIVARIANCE",
        "Downside-risk focused: Only penalises volatility below 0% — upside swings are not punished. Best for investors with asymmetric loss aversion.",
        amount,
        da_weights,
        current_prices,
        {"expected_return": ret, "expected_volatility": semi_vol, "expected_sharpe": sharpe},
        leftover_cash=leftover,
    )


def _execute_efficient_risk(amount, returns_df, current_prices, sector_map, prev_weights, target_volatility):
    """
    #9: Target-volatility allocation.
    "I want maximum return with at most X% annual volatility."
    Calls ef.efficient_risk(target_volatility) on the efficient frontier.
    Falls back to max_sharpe if no target is specified.
    """
    mu = expected_returns.ema_historical_return(returns_df, span=252)
    S = _get_robust_cov(returns_df)
    ef = EfficientFrontier(mu, S)
    _add_sector_constraints(ef, returns_df.columns.tolist(), sector_map)
    ef.add_objective(objective_functions.L2_reg, gamma=0.1)
    if prev_weights:
        w_prev = np.array([prev_weights.get(s, 0.0) for s in returns_df.columns])
        ef.add_objective(objective_functions.transaction_cost, w_prev=w_prev, k=0.001)

    vol_target = target_volatility if target_volatility else 0.15  # default 15% p.a.
    try:
        ef.efficient_risk(target_return=vol_target)  # Note: pypfopt param is target_return for efficient_risk
    except Exception:
        # If target is outside feasible frontier, fall back to max_sharpe
        logger.warning(f"efficient_risk target {vol_target} infeasible; falling back to max_sharpe.")
        ef = EfficientFrontier(mu, S)
        _add_sector_constraints(ef, returns_df.columns.tolist(), sector_map)
        ef.add_objective(objective_functions.L2_reg, gamma=0.1)
        ef.max_sharpe()

    cleaned_weights = ef.clean_weights()
    ret, vol, sharpe = ef.portfolio_performance()
    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "EFFICIENT_RISK",
        f"Maximum return constrained to ≤{vol_target*100:.0f}% annual volatility. Slider-driven efficient frontier optimisation.",
        amount, da_weights, current_prices,
        {"expected_return": ret, "expected_volatility": vol, "expected_sharpe": sharpe},
        leftover_cash=leftover,
    )


def _execute_efficient_return(amount, returns_df, current_prices, sector_map, prev_weights, target_return):
    """
    #9: Target-return allocation.
    "I want at least X% return — minimise risk to get there."
    Calls ef.efficient_return(target_return) on the efficient frontier.
    """
    mu = expected_returns.ema_historical_return(returns_df, span=252)
    S = _get_robust_cov(returns_df)
    ef = EfficientFrontier(mu, S)
    _add_sector_constraints(ef, returns_df.columns.tolist(), sector_map)
    ef.add_objective(objective_functions.L2_reg, gamma=0.1)
    if prev_weights:
        w_prev = np.array([prev_weights.get(s, 0.0) for s in returns_df.columns])
        ef.add_objective(objective_functions.transaction_cost, w_prev=w_prev, k=0.001)

    ret_target = target_return if target_return else 0.12  # default 12% p.a.
    try:
        ef.efficient_return(target_return=ret_target)
    except Exception:
        logger.warning(f"efficient_return target {ret_target} infeasible; falling back to min_volatility.")
        ef = EfficientFrontier(mu, S)
        _add_sector_constraints(ef, returns_df.columns.tolist(), sector_map)
        ef.add_objective(objective_functions.L2_reg, gamma=0.1)
        ef.min_volatility()

    cleaned_weights = ef.clean_weights()
    ret, vol, sharpe = ef.portfolio_performance()
    da_weights, leftover = _apply_discrete_allocation(cleaned_weights, current_prices, amount)

    return _build_result(
        "EFFICIENT_RETURN",
        f"Minimum risk portfolio targeting ≥{ret_target*100:.0f}% annual return. Slider-driven efficient frontier optimisation.",
        amount, da_weights, current_prices,
        {"expected_return": ret, "expected_volatility": vol, "expected_sharpe": sharpe},
        leftover_cash=leftover,
    )


# ── Result Builder ────────────────────────────────────────────────────────────

def _build_result(strategy, rationale, amount, weights, current_prices, metrics, leftover_cash=0.0):
    """Formats the results into the standard response dict, including exact share counts."""
    allocations = []
    significant_weights = {k: v for k, v in weights.items() if v > 0.001}

    for symbol, weight in significant_weights.items():
        alloc_amt = amount * weight
        price = current_prices.get(symbol, 1.0)
        shares = int(alloc_amt // price) if price else 0

        allocations.append(
            {
                "symbol": symbol,
                "allocated_amount": round(alloc_amt, 2),
                "shares": shares,
                "leftover_cash": round(alloc_amt - shares * price, 2) if price else 0,
                "weight_pct": round(weight * 100, 2),
                "reason": f"{strategy} optimised weight: {weight * 100:.1f}%",
            }
        )

    allocations.sort(key=lambda x: x["allocated_amount"], reverse=True)

    return {
        "strategy": strategy,
        "rationale": rationale,
        "allocations": allocations,
        "leftover_cash": round(leftover_cash, 2),
        "metrics": metrics,
    }
