"""
MarketMind — Backtesting Engine
================================
Walk-forward validation on NSE/BSE PriceHistory (2016 – present).

Design Principles
-----------------
  - Zero lookahead bias: signals only use data available on signal_date
  - Walk-forward splits: 24-month train, 6-month test, rolling forward
  - All results stored in BacktestResults table for AI skill context
  - Performance metrics per signal type + per sector + per score threshold

Usage (async, matches FastAPI/SQLAlchemy pattern):
    engine = BacktestEngine(db_session)
    results = await engine.run(
        symbol="RELIANCE",
        isin="INE002A01018",
        signal_type="composite_score",
        score_threshold=70.0,
        hold_days=252,          # ~1 year
        start_date=date(2016, 1, 1),
    )
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional


# ---------------------------------------------------------------------------
# Data containers (mirror what SQLAlchemy models return)
# ---------------------------------------------------------------------------

@dataclass
class PriceBar:
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: Optional[float] = None

    @property
    def effective_close(self) -> float:
        return self.adjusted_close if self.adjusted_close else self.close


@dataclass
class SignalEvent:
    """A single historical signal (from SignalsCache snapshots)."""
    signal_date: date
    symbol: str
    isin: str
    signal_type: str          # e.g. "composite_score", "momentum_score"
    signal_value: float       # e.g. 78.5 (the score)
    composite_score: Optional[float] = None
    sector: str = ""


@dataclass
class Trade:
    """One backtest trade (entry → exit)."""
    signal_date: date
    entry_date: date
    entry_price: float
    exit_date: Optional[date] = None
    exit_price: Optional[float] = None
    hold_days: int = 252
    symbol: str = ""
    sector: str = ""
    signal_value: float = 0.0

    @property
    def pnl_pct(self) -> Optional[float]:
        if self.exit_price is None:
            return None
        return ((self.exit_price - self.entry_price) / self.entry_price) * 100.0

    @property
    def is_winner(self) -> bool:
        pnl = self.pnl_pct
        return pnl is not None and pnl > 0


# ---------------------------------------------------------------------------
# Performance Metrics
# ---------------------------------------------------------------------------

@dataclass
class BacktestMetrics:
    symbol: str
    isin: str
    signal_type: str
    score_threshold: float
    hold_days: int
    start_date: date
    end_date: date

    total_signals: int = 0
    trades_taken: int = 0
    winners: int = 0
    losers: int = 0

    win_rate: float = 0.0           # %
    avg_return: float = 0.0         # %
    median_return: float = 0.0      # %
    best_trade: float = 0.0         # %
    worst_trade: float = 0.0        # %
    max_drawdown: float = 0.0       # % (peak-to-trough on equity curve)
    cagr: float = 0.0               # % annualised
    sharpe_ratio: float = 0.0       # annualised, risk-free = 6.5% (India 10yr avg)
    sortino_ratio: float = 0.0

    walk_forward_windows: list[dict] = field(default_factory=list)

    RISK_FREE_RATE: float = 6.5     # India 10yr gilt average

    def as_ai_context(self) -> str:
        """
        Returns a concise string injected into AI skill prompts.
        Gives the AI factual backtest grounding before it writes narrative.
        """
        if self.trades_taken == 0:
            return "No backtest data available for this signal on this stock."

        lines = [
            f"Backtest summary ({self.start_date.year}–{self.end_date.year}, "
            f"hold {self.hold_days}d, threshold score ≥{self.score_threshold}):",
            f"  Signals generated: {self.total_signals} | Trades: {self.trades_taken}",
            f"  Win rate: {self.win_rate:.1f}% | Avg return: {self.avg_return:.1f}% | "
            f"Median: {self.median_return:.1f}%",
            f"  CAGR: {self.cagr:.1f}% | Sharpe: {self.sharpe_ratio:.2f} | "
            f"Max drawdown: {self.max_drawdown:.1f}%",
            f"Best trade: +{self.best_trade:.1f}% | Worst trade: {self.worst_trade:.1f}%",
        ]
        if self.walk_forward_windows:
            consistent = sum(
                1 for w in self.walk_forward_windows if w.get("win_rate", 0) > 50
            )
            lines.append(
                f"  Walk-forward consistency: {consistent}/{len(self.walk_forward_windows)} "
                f"windows profitable."
            )
        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Returns a JSON-serializable dictionary of all metrics."""
        return {
            "symbol": self.symbol,
            "isin": self.isin,
            "signal_type": self.signal_type,
            "score_threshold": self.score_threshold,
            "hold_days": self.hold_days,
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "total_signals": self.total_signals,
            "trades_taken": self.trades_taken,
            "winners": self.winners,
            "losers": self.losers,
            "win_rate": self.win_rate,
            "avg_return": self.avg_return,
            "median_return": self.median_return,
            "best_trade": self.best_trade,
            "worst_trade": self.worst_trade,
            "max_drawdown": self.max_drawdown,
            "cagr": self.cagr,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "walk_forward_windows": self.walk_forward_windows,
        }


# ---------------------------------------------------------------------------
# Backtesting Engine
# ---------------------------------------------------------------------------

class BacktestEngine:
    """
    Runs signal backtests on PriceHistory data.

    Parameters
    ----------
    price_fetcher : callable
        async def price_fetcher(isin, start_date, end_date) -> list[PriceBar]
        Wrap your SQLAlchemy query here.

    signal_fetcher : callable
        async def signal_fetcher(isin, signal_type, start_date, end_date) -> list[SignalEvent]
        Pulls historical SignalsCache snapshots.
    """

    def __init__(self, price_fetcher, signal_fetcher):
        self.price_fetcher = price_fetcher
        self.signal_fetcher = signal_fetcher

    async def run(
        self,
        symbol: str,
        isin: str,
        signal_type: str = "composite_score",
        score_threshold: float = 65.0,
        hold_days: int = 252,
        start_date: date = date(2016, 1, 1),
        end_date: Optional[date] = None,
        walk_forward: bool = True,
        train_months: int = 24,
        test_months: int = 6,
    ) -> BacktestMetrics:

        if end_date is None:
            end_date = date.today()

        # Fetch all price data and signals
        prices: list[PriceBar] = await self.price_fetcher(isin, start_date, end_date)
        signals: list[SignalEvent] = await self.signal_fetcher(isin, signal_type, start_date, end_date)

        if not prices or not signals:
            return BacktestMetrics(
                symbol=symbol, isin=isin, signal_type=signal_type,
                score_threshold=score_threshold, hold_days=hold_days,
                start_date=start_date, end_date=end_date,
            )

        price_map = {p.date: p for p in prices}

        # Filter signals above threshold
        active_signals = [s for s in signals if s.signal_value >= score_threshold]

        # Run full-period backtest
        trades = self._simulate_trades(active_signals, price_map, hold_days)

        metrics = self._compute_metrics(
            trades=trades,
            symbol=symbol,
            isin=isin,
            signal_type=signal_type,
            score_threshold=score_threshold,
            hold_days=hold_days,
            start_date=start_date,
            end_date=end_date,
            total_signals=len(active_signals),
        )

        # Walk-forward validation
        if walk_forward:
            wf_windows = self._walk_forward(
                signals=active_signals,
                price_map=price_map,
                hold_days=hold_days,
                start_date=start_date,
                end_date=end_date,
                train_months=train_months,
                test_months=test_months,
            )
            metrics.walk_forward_windows = wf_windows

        return metrics

    # ------------------------------------------------------------------
    # Trade Simulation
    # ------------------------------------------------------------------

    def _simulate_trades(
        self,
        signals: list[SignalEvent],
        price_map: dict[date, PriceBar],
        hold_days: int,
    ) -> list[Trade]:
        trades = []
        # Deduplicate: one active trade per stock at a time
        last_entry_date: Optional[date] = None

        sorted_signals = sorted(signals, key=lambda s: s.signal_date)

        for sig in sorted_signals:
            # Entry: next available trading day after signal
            entry_bar = self._next_bar(sig.signal_date, price_map, lookahead=1)
            if entry_bar is None:
                continue

            # Skip if we're still in a previous trade
            if last_entry_date is not None:
                expected_exit = last_entry_date + timedelta(days=hold_days)
                if entry_bar.date < expected_exit:
                    continue

            # Exit: hold_days trading days later
            exit_date = entry_bar.date + timedelta(days=hold_days)
            exit_bar = self._next_bar(exit_date, price_map, lookahead=3)

            trade = Trade(
                signal_date=sig.signal_date,
                entry_date=entry_bar.date,
                entry_price=entry_bar.effective_close,
                exit_date=exit_bar.date if exit_bar else None,
                exit_price=exit_bar.effective_close if exit_bar else None,
                hold_days=hold_days,
                symbol=sig.symbol,
                sector=sig.sector,
                signal_value=sig.signal_value,
            )
            trades.append(trade)
            last_entry_date = entry_bar.date

        return trades

    def _next_bar(
        self, target_date: date, price_map: dict[date, PriceBar], lookahead: int = 1
    ) -> Optional[PriceBar]:
        for offset in range(lookahead + 5):
            d = target_date + timedelta(days=offset)
            if d in price_map:
                return price_map[d]
        return None

    # ------------------------------------------------------------------
    # Metrics Computation
    # ------------------------------------------------------------------

    def _compute_metrics(
        self,
        trades: list[Trade],
        **kwargs,
    ) -> BacktestMetrics:
        completed = [t for t in trades if t.pnl_pct is not None]
        m = BacktestMetrics(**kwargs)
        m.trades_taken = len(completed)

        if not completed:
            return m

        returns = [t.pnl_pct for t in completed]

        m.winners = sum(1 for r in returns if r > 0)
        m.losers = sum(1 for r in returns if r <= 0)
        m.win_rate = round(m.winners / len(returns) * 100, 1)
        m.avg_return = round(sum(returns) / len(returns), 2)
        m.median_return = round(_median(returns), 2)
        m.best_trade = round(max(returns), 2)
        m.worst_trade = round(min(returns), 2)

        # CAGR from equity curve
        equity = 100.0
        equity_curve = [equity]
        for r in returns:
            equity *= (1 + r / 100)
            equity_curve.append(equity)

        years = (kwargs["end_date"] - kwargs["start_date"]).days / 365.25
        if years > 0:
            m.cagr = round((equity_curve[-1] / equity_curve[0]) ** (1 / years) * 100 - 100, 2)

        # Max drawdown
        peak = equity_curve[0]
        max_dd = 0.0
        for val in equity_curve:
            if val > peak:
                peak = val
            dd = (peak - val) / peak * 100
            max_dd = max(max_dd, dd)
        m.max_drawdown = round(max_dd, 2)

        # Sharpe ratio (annualised, using India risk-free 6.5%)
        if len(returns) > 1:
            avg_r = sum(returns) / len(returns)
            std_r = math.sqrt(sum((r - avg_r) ** 2 for r in returns) / (len(returns) - 1))
            trades_per_year = 252 / kwargs["hold_days"]
            if std_r > 0:
                m.sharpe_ratio = round(
                    (avg_r - m.RISK_FREE_RATE / trades_per_year) / std_r * math.sqrt(trades_per_year), 2
                )

        # Sortino (downside deviation only)
        downside = [r for r in returns if r < 0]
        if len(downside) > 1:
            avg_r = sum(returns) / len(returns)
            downside_std = math.sqrt(sum(r ** 2 for r in downside) / len(downside))
            trades_per_year = 252 / kwargs["hold_days"]
            if downside_std > 0:
                m.sortino_ratio = round(
                    (avg_r - m.RISK_FREE_RATE / trades_per_year) / downside_std * math.sqrt(trades_per_year), 2
                )

        return m

    # ------------------------------------------------------------------
    # Walk-Forward Validation
    # ------------------------------------------------------------------

    def _walk_forward(
        self,
        signals: list[SignalEvent],
        price_map: dict[date, PriceBar],
        hold_days: int,
        start_date: date,
        end_date: date,
        train_months: int,
        test_months: int,
    ) -> list[dict]:
        windows = []
        window_start = start_date

        while True:
            train_end = _add_months(window_start, train_months)
            test_start = train_end
            test_end = _add_months(test_start, test_months)

            if test_end > end_date:
                break

            # Only use signals in the test window (train window is implicitly the lookback)
            test_signals = [
                s for s in signals
                if test_start <= s.signal_date < test_end
            ]
            test_trades = self._simulate_trades(test_signals, price_map, hold_days)
            completed = [t for t in test_trades if t.pnl_pct is not None]

            if completed:
                returns = [t.pnl_pct for t in completed]
                win_rate = sum(1 for r in returns if r > 0) / len(returns) * 100
                avg_return = sum(returns) / len(returns)
            else:
                win_rate = 0.0
                avg_return = 0.0

            windows.append({
                "test_start": str(test_start),
                "test_end": str(test_end),
                "trades": len(completed),
                "win_rate": round(win_rate, 1),
                "avg_return": round(avg_return, 2),
            })

            window_start = _add_months(window_start, test_months)

        return windows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _median(values: list[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def _add_months(d: date, months: int) -> date:
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, [31, 29 if year % 4 == 0 else 28, 31, 30, 31, 30,
                      31, 31, 30, 31, 30, 31][month - 1])
    return date(year, month, day)
