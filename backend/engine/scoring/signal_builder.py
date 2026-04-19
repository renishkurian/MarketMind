"""
MarketMind — Momentum & TA Computer
=====================================
Derives all MomentumData and TechnicalData sub-signals directly from
your existing PriceHistory table (close, volume, no_of_trades, high, low).

Also builds FundamentalData by reading from FundamentalsCache.

Used by the integration layer to feed CompositeScorer without any
external data source — everything comes from your own DB.

Key design notes
----------------
- All computations use only data available ON or BEFORE `as_of_date`
  (no lookahead bias — safe for both live scoring and backtesting)
- no_of_trades from PriceHistory is used directly for trades_shock (bonus signal)
- Nifty 50 relative strength uses NIFTY50 symbol in your own PriceHistory
- Minimum rows required: 252 for full signals; degrades gracefully below that
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, Sequence

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

# Your existing models
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.data.db import PriceHistory, FundamentalsCache, StockMaster, SignalsCache
from backend.engine.scoring.composite_score import (
    FundamentalData, TechnicalData, MomentumData
)


# ---------------------------------------------------------------------------
# Nifty 50 benchmark symbol (adjust if stored differently in your DB)
# ---------------------------------------------------------------------------
NIFTY_SYMBOL = "NIFTY50"
NIFTY_EXCHANGE = "NSE"

# ---------------------------------------------------------------------------
# Main builder — async, reads from your DB session
# ---------------------------------------------------------------------------

class SignalBuilder:
    """
    Builds FundamentalData, TechnicalData, and MomentumData for one symbol
    by reading directly from your existing DB tables.

    Usage:
        builder = SignalBuilder(db)
        fa  = await builder.build_fa("RELIANCE", "INE002A01018")
        ta  = await builder.build_ta("RELIANCE", "NSE", as_of_date=date.today())
        mom = await builder.build_momentum("RELIANCE", "NSE", as_of_date=date.today())
    """

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Fundamental Data — from FundamentalsCache
    # ------------------------------------------------------------------

    async def build_fa(self, symbol: str, isin: Optional[str] = None) -> FundamentalData:
        """
        Maps FundamentalsCache → FundamentalData.
        All fields already match 1:1 — no transformation needed.
        """
        result = await self.db.execute(
            select(FundamentalsCache).where(FundamentalsCache.symbol == symbol)
        )
        fc: Optional[FundamentalsCache] = result.scalar_one_or_none()

        if fc is None:
            return FundamentalData()  # all None — score will be neutral

        return FundamentalData(
            pe_ratio=_to_float(fc.pe_ratio),
            pe_5yr_avg=_to_float(fc.pe_5yr_avg),
            roe=_to_float(fc.roe),
            roe_3yr_avg=_to_float(fc.roe_3yr_avg),
            debt_equity=_to_float(fc.debt_equity),
            revenue_growth_3yr=_to_float(fc.revenue_growth_3yr),
            pat_growth_3yr=_to_float(fc.pat_growth_3yr),
            operating_margin=_to_float(fc.operating_margin),
            promoter_holding=_to_float(fc.promoter_holding),
            promoter_pledge_pct=_to_float(fc.promoter_pledge_pct),
        )

    # ------------------------------------------------------------------
    # Technical Data — from PriceHistory (last 252 bars)
    # ------------------------------------------------------------------

    async def build_ta(
        self,
        symbol: str,
        exchange: str = "NSE",
        as_of_date: Optional[date] = None,
        lookback: int = 252,
    ) -> TechnicalData:
        """
        Computes RSI, MACD, SMA comparisons, ADX, Bollinger Bands,
        and trades_shock from PriceHistory OHLCV + no_of_trades.
        """
        if as_of_date is None:
            as_of_date = date.today()

        bars = await self._fetch_bars(symbol, exchange, as_of_date, lookback)
        if len(bars) < 20:
            return TechnicalData()

        closes = [float(b.close) for b in bars]
        volumes = [int(b.volume or 0) for b in bars]
        highs  = [float(b.high  or b.close) for b in bars]
        lows   = [float(b.low   or b.close) for b in bars]
        trades = [int(b.no_of_trades or 0) for b in bars]

        current = closes[-1]

        ta = TechnicalData()

        # --- RSI 14 ---
        if len(closes) >= 15:
            ta.rsi_14 = _rsi(closes, period=14)

        # --- MACD (12, 26, 9) ---
        if len(closes) >= 35:
            macd_line, signal_line = _macd(closes)
            ta.macd_signal = macd_line - signal_line  # positive = bullish crossover

        # --- Price vs SMAs ---
        if len(closes) >= 20:
            sma20 = _sma(closes, 20)
            ta.price_vs_sma20 = ((current - sma20) / sma20) * 100

        if len(closes) >= 50:
            sma50 = _sma(closes, 50)
            ta.price_vs_sma50 = ((current - sma50) / sma50) * 100

        if len(closes) >= 200:
            sma200 = _sma(closes, 200)
            ta.price_vs_sma200 = ((current - sma200) / sma200) * 100

        # --- Bollinger Bands (20, 2σ) ---
        if len(closes) >= 20:
            sma20 = _sma(closes, 20)
            std20 = _std(closes[-20:])
            upper = sma20 + 2 * std20
            lower = sma20 - 2 * std20
            band_range = upper - lower
            if band_range > 0:
                ta.bb_position = (current - lower) / band_range  # 0=lower, 1=upper

        # --- ADX (14) ---
        if len(closes) >= 28 and len(highs) >= 28:
            ta.adx = _adx(highs, lows, closes, period=14)

        # --- Trades shock (bonus signal — uses no_of_trades from PriceHistory) ---
        if len(trades) >= 20 and trades[-1] > 0:
            avg_trades_20 = sum(trades[-20:]) / 20
            ta.avg_trades_20 = avg_trades_20
            if avg_trades_20 > 0:
                ta.trades_shock = trades[-1] / avg_trades_20

        return ta

    # ------------------------------------------------------------------
    # Momentum Data — from PriceHistory
    # ------------------------------------------------------------------

    async def build_momentum(
        self,
        symbol: str,
        exchange: str = "NSE",
        as_of_date: Optional[date] = None,
        lookback: int = 280,  # extra buffer for 252d ROC + volume ratio
    ) -> MomentumData:
        """
        Computes all MomentumData fields from PriceHistory.
        Also fetches Nifty50 bars for relative strength calculation.
        """
        if as_of_date is None:
            as_of_date = date.today()

        bars = await self._fetch_bars(symbol, exchange, as_of_date, lookback)
        if len(bars) < 21:
            return MomentumData()

        closes = [float(b.close) for b in bars]
        volumes = [int(b.volume or 0) for b in bars]

        mom = MomentumData()

        # --- Rate of change signals ---
        if len(closes) >= 21:
            mom.roc_20 = _roc(closes, 20)

        if len(closes) >= 61:
            mom.roc_60 = _roc(closes, 60)

        if len(closes) >= 253:
            mom.roc_252 = _roc(closes, 252)

        # --- Volume trend ratio: 20d avg / 90d avg ---
        if len(volumes) >= 90:
            avg_20  = sum(volumes[-20:])  / 20
            avg_90  = sum(volumes[-90:])  / 90
            if avg_90 > 0:
                mom.volume_ratio_20_90 = avg_20 / avg_90

        # --- 52-week range position ---
        if len(closes) >= 252:
            window = closes[-252:]
            lo52, hi52 = min(window), max(window)
            if hi52 > lo52:
                mom.price_52w_rank = (closes[-1] - lo52) / (hi52 - lo52)

        # --- Relative strength vs Nifty 50 (6-month, ~126 trading days) ---
        if len(closes) >= 127:
            nifty_bars = await self._fetch_bars(
                NIFTY_SYMBOL, NIFTY_EXCHANGE, as_of_date, 140
            )
            if len(nifty_bars) >= 127:
                nifty_closes = [float(b.close) for b in nifty_bars]
                stock_return  = closes[-1] / closes[-127]
                nifty_return  = nifty_closes[-1] / nifty_closes[-127]
                if nifty_return > 0:
                    mom.relative_strength_nifty = stock_return / nifty_return

        return mom

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_bars(
        self,
        symbol: str,
        exchange: str,
        as_of_date: date,
        lookback: int,
    ) -> list[PriceHistory]:
        """
        Returns up to `lookback` most recent PriceHistory rows for
        (symbol, exchange) on or before as_of_date, ordered oldest→newest.
        """
        start = as_of_date - timedelta(days=lookback * 2)  # buffer for weekends/holidays
        result = await self.db.execute(
            select(PriceHistory)
            .where(
                and_(
                    PriceHistory.symbol == symbol,
                    PriceHistory.exchange == exchange,
                    PriceHistory.date <= as_of_date,
                    PriceHistory.date >= start,
                )
            )
            .order_by(PriceHistory.date.asc())
        )
        rows = result.scalars().all()
        # Return the most recent `lookback` rows only
        return list(rows[-lookback:]) if len(rows) > lookback else list(rows)


# ---------------------------------------------------------------------------
# Sector peer builder — reads SignalsCache for sector peers
# ---------------------------------------------------------------------------

async def build_sector_data(
    db: AsyncSession,
    symbol: str,
    sector: str,
    exchange: str = "NSE",
) -> "SectorData":
    """
    Collects peer signals from SignalsCache for the same sector.
    Used by CompositeScorer._score_sector_relative().
    Returns a SectorData instance.
    """
    from backend.engine.scoring.composite_score import SectorData

    if not sector:
        return SectorData()

    # Get all stocks in the same sector from StockMaster
    peers_result = await db.execute(
        select(StockMaster.symbol)
        .where(
            and_(
                StockMaster.sector == sector,
                StockMaster.symbol != symbol,
                StockMaster.is_active == True,
                StockMaster.exchange == exchange,
            )
        )
    )
    peer_symbols = [row[0] for row in peers_result.fetchall()]

    if not peer_symbols:
        return SectorData(sector=sector)

    # Fetch latest SignalsCache + FundamentalsCache for each peer
    signals_result = await db.execute(
        select(
            SignalsCache.symbol,
            SignalsCache.momentum_score,
        )
        .where(SignalsCache.symbol.in_(peer_symbols))
        .order_by(SignalsCache.computed_at.desc())
        .distinct(SignalsCache.symbol)
    )
    peer_signals = {row.symbol: row for row in signals_result.fetchall()}

    funds_result = await db.execute(
        select(
            FundamentalsCache.symbol,
            FundamentalsCache.roe,
            FundamentalsCache.revenue_growth_3yr,
        )
        .where(FundamentalsCache.symbol.in_(peer_symbols))
    )
    peer_funds = {row.symbol: row for row in funds_result.fetchall()}

    # Build peer lists
    roe_list: list[float] = []
    rev_list: list[float] = []
    mom_list: list[float] = []

    for sym in peer_symbols:
        fd = peer_funds.get(sym)
        sg = peer_signals.get(sym)
        if fd and fd.roe is not None:
            roe_list.append(float(fd.roe))
        if fd and fd.revenue_growth_3yr is not None:
            rev_list.append(float(fd.revenue_growth_3yr))
        if sg and sg.momentum_score is not None:
            mom_list.append(float(sg.momentum_score))

    return SectorData(
        sector=sector,
        sector_roe_list=roe_list,
        sector_revenue_growth_list=rev_list,
        sector_momentum_list=mom_list,
    )


# ---------------------------------------------------------------------------
# TA indicator implementations (pure Python, no pandas required)
# Designed for Raspberry Pi — minimal dependencies
# ---------------------------------------------------------------------------

def _sma(series: list[float], period: int) -> float:
    return sum(series[-period:]) / period


def _std(series: list[float]) -> float:
    n = len(series)
    if n < 2:
        return 0.0
    mean = sum(series) / n
    return (sum((x - mean) ** 2 for x in series) / (n - 1)) ** 0.5


def _ema(series: list[float], period: int) -> list[float]:
    k = 2.0 / (period + 1)
    emas = [series[0]]
    for price in series[1:]:
        emas.append(price * k + emas[-1] * (1 - k))
    return emas


def _rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]

    # Wilder smoothing
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_gain == 0 and avg_loss == 0:
        return 50.0   # truly flat — no trend signal
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def _macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float]:
    if len(closes) < slow + signal:
        return 0.0, 0.0
    fast_ema  = _ema(closes, fast)
    slow_ema  = _ema(closes, slow)
    macd_line = [f - s for f, s in zip(fast_ema[slow - 1:], slow_ema[slow - 1:])]
    if len(macd_line) < signal:
        return macd_line[-1], macd_line[-1]
    signal_line = _ema(macd_line, signal)
    return round(macd_line[-1], 4), round(signal_line[-1], 4)


def _roc(closes: list[float], period: int) -> float:
    if len(closes) <= period or closes[-period - 1] == 0:
        return 0.0
    return round(((closes[-1] - closes[-period - 1]) / closes[-period - 1]) * 100, 2)


def _adx(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = 14,
) -> float:
    """
    Average Directional Index — Wilder's smoothing.
    Returns the ADX value (0–100; >25 = strong trend).
    """
    n = len(closes)
    if n < period * 2:
        return 0.0

    tr_list, pdm_list, ndm_list = [], [], []

    for i in range(1, n):
        high_diff = highs[i] - highs[i - 1]
        low_diff  = lows[i - 1] - lows[i]

        pdm = max(high_diff, 0.0) if high_diff > low_diff else 0.0
        ndm = max(low_diff,  0.0) if low_diff > high_diff else 0.0

        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        tr_list.append(tr)
        pdm_list.append(pdm)
        ndm_list.append(ndm)

    def _wilder_smooth(series: list[float], p: int) -> list[float]:
        smoothed = [sum(series[:p])]
        for val in series[p:]:
            smoothed.append(smoothed[-1] - smoothed[-1] / p + val)
        return smoothed

    atr  = _wilder_smooth(tr_list,  period)
    pdi_ = _wilder_smooth(pdm_list, period)
    ndi_ = _wilder_smooth(ndm_list, period)

    dx_list = []
    for a, p_, n_ in zip(atr, pdi_, ndi_):
        if a == 0:
            continue
        pdi = 100 * p_ / a
        ndi = 100 * n_ / a
        denom = pdi + ndi
        if denom == 0:
            continue
        dx_list.append(100 * abs(pdi - ndi) / denom)

    if len(dx_list) < period:
        return 0.0

    adx = sum(dx_list[-period:]) / period
    return round(adx, 2)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
