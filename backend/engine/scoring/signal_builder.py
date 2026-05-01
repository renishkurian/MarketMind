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
from backend.engine.indicators import compute_short_term_indicators, compute_long_term_indicators
import pandas as pd


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

        fa = FundamentalData(
            pe_ratio=_to_float(fc.pe_ratio),
            pe_5yr_avg=_to_float(fc.pe_5yr_avg),
            roe=_to_float(fc.roe),
            roe_3yr_avg=_to_float(fc.roe_3yr_avg),
            debt_equity=_to_float(fc.debt_equity),
            revenue_growth_3yr=_to_float(fc.revenue_growth_3yr),
            revenue_growth=_to_float(fc.revenue_growth),
            pat_growth_3yr=_to_float(fc.pat_growth_3yr),
            operating_margin=_to_float(fc.operating_margin),
            promoter_holding=_to_float(fc.promoter_holding),
            promoter_pledge_pct=_to_float(fc.promoter_pledge_pct),
        )

        from backend.data.db import ScreenerCache
        sc_result = await self.db.execute(
            select(ScreenerCache).where(ScreenerCache.symbol == symbol)
        )
        sc: Optional[ScreenerCache] = sc_result.scalar_one_or_none()

        if sc:
            fa.roce = _to_float(sc.roce)
            fa.revenue_cagr_5yr = _to_float(sc.revenue_cagr_5yr)
            fa.revenue_cagr_10yr = _to_float(sc.revenue_cagr_10yr)
            fa.profit_cagr_5yr = _to_float(sc.profit_cagr_5yr)
            fa.profit_cagr_10yr = _to_float(sc.profit_cagr_10yr)
            fa.debtor_days = _to_float(sc.debtor_days)
            fa.cash_conversion_cycle = _to_float(sc.cash_conversion_cycle)

            fa.cfo_pat_ratio = _derive_cfo_pat(sc.annual_cashflows, sc.annual_pnl)
            fa.fii_trend_direction, fa.fii_trend_quarters = _derive_fii_trend(sc.shareholding_history)

        return fa

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

        # --- Core Expert Indicators ---
        df = pd.DataFrame({
            "date": [b.date for b in bars],
            "close": closes,
            "open": [float(b.open or 0) for b in bars],
            "high": highs,
            "low": lows,
            "volume": volumes,
            "no_of_trades": trades
        })
        
        st_inds = compute_short_term_indicators(df)
        lt_inds = compute_long_term_indicators(df)
        
        from backend.engine.scoring.composite_score import calculate_adx, calculate_atr
        
        adx_info = calculate_adx(highs, lows, closes)
        ta.adx = adx_info["adx"]
        ta.plus_di = adx_info["plus_di"]
        ta.minus_di = adx_info["minus_di"]
        ta.atr = calculate_atr(highs, lows, closes)

        ta.rsi_14 = st_inds.get("rsi")
        ta.macd_signal = st_inds.get("macd_signal")
        ta.ema_crossover = st_inds.get("ema_crossover")
        ta.macd_crossover = st_inds.get("macd_crossover")
        ta.overall_trend = st_inds.get("overall_trend")
        ta.lt_recommendation = lt_inds.get("lt_recommendation")
        ta.avg_trades_20 = st_inds.get("avg_trades_20")
        ta.trades_shock = st_inds.get("trades_shock")
        
        # SMAs from indicators (if available) or manual for distances
        ta.price_vs_sma20 = ((current - st_inds.get("sma20", 0)) / st_inds.get("sma20", 1) * 100) if st_inds.get("sma20") else None
        ta.price_vs_sma50 = ((current - st_inds.get("sma50", 0)) / st_inds.get("sma50", 1) * 100) if st_inds.get("sma50") else None
        ta.price_vs_sma200 = ((current - lt_inds.get("sma200", 0)) / lt_inds.get("sma200", 1) * 100) if lt_inds.get("sma200") else None

        # Bollinger BB Pos
        low = st_inds.get("bb_lower")
        high = st_inds.get("bb_upper")
        if low is not None and high is not None and (high - low) > 0:
            ta.bb_position = (current - low) / (high - low)

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

        from backend.data.db import ScreenerCache, CorporateAction

        sc_result = await self.db.execute(
            select(ScreenerCache.quarterly_results).where(ScreenerCache.symbol == symbol)
        )
        sc_row = sc_result.first()
        if sc_row and sc_row[0]:
            mom.earnings_velocity, mom.earnings_velocity_quarters = _derive_earnings_velocity(sc_row[0])

        today = as_of_date
        window_end = today + timedelta(days=30)
        ca_result = await self.db.execute(
            select(CorporateAction)
            .where(
                and_(
                    CorporateAction.symbol == symbol,
                    CorporateAction.ex_date >= today,
                    CorporateAction.ex_date <= window_end,
                )
            )
            .order_by(CorporateAction.ex_date.asc())
            .limit(1)
        )
        ca = ca_result.scalar_one_or_none()
        if ca:
            days_away = (ca.ex_date - today).days
            mom.corporate_action_days = days_away
            purpose_lower = (ca.purpose or "").lower()
            if "dividend" in purpose_lower or "div" in purpose_lower:
                mom.corporate_action_proximity = "DIVIDEND_SOON"
            elif "bonus" in purpose_lower:
                mom.corporate_action_proximity = "BONUS_SOON"
            elif "split" in purpose_lower:
                mom.corporate_action_proximity = "SPLIT_SOON"

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

    from backend.data.db import ScreenerCache
    screener_result = await db.execute(
        select(
            ScreenerCache.symbol,
            ScreenerCache.roce,
        )
        .where(ScreenerCache.symbol.in_(peer_symbols))
    )
    peer_screener = {row.symbol: row for row in screener_result.fetchall()}

    # Build peer lists
    roe_list: list[float] = []
    rev_list: list[float] = []
    mom_list: list[float] = []
    roce_list: list[float] = []

    for sym in peer_symbols:
        fd = peer_funds.get(sym)
        sg = peer_signals.get(sym)
        sc = peer_screener.get(sym)
        
        if fd and fd.roe is not None:
            roe_list.append(float(fd.roe))
        if fd and fd.revenue_growth_3yr is not None:
            rev_list.append(float(fd.revenue_growth_3yr))
        if sg and sg.momentum_score is not None:
            mom_list.append(float(sg.momentum_score))
        if sc and sc.roce is not None:
            try: roce_list.append(float(sc.roce))
            except: pass

    return SectorData(
        sector=sector,
        sector_roe_list=roe_list,
        sector_revenue_growth_list=rev_list,
        sector_momentum_list=mom_list,
        sector_roce_list=roce_list,
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

def _derive_cfo_pat(cashflows_json, pnl_json) -> Optional[float]:
    """
    Parse annual_cashflows and annual_pnl JSON from ScreenerCache.
    Returns CFO / Net Profit for the most recent full year.
    Returns None if data is missing or net profit is 0/negative.
    """
    try:
        if not cashflows_json or not pnl_json:
            return None
        cf_list = cashflows_json if isinstance(cashflows_json, list) else []
        pnl_list = pnl_json if isinstance(pnl_json, list) else []

        # Build lookup by report date
        cfo_by_date = {}
        for row in cf_list:
            rd = row.get("Report Date") or row.get("report_date")
            cfo = row.get("Cash from Operating Activity +") or row.get("CFO")
            if rd and cfo is not None:
                try: cfo_by_date[rd] = float(cfo)
                except: pass

        for row in sorted(pnl_list, key=lambda x: x.get("Report Date",""), reverse=True):
            rd = row.get("Report Date") or row.get("report_date")
            pat = row.get("Net Profit +") or row.get("Net Profit")
            if rd and pat is not None and rd in cfo_by_date:
                try:
                    pat_f = float(pat)
                    if pat_f <= 0:
                        return None
                    return round(cfo_by_date[rd] / pat_f, 3)
                except:
                    pass
        return None
    except:
        return None

def _derive_fii_trend(shareholding_history_json) -> tuple[Optional[str], Optional[int]]:
    """
    Parse shareholding_history JSON from ScreenerCache.
    Returns (direction, consecutive_quarters).
    """
    try:
        if not shareholding_history_json:
            return None, None
        history = shareholding_history_json if isinstance(shareholding_history_json, list) else []

        # Extract FII % per quarter — try multiple key names
        fii_vals = []
        for row in history[:6]:  # last 6 quarters max
            for key in ["FII", "fii", "FII Holding", "fii_holding"]:
                v = row.get(key)
                if v is not None:
                    try:
                        fii_vals.append(float(v))
                        break
                    except:
                        pass

        if len(fii_vals) < 2:
            return None, None

        # Deltas: positive = accumulating (newer data is index 0 = most recent)
        deltas = [fii_vals[i-1] - fii_vals[i] for i in range(1, len(fii_vals))]

        # Count consecutive streak from most recent quarter
        if not deltas:
            return "STABLE", 0

        direction_sign = 1 if deltas[0] > 0 else (-1 if deltas[0] < 0 else 0)
        streak = 0
        for d in deltas:
            d_sign = 1 if d > 0.05 else (-1 if d < -0.05 else 0)  # 0.05% threshold to avoid noise
            if d_sign == direction_sign and direction_sign != 0:
                streak += 1
            else:
                break

        if direction_sign == 1 and streak >= 2:
            return "ACCUMULATING", streak
        elif direction_sign == -1 and streak >= 2:
            return "REDUCING", streak
        else:
            return "STABLE", 0
    except:
        return None, None

def _derive_earnings_velocity(quarterly_results_json) -> tuple[Optional[str], Optional[int]]:
    """
    Parse quarterly_results JSON from ScreenerCache.
    Returns (velocity, consecutive_quarters).
    """
    try:
        if not quarterly_results_json:
            return None, None
        rows = quarterly_results_json if isinstance(quarterly_results_json, list) else []

        profits = []
        for row in rows[:6]:
            for key in ["Net Profit +", "Net Profit", "net_profit"]:
                v = row.get(key)
                if v is not None:
                    try:
                        profits.append(float(v))
                        break
                    except:
                        pass

        if len(profits) < 4:
            return None, None

        # Deltas newest-first: positive = this quarter better than previous
        deltas = [profits[i-1] - profits[i] for i in range(1, len(profits))]

        direction_sign = 1 if deltas[0] > 0 else (-1 if deltas[0] < 0 else 0)
        streak = 0
        for d in deltas:
            d_sign = 1 if d > 0 else (-1 if d < 0 else 0)
            if d_sign == direction_sign and direction_sign != 0:
                streak += 1
            else:
                break

        if direction_sign == 1 and streak >= 3:
            return "ACCELERATING", streak
        elif direction_sign == -1 and streak >= 3:
            return "DECELERATING", streak
        else:
            return "STABLE", 0
    except:
        return None, None
