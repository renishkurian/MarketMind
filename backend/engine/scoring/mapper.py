from __future__ import annotations
import re
import pandas as pd
from typing import Optional, Dict, Any
from backend.data.db import SignalsCache, FundamentalsCache
from backend.engine.scoring.composite_score import (
    FundamentalData, TechnicalData, MomentumData
)

def build_fa_from_db(fund: Optional[FundamentalsCache]) -> FundamentalData:
    """Map FundamentalsCache SQLAlchemy object to Scorer container."""
    if not fund: 
        return FundamentalData()
    return FundamentalData(
        pe_ratio=float(fund.pe_ratio) if fund.pe_ratio else None,
        pe_5yr_avg=float(fund.pe_5yr_avg) if fund.pe_5yr_avg else None,
        roe=float(fund.roe) if fund.roe else None,
        roe_3yr_avg=float(fund.roe_3yr_avg) if fund.roe_3yr_avg else None,
        debt_equity=float(fund.debt_equity) if fund.debt_equity else None,
        revenue_growth_3yr=float(fund.revenue_growth_3yr) if fund.revenue_growth_3yr else None,
        pat_growth_3yr=float(fund.pat_growth_3yr) if fund.pat_growth_3yr else None,
        operating_margin=float(fund.operating_margin) if fund.operating_margin else None,
        promoter_holding=float(fund.promoter_holding) if fund.promoter_holding else None,
        promoter_pledge_pct=float(fund.promoter_pledge_pct) if fund.promoter_pledge_pct else None
    )

def build_ta_from_indicators(st: Dict[str, Any], lt: Dict[str, Any]) -> TechnicalData:
    """Map raw indicator dictionaries (from indicators.py) to Scorer container."""
    if not st: 
        return TechnicalData()
    
    price = st.get("close")
    s20   = st.get("sma20")
    s50   = st.get("sma50")
    s200  = lt.get("sma200")
    low   = st.get("bb_lower")
    high  = st.get("bb_upper")
    
    # Bollinger Position (0 = lower band, 1 = upper band)
    bb_pos = None
    if price and low and high and (high - low) > 0:
        bb_pos = (price - low) / (high - low)

    return TechnicalData(
        rsi_14=st.get("rsi"),
        macd_signal=st.get("macd_signal"),
        price_vs_sma20=((price - s20) / s20 * 100) if price and s20 else None,
        price_vs_sma50=((price - s50) / s50 * 100) if price and s50 else None,
        price_vs_sma200=((price - s200) / s200 * 100) if price and s200 else None,
        bb_position=bb_pos,
        adx=lt.get("adx"),
        avg_trades_20=st.get("avg_trades_20"),
        trades_shock=st.get("trades_shock"),
        ema_crossover=st.get("ema_crossover"),
        macd_crossover=st.get("macd_crossover"),
        overall_trend=st.get("overall_trend"),
        lt_recommendation=lt.get("lt_recommendation")
    )

def build_signals_from_indicators(st: Any, lt: Any) -> Dict[str, Any]:
    """
    Returns the composite signal fields that populate SignalsCache.st_signal / lt_signal.
    Replaces the hardcoded threshold logic.
    
    Accepts either raw dicts (from indicators.py) or TechnicalData objects.
    """
    def get_val(obj, key):
        if isinstance(obj, dict): return obj.get(key)
        return getattr(obj, key, None)

    overall = get_val(st, "overall_trend") or "Hold"
    lt_rec   = get_val(lt, "lt_recommendation") or "Hold"

    # Map to SignalsCache Enum values
    def to_enum(s: str) -> str:
        mapping = {
            "Buy Signal": "BUY",
            "Sell Signal": "SELL",
            "Buy": "BUY",
            "Sell": "SELL",
        }
        return mapping.get(s, "HOLD")

    return {
        "st_signal": to_enum(overall),
        "lt_signal": to_enum(lt_rec),
    }

def build_ta_from_cache(sig: Optional[SignalsCache]) -> TechnicalData:
    """Legacy/Fallback: Parse the formatted strings in indicator_breakdown back into values."""
    if not sig or not sig.indicator_breakdown: 
        return TechnicalData()
    
    ib = sig.indicator_breakdown
    st = ib.get("short_term", {})
    lt = ib.get("long_term", {})
    
    def parse_value(data_obj, key):
        if not data_obj: return None
        # Some indicators are nested structs, some are just numeric
        val = data_obj.get(key)
        if isinstance(val, (int, float)): return val
        if isinstance(val, dict): return val.get("value")
        return None

    # This is slightly more robust than regex for the EOD structured JSON
    return TechnicalData(
        rsi_14=parse_value(st.get("RSI"), "value"),
        macd_signal=parse_value(st.get("MACD"), "value"),
    )

def build_momentum_from_df(df: pd.DataFrame, nifty_df: pd.DataFrame | None = None) -> MomentumData:
    """
    Compute V2 Momentum metrics from price history DataFrame.
    Pass nifty_df (same date range) to enable relative_strength_nifty.
    """
    if df.empty or len(df) < 20: 
        return MomentumData()
    
    close = df["close"]
    roc_20  = ((close.iloc[-1] - close.iloc[-20])  / close.iloc[-20]  * 100) if len(df) >= 20  else None
    roc_60  = ((close.iloc[-1] - close.iloc[-60])  / close.iloc[-60]  * 100) if len(df) >= 60  else None
    roc_252 = ((close.iloc[-1] - close.iloc[-252]) / close.iloc[-252] * 100) if len(df) >= 252 else None
    
    # 52-week range rank
    high_52w = close.tail(252).max()
    low_52w  = close.tail(252).min()
    rank = (close.iloc[-1] - low_52w) / (high_52w - low_52w) if high_52w > low_52w else 0.5
    
    # Volume Trend
    vol    = df["volume"]
    v20    = vol.tail(20).mean()
    v90    = vol.tail(90).mean() if len(df) >= 90 else v20
    vol_ratio = (v20 / v90) if v90 and v90 > 0 else 1.0

    # Relative strength vs Nifty (6m / 126 bars)
    rs_nifty = None
    if nifty_df is not None and len(nifty_df) >= 127 and len(df) >= 127:
        nifty_close = nifty_df["close"]
        stock_ret  = close.iloc[-1]  / close.iloc[-127]
        nifty_ret  = nifty_close.iloc[-1] / nifty_close.iloc[-127]
        if nifty_ret > 0:
            rs_nifty = stock_ret / nifty_ret

    return MomentumData(
        roc_20=roc_20,
        roc_60=roc_60,
        roc_252=roc_252,
        price_52w_rank=rank,
        volume_ratio_20_90=vol_ratio,
        relative_strength_nifty=rs_nifty,
    )
