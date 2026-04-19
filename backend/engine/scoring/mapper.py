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
        roe=float(fund.roe) if fund.roe else None,
        debt_equity=float(fund.debt_equity) if fund.debt_equity else None,
        revenue_growth_3yr=float(fund.revenue_growth) if fund.revenue_growth else None,
        promoter_holding=float(fund.promoter_holding) if fund.promoter_holding else None,
        promoter_pledge_pct=float(fund.promoter_pledge_pct) if fund.promoter_pledge_pct else None
    )

def build_ta_from_indicators(st: Dict[str, Any], lt: Dict[str, Any]) -> TechnicalData:
    """Map raw indicator dictionaries (from indicators.py) to Scorer container."""
    if not st: 
        return TechnicalData()
    
    price = st.get("close")
    s20 = st.get("sma20")
    s50 = st.get("sma50")
    s200 = lt.get("sma200")

    return TechnicalData(
        rsi_14=st.get("rsi"),
        macd_signal=st.get("macd_signal"),
        price_vs_sma20=((price - s20) / s20 * 100) if price and s20 else None,
        price_vs_sma50=((price - s50) / s50 * 100) if price and s50 else None,
        price_vs_sma200=((price - s200) / s200 * 100) if price and s200 else None,
        bb_position=None, # To be added in future indicator update
        adx=lt.get("adx"),
        avg_trades_20=st.get("avg_trades_20"),
        trades_shock=st.get("trades_shock")
    )

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
        # SMA parsing still needs regex if stored as string "P:100 S20:90"
        # but in scheduler we should use raw values. 
        # For analysis.py fallback, we use regex.
    )

def build_momentum_from_df(df: pd.DataFrame) -> MomentumData:
    """Compute V2 Momentum metrics from price history DataFrame."""
    if df.empty or len(df) < 20: 
        return MomentumData()
    
    close = df["close"]
    # ROC (Rate of Change)
    roc_20 = ((close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100) if len(df) >= 20 else None
    roc_60 = ((close.iloc[-1] - close.iloc[-60]) / close.iloc[-60] * 100) if len(df) >= 60 else None
    roc_252 = ((close.iloc[-1] - close.iloc[-252]) / close.iloc[-252] * 100) if len(df) >= 252 else None
    
    # 52-week range rank
    high_52w = close.tail(252).max()
    low_52w = close.tail(252).min()
    rank = (close.iloc[-1] - low_52w) / (high_52w - low_52w) if high_52w > low_52w else 0.5
    
    # Volume Trend
    vol = df["volume"]
    v20 = vol.tail(20).mean()
    v90 = vol.tail(90).mean()
    vol_ratio = (v20 / v90) if v90 and v90 > 0 else 1.0

    return MomentumData(
        roc_20=roc_20,
        roc_60=roc_60,
        roc_252=roc_252,
        price_52w_rank=rank,
        volume_ratio_20_90=vol_ratio
    )
