import pandas as pd
import pandas_ta as ta
import numpy as np

def compute_short_term_indicators(df: pd.DataFrame) -> dict:
    """
    Compute short-term indicators, expecting ~3 months (90 days) data.
    Returns dict for latest date.

    Added (ported from StockVisualizer algorithm, bugs fixed):
      - EMA 3 and EMA 7 (short-term momentum)
      - ema_crossover: +1 = bullish cross, -1 = bearish cross, 0 = none
        FIX: original used .diff() on crossover column which produced ±2 values.
             We detect the cross directly from current vs previous bar.
      - macd_hist_positive: bool — histogram direction (acceleration signal)
      - overall_trend: 'Buy Signal' / 'Sell Signal' / 'Hold'
        Logic: EMA bullish cross AND MACD bullish cross AND RSI < 70 AND close > SMA20
    """
    if len(df) < 50: # Need at least 50 days for SMA50
        return {}
        
    df = df.copy()
    
    # RSI
    df['rsi'] = ta.rsi(df['close'], length=14)
    
    # MACD
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    if macd is not None:
        df = pd.concat([df, macd], axis=1)
        macd_col  = [c for c in df.columns if c.startswith('MACD_')][0]
        macdh_col = [c for c in df.columns if c.startswith('MACDh_')][0]
        macds_col = [c for c in df.columns if c.startswith('MACDs_')][0]
    else:
        macd_col, macdh_col, macds_col = None, None, None
        
    # SMAs
    df['sma20'] = ta.sma(df['close'], length=20)
    df['sma50'] = ta.sma(df['close'], length=50)

    # EMA 3 and EMA 7 (from algorithm)
    df['ema3'] = df['close'].ewm(span=3, adjust=False).mean()
    df['ema7'] = df['close'].ewm(span=7, adjust=False).mean()

    # EMA crossover detection — FIX: compare current vs previous bar directly
    # Original algorithm used .diff() on a 0/1/-1 column which produced ±2 spurious values
    ema_cross = 0
    if len(df) >= 2:
        curr_above = df['ema3'].iloc[-1] > df['ema7'].iloc[-1]
        prev_above = df['ema3'].iloc[-2] > df['ema7'].iloc[-2]
        if curr_above and not prev_above:
            ema_cross = 1   # bullish cross
        elif not curr_above and prev_above:
            ema_cross = -1  # bearish cross

    # MACD crossover (current bar vs previous bar — same pattern)
    macd_cross = 0
    if macd_col and len(df) >= 2:
        curr_macd_above = df[macd_col].iloc[-1] > df[macds_col].iloc[-1]
        prev_macd_above = df[macd_col].iloc[-2] > df[macds_col].iloc[-2]
        if curr_macd_above and not prev_macd_above:
            macd_cross = 1
        elif not curr_macd_above and prev_macd_above:
            macd_cross = -1

    # Bollinger Bands
    bbands = ta.bbands(df['close'], length=20, std=2)
    if bbands is not None:
        df = pd.concat([df, bbands], axis=1)
        bbl_col = [c for c in df.columns if c.startswith('BBL_')][0]
        bbu_col = [c for c in df.columns if c.startswith('BBU_')][0]
    else:
        bbl_col, bbu_col = None, None
        
    # Volume & Trades
    df['vol_sma20'] = ta.sma(df['volume'], length=20)
    if 'no_of_trades' in df.columns:
        df['trades_sma20'] = ta.sma(df['no_of_trades'].astype(float), length=20)
    
    latest = df.iloc[-1]

    # Overall_Trend — composite signal (ported from algorithm, .diff() bug fixed)
    rsi_val   = float(latest['rsi'])   if pd.notna(latest.get('rsi',   pd.NA)) else None
    sma20_val = float(latest['sma20']) if pd.notna(latest.get('sma20', pd.NA)) else None
    close_val = float(latest['close'])
    overall_trend = _compute_overall_trend(ema_cross, macd_cross, rsi_val, close_val, sma20_val)

    return {
        'rsi': float(latest['rsi']) if pd.notna(latest.get('rsi', pd.NA)) else None,
        'macd_line':   float(latest[macd_col])  if macd_col  and pd.notna(latest.get(macd_col,  pd.NA)) else None,
        'macd_signal': float(latest[macds_col]) if macds_col and pd.notna(latest.get(macds_col, pd.NA)) else None,
        'macd_hist':   float(latest[macdh_col]) if macdh_col and pd.notna(latest.get(macdh_col, pd.NA)) else None,
        'macd_hist_positive': (float(latest[macdh_col]) > 0) if macdh_col and pd.notna(latest.get(macdh_col, pd.NA)) else None,
        'sma20': float(latest['sma20']) if pd.notna(latest.get('sma20', pd.NA)) else None,
        'sma50': float(latest['sma50']) if pd.notna(latest.get('sma50', pd.NA)) else None,
        'ema3':  float(latest['ema3'])  if pd.notna(latest.get('ema3',  pd.NA)) else None,
        'ema7':  float(latest['ema7'])  if pd.notna(latest.get('ema7',  pd.NA)) else None,
        'ema_crossover': ema_cross,    # +1 bullish, -1 bearish, 0 none
        'macd_crossover': macd_cross,  # +1 bullish, -1 bearish, 0 none
        'overall_trend': overall_trend,  # 'Buy Signal' / 'Sell Signal' / 'Hold'
        'bb_lower': float(latest[bbl_col]) if bbl_col and pd.notna(latest.get(bbl_col, pd.NA)) else None,
        'bb_upper': float(latest[bbu_col]) if bbu_col and pd.notna(latest.get(bbu_col, pd.NA)) else None,
        'vol_sma20': float(latest['vol_sma20']) if pd.notna(latest.get('vol_sma20', pd.NA)) else None,
        'avg_trades_20': float(latest['trades_sma20']) if 'trades_sma20' in df.columns and pd.notna(latest.get('trades_sma20', pd.NA)) else None,
        'trades_shock': (float(latest['no_of_trades']) / float(latest['trades_sma20']))
            if 'trades_sma20' in df.columns and pd.notna(latest.get('trades_sma20', pd.NA)) and float(latest.get('trades_sma20', 0)) > 0
            else 1.0,
        'close': float(latest['close']),
        'volume': float(latest['volume']),
        'no_of_trades': int(latest['no_of_trades']) if 'no_of_trades' in df.columns and pd.notna(latest.get('no_of_trades', pd.NA)) else None,
    }


def _compute_overall_trend(
    ema_cross: int,
    macd_cross: int,
    rsi: float | None,
    close: float | None,
    sma20: float | None,
) -> str:
    """
    Composite Buy/Sell/Hold signal ported from StockVisualizer.Overall_Trend.
    FIX: removed .diff() from crossover detection — now uses direct +1/-1 comparison.

    Buy Signal  = EMA bullish cross (+1) AND MACD bullish cross (+1)
                  AND RSI < 70 AND close > SMA20
    Sell Signal = EMA bearish cross (-1) AND MACD bearish cross (-1)
                  AND RSI > 30 AND close < SMA20
    """
    if rsi is None or close is None or sma20 is None:
        return 'Hold'
    if ema_cross == 1 and macd_cross == 1 and rsi < 70 and close > sma20:
        return 'Buy Signal'
    if ema_cross == -1 and macd_cross == -1 and rsi > 30 and close < sma20:
        return 'Sell Signal'
    return 'Hold'

def compute_long_term_indicators(df: pd.DataFrame) -> dict:
    """
    Compute long-term indicators, expecting full history data.
    Returns dict for latest date.

    Added (ported from StockVisualizer._add_trading_recommendations):
      - lt_recommendation: 'Buy' / 'Sell' / 'Hold'
        Logic: EMA_long now vs EMA_long[20 bars ago] ±5% threshold + RSI gate
    """
    if len(df) < 200: # Need at least 200 days for SMA200
        return {}
        
    df = df.copy()
    
    # SMAs
    df['sma50'] = ta.sma(df['close'], length=50)
    df['sma200'] = ta.sma(df['close'], length=200)

    # EMA 26 (long-term EMA used by algorithm)
    df['ema_long'] = df['close'].ewm(span=26, adjust=False).mean()
    
    # ADX
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
    if adx_df is not None:
        df = pd.concat([df, adx_df], axis=1)
        adx_col = [c for c in df.columns if c.startswith('ADX_')][0]
    else:
        adx_col = None
        
    # RSI monthly average
    df['date'] = pd.to_datetime(df['date'])
    df_indexed = df.set_index('date')
    df_indexed['rsi'] = ta.rsi(df_indexed['close'], length=14)
    monthly_rsi = df_indexed['rsi'].resample('ME').mean()
    latest_monthly_rsi = monthly_rsi.iloc[-1] if not monthly_rsi.empty else None
    
    latest = df.iloc[-1]

    # LT recommendation (ported from algorithm)
    ema_long_now = float(latest['ema_long']) if pd.notna(latest.get('ema_long', pd.NA)) else None
    ema_long_20ago = float(df['ema_long'].iloc[-21]) if len(df) >= 21 and pd.notna(df['ema_long'].iloc[-21]) else None
    rsi_now = float(df_indexed['rsi'].iloc[-1]) if pd.notna(df_indexed['rsi'].iloc[-1]) else None
    lt_rec = _compute_lt_recommendation(ema_long_now, ema_long_20ago, rsi_now)

    return {
        'sma50':   float(latest['sma50'])  if pd.notna(latest.get('sma50',  pd.NA)) else None,
        'sma200':  float(latest['sma200']) if pd.notna(latest.get('sma200', pd.NA)) else None,
        'adx':     float(latest[adx_col])  if adx_col and pd.notna(latest.get(adx_col, pd.NA)) else None,
        'rsi_monthly_avg': float(latest_monthly_rsi) if pd.notna(latest_monthly_rsi) else None,
        'ema_long_now':   ema_long_now,
        'ema_long_20ago': ema_long_20ago,
        'lt_recommendation': lt_rec,   # 'Buy' / 'Sell' / 'Hold'
        'close': float(latest['close']),
    }


def _compute_lt_recommendation(
    ema_long_now: float | None,
    ema_long_20ago: float | None,
    rsi: float | None,
) -> str:
    """
    Long-term recommendation ported from StockVisualizer._add_trading_recommendations.
    EMA_long threshold: ±5% over 20 bars + RSI confirmation.
    """
    if ema_long_now is None or ema_long_20ago is None or ema_long_20ago == 0 or rsi is None:
        return 'Hold'
    if ema_long_now > ema_long_20ago * 1.05 and rsi < 60:
        return 'Buy'
    if ema_long_now < ema_long_20ago * 0.95 and rsi > 40:
        return 'Sell'
    return 'Hold'
