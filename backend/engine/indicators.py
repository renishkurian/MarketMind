import pandas as pd
import pandas_ta as ta

def compute_short_term_indicators(df: pd.DataFrame) -> dict:
    """
    Compute short-term indicators, expecting ~3 months (90 days) data
    Returns dict for latest date.
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
        # MACD columns are typically MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9
        macd_col = [c for c in df.columns if c.startswith('MACD_')][0]
        macdh_col = [c for c in df.columns if c.startswith('MACDh_')][0]
        macds_col = [c for c in df.columns if c.startswith('MACDs_')][0]
    else:
        macd_col, macdh_col, macds_col = None, None, None
        
    # SMAs
    df['sma20'] = ta.sma(df['close'], length=20)
    df['sma50'] = ta.sma(df['close'], length=50)
    
    # Bollinger Bands
    bbands = ta.bbands(df['close'], length=20, std=2)
    if bbands is not None:
        df = pd.concat([df, bbands], axis=1)
        bbl_col = [c for c in df.columns if c.startswith('BBL_')][0]
        bbu_col = [c for c in df.columns if c.startswith('BBU_')][0]
    else:
        bbl_col, bbu_col = None, None
        
    # Volume SMA
    df['vol_sma20'] = ta.sma(df['volume'], length=20)
    
    latest = df.iloc[-1]
    
    return {
        'rsi': float(latest['rsi']) if pd.notna(latest.get('rsi', pd.NA)) else None,
        'macd_line': float(latest[macd_col]) if macd_col and pd.notna(latest.get(macd_col, pd.NA)) else None,
        'macd_signal': float(latest[macds_col]) if macds_col and pd.notna(latest.get(macds_col, pd.NA)) else None,
        'macd_hist': float(latest[macdh_col]) if macdh_col and pd.notna(latest.get(macdh_col, pd.NA)) else None,
        'sma20': float(latest['sma20']) if pd.notna(latest.get('sma20', pd.NA)) else None,
        'sma50': float(latest['sma50']) if pd.notna(latest.get('sma50', pd.NA)) else None,
        'bb_lower': float(latest[bbl_col]) if bbl_col and pd.notna(latest.get(bbl_col, pd.NA)) else None,
        'bb_upper': float(latest[bbu_col]) if bbu_col and pd.notna(latest.get(bbu_col, pd.NA)) else None,
        'vol_sma20': float(latest['vol_sma20']) if pd.notna(latest.get('vol_sma20', pd.NA)) else None,
        'close': float(latest['close']),
        'volume': float(latest['volume'])
    }

def compute_long_term_indicators(df: pd.DataFrame) -> dict:
    """
    Compute long-term indicators, expecting full history data
    Returns dict for latest date.
    """
    if len(df) < 200: # Need at least 200 days for SMA200
        return {}
        
    df = df.copy()
    
    # SMAs
    df['sma50'] = ta.sma(df['close'], length=50)
    df['sma200'] = ta.sma(df['close'], length=200)
    
    # ADX
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
    if adx_df is not None:
        df = pd.concat([df, adx_df], axis=1)
        adx_col = [c for c in df.columns if c.startswith('ADX_')][0]
    else:
        adx_col = None
        
    # RSI monthly average
    df['date'] = pd.to_datetime(df['date']) # Ensure datetime
    df_indexed = df.set_index('date')
    df_indexed['rsi'] = ta.rsi(df_indexed['close'], length=14)
    monthly_rsi = df_indexed['rsi'].resample('ME').mean() # Use 'ME' instead of 'M' for Pandas 2.2+
    latest_monthly_rsi = monthly_rsi.iloc[-1] if not monthly_rsi.empty else None
    
    latest = df.iloc[-1]
    
    return {
        'sma50': float(latest['sma50']) if pd.notna(latest.get('sma50', pd.NA)) else None,
        'sma200': float(latest['sma200']) if pd.notna(latest.get('sma200', pd.NA)) else None,
        'adx': float(latest[adx_col]) if adx_col and pd.notna(latest.get(adx_col, pd.NA)) else None,
        'rsi_monthly_avg': float(latest_monthly_rsi) if pd.notna(latest_monthly_rsi) else None,
        'close': float(latest['close'])
    }
