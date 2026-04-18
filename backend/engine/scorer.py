import math

def score_short_term(tech_indicators: dict, fundamentals: dict) -> dict:
    score = 0
    breakdown = {}
    
    # 1. RSI Score (20 pts)
    rsi = tech_indicators.get('rsi')
    rsi_score = 0
    if rsi is not None:
        if rsi < 30: rsi_score = 20
        elif 30 <= rsi < 45: rsi_score = 15
        elif 45 <= rsi < 55: rsi_score = 10
        elif 55 <= rsi < 70: rsi_score = 5
        else: rsi_score = 0
    breakdown['RSI'] = {"value": rsi, "score": rsi_score, "max": 20}
    score += rsi_score

    # 2. MACD Score (20 pts)
    macd_line = tech_indicators.get('macd_line')
    macd_signal = tech_indicators.get('macd_signal')
    macd_hist = tech_indicators.get('macd_hist')
    macd_score = 0
    
    if None not in [macd_line, macd_signal, macd_hist]:
        if macd_hist > 0 and macd_line > 0: # Bullish crossover and above signal line/zero
            macd_score = 20
        elif macd_hist > 0: # Bullish crossover only
            macd_score = 15
        elif macd_line > 0: # Above zero
            macd_score = 10
        elif macd_hist < 0 and macd_line < 0: # bearish crossover
            macd_score = 0
        else: # MACD below zero
            macd_score = 5
    breakdown['MACD'] = {"value": macd_line, "score": macd_score, "max": 20}
    score += macd_score

    # 3. SMA 20/50 Score (15 pts)
    close = tech_indicators.get('close', 0)
    sma20 = tech_indicators.get('sma20')
    sma50 = tech_indicators.get('sma50')
    sma_score = 0
    if sma20 is not None and sma50 is not None:
        if close > sma20 > sma50: sma_score = 15
        elif close > sma20: sma_score = 10
        elif close > sma50: sma_score = 5
        else: sma_score = 0
    breakdown['SMA'] = {"value": f"P:{close}, S20:{sma20}, S50:{sma50}", "score": sma_score, "max": 15}
    score += sma_score

    # 4. Bollinger Bands (15 pts)
    bbl = tech_indicators.get('bb_lower')
    bbu = tech_indicators.get('bb_upper')
    bb_score = 0
    if bbl is not None and bbu is not None and bbl > 0:
        dist_lower = abs(close - bbl) / bbl
        dist_upper = abs(bbu - close) / bbu
        
        if dist_lower <= 0.02: bb_score = 15
        elif dist_upper <= 0.02: bb_score = 0
        else: bb_score = 8
    breakdown['BB'] = {"value": f"L:{bbl}, U:{bbu}", "score": bb_score, "max": 15}
    score += bb_score

    # 5. Volume Trend (10 pts)
    vol = tech_indicators.get('volume')
    vol_sma20 = tech_indicators.get('vol_sma20')
    vol_score = 0
    # Approximate price action by seeing if price > sma20
    if vol is not None and vol_sma20 is not None and sma20 is not None:
        if vol > vol_sma20 and close > sma20: vol_score = 10
        elif vol > vol_sma20 and abs(close - sma20)/sma20 < 0.01: vol_score = 5
        elif vol < vol_sma20: vol_score = 3
        else: vol_score = 0
    breakdown['Volume'] = {"value": f"V:{vol}, V20:{vol_sma20}", "score": vol_score, "max": 10}
    score += vol_score

    # 6. Fundamentals ST (20 pts)
    pe = fundamentals.get('pe_ratio')
    sector_pe = fundamentals.get('sector_pe', 20) # Mock sector PE
    pe_score = 5
    if pe is not None and sector_pe is not None and sector_pe > 0:
        ratio = pe / sector_pe
        if ratio < 0.7: pe_score = 10
        elif 0.7 <= ratio < 1.0: pe_score = 7
        elif 1.0 <= ratio < 1.3: pe_score = 4
        else: pe_score = 0
    breakdown['PE'] = {"value": pe, "score": pe_score, "max": 10}
    score += pe_score
    
    eps = fundamentals.get('eps')
    eps_score = 5
    if eps is not None:
        if eps > 0: eps_score = 10
        elif eps == 0: eps_score = 5
        else: eps_score = 0
    breakdown['EPS'] = {"value": eps, "score": eps_score, "max": 10}
    score += eps_score

    signal = 'HOLD'
    if score > 65: signal = 'BUY'
    elif score < 40: signal = 'SELL'

    return {"signal": signal, "score": score, "breakdown": breakdown}

def score_long_term(tech_indicators: dict, fundamentals: dict) -> dict:
    score = 0
    breakdown = {}
    
    # 1. Golden/Death Cross (15 pts)
    sma50 = tech_indicators.get('sma50')
    sma200 = tech_indicators.get('sma200')
    cross_score = 0
    if sma50 is not None and sma200 is not None:
        gap = (sma50 - sma200)/sma200
        if sma50 > sma200 and gap > 0.05: cross_score = 15 # Gap widening approximated
        elif sma50 > sma200: cross_score = 10
        elif sma50 < sma200 and abs(gap) < 0.05: cross_score = 6 # Approaching
        else: cross_score = 0
    breakdown['Cross'] = {"value": f"50:{sma50}, 200:{sma200}", "score": cross_score, "max": 15}
    score += cross_score

    # 2. RSI Monthly Avg (10 pts)
    rsi_avg = tech_indicators.get('rsi_monthly_avg')
    rsi_score = 0
    if rsi_avg is not None:
        if 30 <= rsi_avg <= 60: rsi_score = 10
        elif rsi_avg < 30: rsi_score = 8
        elif 60 < rsi_avg <= 70: rsi_score = 5
        else: rsi_score = 0
    breakdown['RSI_Avg'] = {"value": rsi_avg, "score": rsi_score, "max": 10}
    score += rsi_score

    # 3. ADX (15 pts)
    adx = tech_indicators.get('adx')
    adx_score = 8
    if adx is not None and sma50 is not None and sma200 is not None:
        if adx > 25 and sma50 > sma200: adx_score = 15
        elif adx > 25 and sma50 < sma200: adx_score = 5
        elif adx < 25: adx_score = 8
    breakdown['ADX'] = {"value": adx, "score": adx_score, "max": 15}
    score += adx_score

    # 4. Fundamentals LT (60 pts)
    roe = fundamentals.get('roe')
    roe_score = 7
    if roe is not None:
        if roe > 0.20: roe_score = 15
        elif 0.15 <= roe <= 0.20: roe_score = 10
        elif 0.10 <= roe < 0.15: roe_score = 5
        else: roe_score = 0
    breakdown['ROE'] = {"value": roe, "score": roe_score, "max": 15}
    score += roe_score

    de = fundamentals.get('debt_equity')
    de_score = 7
    if de is not None:
        if de < 0.5: de_score = 15
        elif 0.5 <= de <= 1.0: de_score = 10
        elif 1.0 < de <= 2.0: de_score = 5
        else: de_score = 0
    breakdown['D/E'] = {"value": de, "score": de_score, "max": 15}
    score += de_score

    rg = fundamentals.get('revenue_growth')
    rg_score = 7
    if rg is not None:
        if rg > 0.20: rg_score = 15
        elif 0.10 <= rg <= 0.20: rg_score = 10
        elif 0 <= rg < 0.10: rg_score = 5
        else: rg_score = 0
    breakdown['Rev_Growth'] = {"value": rg, "score": rg_score, "max": 15}
    score += rg_score

    pe = fundamentals.get('pe_ratio')
    sector_pe = fundamentals.get('sector_pe', 20)
    pe_score = 7
    if pe is not None and sector_pe is not None and sector_pe > 0:
        ratio = pe / sector_pe
        if ratio < 0.7: pe_score = 15
        elif 0.7 <= ratio < 1.0: pe_score = 10
        elif 1.0 <= ratio < 1.3: pe_score = 5
        else: pe_score = 0
    breakdown['PE'] = {"value": pe, "score": pe_score, "max": 15}
    score += pe_score

    signal = 'HOLD'
    if score > 65: signal = 'BUY'
    elif score < 40: signal = 'SELL'

    return {"signal": signal, "score": score, "breakdown": breakdown}

def calculate_confidence(st_result: dict, lt_result: dict) -> float:
    st_sig = st_result['signal']
    lt_sig = lt_result['signal']
    st_score = st_result['score']
    lt_score = lt_result['score']
    
    if st_sig == 'BUY' and lt_sig == 'BUY':
        return (st_score + lt_score) / 2.0
    elif st_sig == 'SELL' and lt_sig == 'SELL':
        return 100 - ((st_score + lt_score) / 2.0)
    else:
        return abs(st_score - lt_score) / 2.0
