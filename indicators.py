"""
indicators.py — EntryBot / Position Guardian 공용 기술 지표
============================================================
두 봇에 중복돼 있던 계산 함수 병합. calc_atr은 Wilder smoothing (양쪽 동일 로직이었음).
가디언 전용 구조 분석(스윙/VWAP/피보나치 등)은 position_guardian.py에 유지.
"""

import math

# ─── 지표 계산 ──────────────────────────────────────────
def calc_ema(data, period):
    result = [None] * len(data)
    k = 2 / (period + 1)
    for i in range(len(data)):
        if i < period - 1: continue
        result[i] = sum(data[:period]) / period if i == period - 1 else data[i] * k + result[i-1] * (1 - k)
    return result

def calc_rsi(data, period=14):
    result = [None] * len(data)
    for i in range(period, len(data)):
        gains  = [max(data[j]-data[j-1], 0) for j in range(i-period+1, i+1)]
        losses = [max(data[j-1]-data[j], 0) for j in range(i-period+1, i+1)]
        ag, al = sum(gains)/period, sum(losses)/period
        result[i] = 100 - (100/(1+ag/al)) if al else 100
    return result

def calc_stoch_rsi(data, rsi_p=14, stoch_p=14):
    rsi = calc_rsi(data, rsi_p)
    result = [None] * len(rsi)
    for i in range(stoch_p, len(rsi)):
        window = [rsi[j] for j in range(i-stoch_p+1, i+1) if rsi[j] is not None]
        if len(window) < stoch_p: continue
        mn, mx = min(window), max(window)
        result[i] = (rsi[i] - mn) / (mx - mn) * 100 if mx != mn else 50
    return result

def calc_bollinger(data, period=20, std_mult=2.0):
    upper, mid, lower = [], [], []
    for i in range(len(data)):
        if i < period - 1:
            upper.append(None); mid.append(None); lower.append(None); continue
        sl = data[i-period+1:i+1]
        m  = sum(sl) / period
        s  = math.sqrt(sum((x-m)**2 for x in sl) / period)
        mid.append(m); upper.append(m + std_mult*s); lower.append(m - std_mult*s)
    return upper, mid, lower

def calc_donchian(highs, lows, period=20):
    dc_high = [None] * len(highs)
    dc_low  = [None] * len(lows)
    for i in range(period-1, len(highs)):
        dc_high[i] = max(highs[i-period+1:i+1])
        dc_low[i]  = min(lows[i-period+1:i+1])
    return dc_high, dc_low

def calc_bbw(upper, mid, lower):
    if upper and mid and lower and mid != 0:
        return (upper - lower) / mid
    return None

def calc_atr(highs, lows, closes, period=14):
    """ATR (Wilder smoothing). position_guardian.py와 동일 로직."""
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i]  - closes[i-1]))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr


