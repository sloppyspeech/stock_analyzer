import pandas as pd
import numpy as np
from backend.stock_data import get_price_history


def compute_technicals(symbol: str, exchange: str) -> dict:
    """Compute comprehensive technical indicators."""
    df = get_price_history(symbol, exchange, period="1y")
    if df.empty:
        return {"error": "No price data available"}

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    result = {}

    # ── Moving Averages ──────────────────────────────────────────────────────
    sma_20 = close.rolling(20).mean()
    sma_50 = close.rolling(50).mean()
    sma_200 = close.rolling(200).mean()
    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    ema_50 = close.ewm(span=50, adjust=False).mean()
    ema_200 = close.ewm(span=200, adjust=False).mean()

    current_price = float(close.iloc[-1])

    result["moving_averages"] = {
        "current_price": round(current_price, 2),
        "sma_20": safe_round(sma_20.iloc[-1]),
        "sma_50": safe_round(sma_50.iloc[-1]),
        "sma_200": safe_round(sma_200.iloc[-1]),
        "ema_12": safe_round(ema_12.iloc[-1]),
        "ema_26": safe_round(ema_26.iloc[-1]),
        "ema_50": safe_round(ema_50.iloc[-1]),
        "ema_200": safe_round(ema_200.iloc[-1]),
        "price_vs_sma20": signal_vs_ma(current_price, sma_20.iloc[-1]),
        "price_vs_sma50": signal_vs_ma(current_price, sma_50.iloc[-1]),
        "price_vs_sma200": signal_vs_ma(current_price, sma_200.iloc[-1]),
        "golden_cross": bool(sma_50.iloc[-1] > sma_200.iloc[-1]) if not pd.isna(sma_200.iloc[-1]) else None,
    }

    # ── MACD ─────────────────────────────────────────────────────────────────
    macd_line = ema_12 - ema_26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    result["macd"] = {
        "macd": safe_round(macd_line.iloc[-1]),
        "signal": safe_round(signal_line.iloc[-1]),
        "histogram": safe_round(histogram.iloc[-1]),
        "crossover": macd_crossover(macd_line, signal_line),
    }

    # ── RSI ──────────────────────────────────────────────────────────────────
    rsi_14 = compute_rsi(close, 14)
    result["rsi"] = {
        "value": safe_round(rsi_14.iloc[-1]),
        "signal": rsi_signal(rsi_14.iloc[-1]),
    }

    # ── Bollinger Bands ──────────────────────────────────────────────────────
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_pct_b = (close - bb_lower) / (bb_upper - bb_lower)
    bb_width = (bb_upper - bb_lower) / bb_mid

    result["bollinger_bands"] = {
        "upper": safe_round(bb_upper.iloc[-1]),
        "middle": safe_round(bb_mid.iloc[-1]),
        "lower": safe_round(bb_lower.iloc[-1]),
        "percent_b": safe_round(float(bb_pct_b.iloc[-1]) * 100),
        "bandwidth": safe_round(float(bb_width.iloc[-1]) * 100),
        "signal": bb_signal(current_price, float(bb_upper.iloc[-1]), float(bb_lower.iloc[-1]), float(bb_mid.iloc[-1])),
    }

    # ── Stochastic ───────────────────────────────────────────────────────────
    stoch_k, stoch_d = compute_stochastic(high, low, close)
    result["stochastic"] = {
        "k": safe_round(stoch_k.iloc[-1]),
        "d": safe_round(stoch_d.iloc[-1]),
        "signal": stoch_signal(stoch_k.iloc[-1], stoch_d.iloc[-1]),
    }

    # ── ATR (Average True Range) ─────────────────────────────────────────────
    atr = compute_atr(high, low, close, 14)
    result["atr"] = {
        "value": safe_round(atr.iloc[-1]),
        "percent": safe_round(float(atr.iloc[-1]) / current_price * 100),
    }

    # ── ADX (Average Directional Index) ──────────────────────────────────────
    adx, plus_di, minus_di = compute_adx(high, low, close)
    result["adx"] = {
        "adx": safe_round(adx.iloc[-1]),
        "plus_di": safe_round(plus_di.iloc[-1]),
        "minus_di": safe_round(minus_di.iloc[-1]),
        "trend_strength": adx_strength(adx.iloc[-1]),
        "direction": "Bullish" if float(plus_di.iloc[-1]) > float(minus_di.iloc[-1]) else "Bearish",
    }

    # ── Volume Analysis ──────────────────────────────────────────────────────
    vol_sma_20 = volume.rolling(20).mean()
    result["volume"] = {
        "current": int(volume.iloc[-1]),
        "avg_20d": int(vol_sma_20.iloc[-1]),
        "ratio": safe_round(float(volume.iloc[-1]) / float(vol_sma_20.iloc[-1])),
        "signal": "High" if float(volume.iloc[-1]) > float(vol_sma_20.iloc[-1]) * 1.5 else
                  "Low" if float(volume.iloc[-1]) < float(vol_sma_20.iloc[-1]) * 0.5 else "Normal",
    }

    # ── Support & Resistance ─────────────────────────────────────────────────
    result["support_resistance"] = compute_support_resistance(high, low, close)

    # ── Pivot Points ─────────────────────────────────────────────────────────
    result["pivot_points"] = compute_pivot_points(
        float(high.iloc[-2]), float(low.iloc[-2]), float(close.iloc[-2])
    )

    # ── OBV (On-Balance Volume) ──────────────────────────────────────────────
    obv = compute_obv(close, volume)
    obv_ema = obv.ewm(span=20).mean()
    result["obv"] = {
        "trend": "Bullish" if float(obv.iloc[-1]) > float(obv_ema.iloc[-1]) else "Bearish",
    }

    # ── Overall Signal ────────────────────────────────────────────────────────
    result["overall_signal"] = compute_overall_signal(result)

    # ── OHLCV for charting ───────────────────────────────────────────────────
    chart_data = []
    for i, (idx, row) in enumerate(df.tail(180).iterrows()):
        sma20_val = sma_20.iloc[-(180 - i)] if i < len(sma_20) else None
        sma50_val = sma_50.iloc[-(180 - i)] if i < len(sma_50) else None
        bb_u = bb_upper.iloc[-(180 - i)] if i < len(bb_upper) else None
        bb_l = bb_lower.iloc[-(180 - i)] if i < len(bb_lower) else None

        chart_data.append({
            "date": str(idx.date()),
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
            "volume": int(row["Volume"]),
            "sma20": safe_round(sma20_val) if sma20_val is not None else None,
            "sma50": safe_round(sma50_val) if sma50_val is not None else None,
            "bb_upper": safe_round(bb_u) if bb_u is not None else None,
            "bb_lower": safe_round(bb_l) if bb_l is not None else None,
        })

    result["chart_data"] = chart_data

    return result


# ── Helper functions ──────────────────────────────────────────────────────────

def safe_round(val, decimals=2):
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return round(float(val), decimals)
    except Exception:
        return None


def signal_vs_ma(price, ma):
    if ma is None or np.isnan(ma):
        return "N/A"
    pct = (price - ma) / ma * 100
    if pct > 5:
        return f"Above ({pct:+.1f}%)"
    elif pct < -5:
        return f"Below ({pct:+.1f}%)"
    else:
        return f"Near ({pct:+.1f}%)"


def compute_rsi(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def rsi_signal(val):
    if val is None or np.isnan(val):
        return "N/A"
    if val >= 70:
        return "Overbought"
    elif val <= 30:
        return "Oversold"
    elif val >= 60:
        return "Bullish"
    elif val <= 40:
        return "Bearish"
    return "Neutral"


def macd_crossover(macd, signal):
    if len(macd) < 2:
        return "N/A"
    if float(macd.iloc[-2]) < float(signal.iloc[-2]) and float(macd.iloc[-1]) > float(signal.iloc[-1]):
        return "Bullish Crossover"
    elif float(macd.iloc[-2]) > float(signal.iloc[-2]) and float(macd.iloc[-1]) < float(signal.iloc[-1]):
        return "Bearish Crossover"
    elif float(macd.iloc[-1]) > float(signal.iloc[-1]):
        return "Bullish"
    return "Bearish"


def bb_signal(price, upper, lower, mid):
    if price >= upper:
        return "Overbought / Near Upper Band"
    elif price <= lower:
        return "Oversold / Near Lower Band"
    elif price > mid:
        return "Above Middle Band (Bullish)"
    return "Below Middle Band (Bearish)"


def compute_stochastic(high, low, close, k_period=14, d_period=3):
    lowest_low = low.rolling(k_period).min()
    highest_high = high.rolling(k_period).max()
    k = 100 * (close - lowest_low) / (highest_high - lowest_low)
    d = k.rolling(d_period).mean()
    return k, d


def stoch_signal(k, d):
    if k is None or np.isnan(k):
        return "N/A"
    if k > 80:
        return "Overbought"
    elif k < 20:
        return "Oversold"
    elif k > d:
        return "Bullish"
    return "Bearish"


def compute_atr(high, low, close, period=14):
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def compute_adx(high, low, close, period=14):
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

    plus_di = 100 * plus_dm.ewm(span=period, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(span=period, adjust=False).mean() / atr
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.ewm(span=period, adjust=False).mean()
    return adx, plus_di, minus_di


def adx_strength(val):
    if val is None or np.isnan(val):
        return "N/A"
    val = float(val)
    if val < 20:
        return "Weak / No Trend"
    elif val < 40:
        return "Moderate Trend"
    elif val < 60:
        return "Strong Trend"
    return "Very Strong Trend"


def compute_support_resistance(high, low, close, lookback=60):
    recent_high = high.tail(lookback)
    recent_low = low.tail(lookback)
    current = float(close.iloc[-1])

    resistance_levels = []
    support_levels = []

    # Find local maxima/minima
    for i in range(1, len(recent_high) - 1):
        if float(recent_high.iloc[i]) > float(recent_high.iloc[i - 1]) and \
           float(recent_high.iloc[i]) > float(recent_high.iloc[i + 1]):
            level = float(recent_high.iloc[i])
            if level > current:
                resistance_levels.append(level)

        if float(recent_low.iloc[i]) < float(recent_low.iloc[i - 1]) and \
           float(recent_low.iloc[i]) < float(recent_low.iloc[i + 1]):
            level = float(recent_low.iloc[i])
            if level < current:
                support_levels.append(level)

    # Keep closest 3 levels
    resistance_levels = sorted(set([round(x, 2) for x in resistance_levels]))[:3]
    support_levels = sorted(set([round(x, 2) for x in support_levels]), reverse=True)[:3]

    return {
        "resistance": resistance_levels,
        "support": support_levels,
        "fifty_two_week_high": round(float(high.max()), 2),
        "fifty_two_week_low": round(float(low.min()), 2),
    }


def compute_pivot_points(prev_high, prev_low, prev_close):
    pivot = (prev_high + prev_low + prev_close) / 3
    r1 = 2 * pivot - prev_low
    r2 = pivot + (prev_high - prev_low)
    r3 = prev_high + 2 * (pivot - prev_low)
    s1 = 2 * pivot - prev_high
    s2 = pivot - (prev_high - prev_low)
    s3 = prev_low - 2 * (prev_high - pivot)
    return {
        "pivot": round(pivot, 2),
        "r1": round(r1, 2), "r2": round(r2, 2), "r3": round(r3, 2),
        "s1": round(s1, 2), "s2": round(s2, 2), "s3": round(s3, 2),
    }


def compute_obv(close, volume):
    obv = [0]
    for i in range(1, len(close)):
        if close.iloc[i] > close.iloc[i - 1]:
            obv.append(obv[-1] + volume.iloc[i])
        elif close.iloc[i] < close.iloc[i - 1]:
            obv.append(obv[-1] - volume.iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=close.index)


def compute_overall_signal(data: dict) -> dict:
    """Aggregate all signals into a buy/sell/hold recommendation."""
    signals = []  # (indicator, signal, weight, value_str)

    # RSI
    rsi_data = data.get("rsi", {})
    rsi_val = rsi_data.get("value")
    rsi_sig = rsi_data.get("signal", "")
    rsi_str = f"{rsi_val}" if rsi_val is not None else "N/A"
    if "Oversold" in rsi_sig:
        signals.append(("RSI", "BUY", 2, f"{rsi_str} (Oversold)"))
    elif "Overbought" in rsi_sig:
        signals.append(("RSI", "SELL", 2, f"{rsi_str} (Overbought)"))
    elif "Bullish" in rsi_sig:
        signals.append(("RSI", "BUY", 1, f"{rsi_str} (Bullish)"))
    elif "Bearish" in rsi_sig:
        signals.append(("RSI", "SELL", 1, f"{rsi_str} (Bearish)"))

    # MACD
    macd_data = data.get("macd", {})
    macd_val = macd_data.get("macd")
    macd_sig = macd_data.get("crossover", "")
    macd_str = f"{macd_val}" if macd_val is not None else "N/A"
    if "Bullish" in macd_sig:
        signals.append(("MACD", "BUY", 2 if "Crossover" in macd_sig else 1, f"{macd_str} ({macd_sig})"))
    elif "Bearish" in macd_sig:
        signals.append(("MACD", "SELL", 2 if "Crossover" in macd_sig else 1, f"{macd_str} ({macd_sig})"))

    # Moving Averages
    ma = data.get("moving_averages", {})
    price = ma.get("current_price", "N/A")
    if ma.get("golden_cross"):
        signals.append(("MA Cross", "BUY", 2, "Golden Cross (SMA50 > SMA200)"))
    elif ma.get("golden_cross") is False:
        signals.append(("MA Cross", "SELL", 2, "Death Cross (SMA50 < SMA200)"))

    for key, label in [("price_vs_sma20", "SMA 20"), ("price_vs_sma50", "SMA 50"), ("price_vs_sma200", "SMA 200")]:
        sig = ma.get(key, "")
        ma_val = ma.get(key.replace("price_vs_", ""), "N/A")
        if "Above" in sig:
            signals.append((label, "BUY", 1, f"{price} vs {ma_val} {sig}"))
        elif "Below" in sig:
            signals.append((label, "SELL", 1, f"{price} vs {ma_val} {sig}"))

    # Stochastic
    stoch_data = data.get("stochastic", {})
    stoch_k = stoch_data.get("k")
    stoch_d = stoch_data.get("d")
    stoch_sig = stoch_data.get("signal", "")
    stoch_str = f"K:{stoch_k} D:{stoch_d}" if stoch_k is not None else "N/A"
    if "Oversold" in stoch_sig:
        signals.append(("Stochastic", "BUY", 2, f"{stoch_str} (Oversold)"))
    elif "Overbought" in stoch_sig:
        signals.append(("Stochastic", "SELL", 2, f"{stoch_str} (Overbought)"))

    # Bollinger Bands
    bb_data = data.get("bollinger_bands", {})
    bb_pct = bb_data.get("percent_b")
    bb_sig = bb_data.get("signal", "")
    bb_str = f"%B: {bb_pct}" if bb_pct is not None else "N/A"
    if "Oversold" in bb_sig:
        signals.append(("Bollinger", "BUY", 1, f"{bb_str} (Near Lower)"))
    elif "Overbought" in bb_sig:
        signals.append(("Bollinger", "SELL", 1, f"{bb_str} (Near Upper)"))

    # OBV
    obv_data = data.get("obv", {})
    obv_trend = obv_data.get("trend", "")
    if obv_trend == "Bullish":
        signals.append(("OBV", "BUY", 1, "OBV > EMA (Accumulation)"))
    elif obv_trend == "Bearish":
        signals.append(("OBV", "SELL", 1, "OBV < EMA (Distribution)"))

    # ADX
    adx_data = data.get("adx", {})
    adx_val = adx_data.get("adx")
    adx_dir = adx_data.get("direction", "")
    adx_str = f"ADX: {adx_val}" if adx_val is not None else "N/A"
    if adx_val is not None and adx_val > 25:
        if adx_dir == "Bullish":
            signals.append(("ADX", "BUY", 1, f"{adx_str} ({adx_data.get('trend_strength', '')})"))
        elif adx_dir == "Bearish":
            signals.append(("ADX", "SELL", 1, f"{adx_str} ({adx_data.get('trend_strength', '')})"))

    # Tally
    buy_score = sum(w for _, s, w, _ in signals if s == "BUY")
    sell_score = sum(w for _, s, w, _ in signals if s == "SELL")
    total = buy_score + sell_score

    if total == 0:
        overall = "NEUTRAL"
        confidence = 0
    elif buy_score > sell_score * 1.5:
        overall = "STRONG BUY"
        confidence = round(buy_score / total * 100)
    elif buy_score > sell_score:
        overall = "BUY"
        confidence = round(buy_score / total * 100)
    elif sell_score > buy_score * 1.5:
        overall = "STRONG SELL"
        confidence = round(sell_score / total * 100)
    elif sell_score > buy_score:
        overall = "SELL"
        confidence = round(sell_score / total * 100)
    else:
        overall = "NEUTRAL"
        confidence = 50

    return {
        "signal": overall,
        "confidence": confidence,
        "buy_score": buy_score,
        "sell_score": sell_score,
        "signals": [{"indicator": s[0], "signal": s[1], "weight": s[2], "value": s[3]} for s in signals],
    }
