import requests
import json

OLLAMA_BASE_URL = "http://localhost:11435"


def get_available_models() -> list:
    """Fetch available Ollama models."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if resp.ok:
            return [m["name"] for m in resp.json().get("models", [])]
        return []
    except Exception:
        return []


def ollama_generate(prompt: str, model: str = "llama3.2", system: str = None) -> str:
    """Call Ollama generate API."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.3,
            "top_p": 0.9,
            "num_predict": 2048,
        }
    }
    if system:
        payload["system"] = system

    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json=payload,
            timeout=120
        )
        if resp.ok:
            return resp.json().get("response", "No response from model.")
        return f"Ollama API error: {resp.status_code} - {resp.text}"
    except requests.ConnectionError:
        return "❌ Cannot connect to Ollama. Ensure Ollama is running on port 11435."
    except requests.Timeout:
        return "❌ Ollama request timed out. Try a faster/smaller model."
    except Exception as e:
        return f"❌ Error: {str(e)}"


def analyze_fundamentals_ai(symbol: str, company_name: str, fundamentals: dict, model: str = "llama3.2") -> str:
    """AI-powered fundamental analysis."""

    system = """You are an expert equity research analyst specializing in Indian stock markets (NSE/BSE).
You provide detailed, data-driven fundamental analysis to help investors make informed decisions.
Always structure your response with clear sections.
Be specific with numbers. Highlight risks and opportunities.
End with a clear investment thesis."""

    val = fundamentals.get("valuation", {})
    prof = fundamentals.get("profitability", {})
    grow = fundamentals.get("growth", {})
    health = fundamentals.get("financial_health", {})
    per_share = fundamentals.get("per_share", {})
    analyst = fundamentals.get("analyst", {})

    def fmt(val, suffix="", multiplier=1, prefix=""):
        if val is None:
            return "N/A"
        return f"{prefix}{round(float(val) * multiplier, 2)}{suffix}"

    def fmt_pct(val):
        if val is None:
            return "N/A"
        return f"{round(float(val) * 100, 2)}%"

    def fmt_cr(val):
        """Format in crores."""
        if val is None:
            return "N/A"
        return f"₹{round(float(val) / 1e7, 2)} Cr"

    prompt = f"""Analyze the following fundamental data for {company_name} ({symbol}) listed on Indian markets.

=== VALUATION METRICS ===
P/E Ratio (Trailing): {fmt(val.get('pe_ratio'))}
P/E Ratio (Forward): {fmt(val.get('forward_pe'))}
Price-to-Book (P/B): {fmt(val.get('pb_ratio'))}
Price-to-Sales (P/S): {fmt(val.get('ps_ratio'))}
EV/EBITDA: {fmt(val.get('ev_ebitda'))}
EV/Revenue: {fmt(val.get('ev_revenue'))}

=== PROFITABILITY ===
Return on Equity (ROE): {fmt_pct(prof.get('roe'))}
Return on Assets (ROA): {fmt_pct(prof.get('roa'))}
Net Profit Margin: {fmt_pct(prof.get('profit_margin'))}
Operating Margin: {fmt_pct(prof.get('operating_margin'))}
Gross Margin: {fmt_pct(prof.get('gross_margin'))}

=== GROWTH ===
Revenue Growth (YoY): {fmt_pct(grow.get('revenue_growth'))}
Earnings Growth (YoY): {fmt_pct(grow.get('earnings_growth'))}
Quarterly Earnings Growth: {fmt_pct(grow.get('earnings_quarterly_growth'))}

=== FINANCIAL HEALTH ===
Debt-to-Equity: {fmt(health.get('debt_to_equity'))}
Current Ratio: {fmt(health.get('current_ratio'))}
Quick Ratio: {fmt(health.get('quick_ratio'))}
Free Cash Flow: {fmt_cr(health.get('free_cashflow'))}

=== PER SHARE DATA ===
EPS (Trailing): ₹{fmt(per_share.get('eps'))}
EPS (Forward): ₹{fmt(per_share.get('forward_eps'))}
Book Value per Share: ₹{fmt(per_share.get('book_value'))}
Dividend Yield: {fmt_pct(per_share.get('dividend_yield'))}
Payout Ratio: {fmt_pct(per_share.get('payout_ratio'))}

=== ANALYST COVERAGE ===
Analyst Recommendation: {analyst.get('recommendation', 'N/A').upper()}
No. of Analysts: {analyst.get('number_of_analyst_opinions', 'N/A')}
Price Target (Mean): ₹{fmt(analyst.get('target_mean'))}
Price Target Range: ₹{fmt(analyst.get('target_low'))} - ₹{fmt(analyst.get('target_high'))}

Please provide:
1. **Valuation Assessment** — Is the stock cheap, fairly valued, or expensive? Compare to typical Indian market benchmarks.
2. **Profitability Analysis** — Quality of earnings, margin trends, capital efficiency (ROE/ROA).
3. **Growth Outlook** — Revenue and earnings trajectory, sustainability of growth.
4. **Balance Sheet Strength** — Debt levels, liquidity, financial stability.
5. **Dividend Analysis** — Income potential, payout sustainability.
6. **Key Risks** — What could go wrong? Red flags in the data.
7. **Key Opportunities** — What could drive upside?
8. **Investment Thesis** — Clear BUY / HOLD / SELL recommendation with reasoning and target horizon.
"""

    return ollama_generate(prompt, model=model, system=system)


def analyze_technicals_ai(symbol: str, company_name: str, technicals: dict, model: str = "llama3.2") -> str:
    """AI-powered technical analysis."""

    system = """You are an expert technical analyst specializing in Indian stock markets.
You interpret technical indicators to provide actionable trading insights.
Be precise with support/resistance levels, entry/exit points, and stop-loss recommendations.
Always quantify your analysis with actual price levels."""

    ma = technicals.get("moving_averages", {})
    macd = technicals.get("macd", {})
    rsi = technicals.get("rsi", {})
    bb = technicals.get("bollinger_bands", {})
    stoch = technicals.get("stochastic", {})
    atr = technicals.get("atr", {})
    adx = technicals.get("adx", {})
    vol = technicals.get("volume", {})
    sr = technicals.get("support_resistance", {})
    pp = technicals.get("pivot_points", {})
    overall = technicals.get("overall_signal", {})

    prompt = f"""Technical Analysis Report for {company_name} ({symbol})

=== PRICE & MOVING AVERAGES ===
Current Price: ₹{ma.get('current_price', 'N/A')}
SMA 20: ₹{ma.get('sma_20', 'N/A')} | Price vs SMA20: {ma.get('price_vs_sma20', 'N/A')}
SMA 50: ₹{ma.get('sma_50', 'N/A')} | Price vs SMA50: {ma.get('price_vs_sma50', 'N/A')}
SMA 200: ₹{ma.get('sma_200', 'N/A')} | Price vs SMA200: {ma.get('price_vs_sma200', 'N/A')}
EMA 50: ₹{ma.get('ema_50', 'N/A')} | EMA 200: ₹{ma.get('ema_200', 'N/A')}
Golden Cross Active: {ma.get('golden_cross', 'N/A')}

=== MOMENTUM INDICATORS ===
RSI (14): {rsi.get('value', 'N/A')} → {rsi.get('signal', 'N/A')}
MACD: {macd.get('macd', 'N/A')} | Signal: {macd.get('signal', 'N/A')} | Histogram: {macd.get('histogram', 'N/A')}
MACD Status: {macd.get('crossover', 'N/A')}
Stochastic %K: {stoch.get('k', 'N/A')} | %D: {stoch.get('d', 'N/A')} → {stoch.get('signal', 'N/A')}

=== VOLATILITY ===
Bollinger Bands: Upper ₹{bb.get('upper', 'N/A')} | Mid ₹{bb.get('middle', 'N/A')} | Lower ₹{bb.get('lower', 'N/A')}
BB %B: {bb.get('percent_b', 'N/A')}% | BB Width: {bb.get('bandwidth', 'N/A')}%
BB Signal: {bb.get('signal', 'N/A')}
ATR (14): ₹{atr.get('value', 'N/A')} ({atr.get('percent', 'N/A')}% of price)

=== TREND STRENGTH ===
ADX: {adx.get('adx', 'N/A')} → {adx.get('trend_strength', 'N/A')}
+DI: {adx.get('plus_di', 'N/A')} | -DI: {adx.get('minus_di', 'N/A')} → {adx.get('direction', 'N/A')}
OBV Trend: {technicals.get('obv', {}).get('trend', 'N/A')}

=== VOLUME ===
Today's Volume: {vol.get('current', 'N/A'):,} | 20D Avg: {vol.get('avg_20d', 'N/A'):,}
Volume Signal: {vol.get('signal', 'N/A')} (Ratio: {vol.get('ratio', 'N/A')}x)

=== SUPPORT & RESISTANCE ===
Resistance Levels: {sr.get('resistance', [])}
Support Levels: {sr.get('support', [])}
52-Week High: ₹{sr.get('fifty_two_week_high', 'N/A')} | 52-Week Low: ₹{sr.get('fifty_two_week_low', 'N/A')}

=== PIVOT POINTS (Daily) ===
PP: ₹{pp.get('pivot', 'N/A')}
R1: ₹{pp.get('r1', 'N/A')} | R2: ₹{pp.get('r2', 'N/A')} | R3: ₹{pp.get('r3', 'N/A')}
S1: ₹{pp.get('s1', 'N/A')} | S2: ₹{pp.get('s2', 'N/A')} | S3: ₹{pp.get('s3', 'N/A')}

=== OVERALL SIGNAL ===
Signal: {overall.get('signal', 'N/A')} (Confidence: {overall.get('confidence', 'N/A')}%)
Buy Score: {overall.get('buy_score', 0)} | Sell Score: {overall.get('sell_score', 0)}

Please provide:
1. **Trend Analysis** — Primary and secondary trend direction, strength, and phase.
2. **Momentum Assessment** — RSI, MACD, and Stochastic confluence.
3. **Support & Resistance** — Key price levels to watch, breakout/breakdown zones.
4. **Volume Confirmation** — Is volume supporting the price action?
5. **Trade Setup** — 
   - Entry zone (ideal price range to enter)
   - Stop Loss (with reasoning based on ATR/support)
   - Target 1 (short-term, 1-2 weeks)
   - Target 2 (medium-term, 1-3 months)
6. **Risk/Reward Assessment** — Favorable or unfavorable setup?
7. **Summary** — Overall technical outlook: BULLISH / BEARISH / SIDEWAYS
"""

    return ollama_generate(prompt, model=model, system=system)


def combined_ai_analysis(symbol: str, company_name: str, fundamentals: dict, technicals: dict,
                          stock_info: dict, model: str = "llama3.2") -> str:
    """Combined fundamental + technical AI analysis."""

    system = """You are a top-tier equity research analyst with expertise in both fundamental analysis
and technical analysis for Indian markets (NSE/BSE). You provide holistic investment recommendations
combining value investing principles with technical timing. Your analysis is actionable, specific,
and suitable for both short-term traders and long-term investors."""

    overall_tech = technicals.get("overall_signal", {})
    val = fundamentals.get("valuation", {})
    prof = fundamentals.get("profitability", {})
    analyst = fundamentals.get("analyst", {})
    ma = technicals.get("moving_averages", {})
    rsi = technicals.get("rsi", {})

    prompt = f"""Provide a comprehensive investment analysis for {company_name} ({symbol}).

KEY METRICS SUMMARY:
Current Price: ₹{ma.get('current_price', stock_info.get('current_price', 'N/A'))}
Sector: {stock_info.get('sector', 'N/A')} | Industry: {stock_info.get('industry', 'N/A')}

FUNDAMENTALS SNAPSHOT:
- P/E: {val.get('pe_ratio', 'N/A')} | P/B: {val.get('pb_ratio', 'N/A')}
- ROE: {round(float(prof.get('roe') or 0) * 100, 1)}% | Net Margin: {round(float(prof.get('profit_margin') or 0) * 100, 1)}%
- D/E Ratio: {fundamentals.get('financial_health', {}).get('debt_to_equity', 'N/A')}
- Analyst View: {analyst.get('recommendation', 'N/A').upper()} | Target: ₹{analyst.get('target_mean', 'N/A')}

TECHNICALS SNAPSHOT:
- Technical Signal: {overall_tech.get('signal', 'N/A')} (Confidence: {overall_tech.get('confidence', 'N/A')}%)
- RSI: {rsi.get('value', 'N/A')} ({rsi.get('signal', 'N/A')})
- Price vs SMA200: {ma.get('price_vs_sma200', 'N/A')}
- Trend: {technicals.get('adx', {}).get('direction', 'N/A')} | Strength: {technicals.get('adx', {}).get('trend_strength', 'N/A')}

Please write a comprehensive research note covering:

## Executive Summary
One paragraph overview with clear BUY/HOLD/SELL stance.

## Business Quality Assessment  
Evaluate the fundamental quality of this business.

## Valuation
Is the stock cheap or expensive? What's fair value?

## Technical Timing
Is now a good time to enter? What do charts suggest?

## Catalysts & Risks
Key upcoming catalysts that could move the stock. Major risks.

## Investment Recommendation
- **Stance**: BUY / HOLD / SELL / AVOID
- **Investment Horizon**: Short-term (weeks) / Medium-term (months) / Long-term (years)
- **Entry Range**: ₹___ to ₹___
- **Stop Loss**: ₹___ (for traders)
- **Target Price**: ₹___ (6 months) | ₹___ (12 months)
- **Risk Level**: Low / Medium / High
- **Suitable For**: Long-term investors / Swing traders / Both
"""

    return ollama_generate(prompt, model=model, system=system)


def analyze_sentiment_ai(symbol: str, company_name: str, news_data: dict, model: str = "llama3.2") -> str:
    """AI-powered sentiment analysis from aggregated news."""
    from backend.news_sentiment import build_sentiment_prompt

    system = """You are a senior equity research analyst at a top Indian brokerage house.
You specialize in news flow analysis, event-driven trading, and market sentiment for NSE/BSE stocks.
You synthesize news from multiple sources into actionable investment insights.
Be specific, cite source types, and clearly quantify confidence in your sentiment verdict."""

    prompt = build_sentiment_prompt(symbol, company_name, news_data)
    return ollama_generate(prompt, model=model, system=system)
