from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from pathlib import Path

from backend.database import (
    init_db, save_analysis, get_analysis_history,
    get_watchlist, add_to_watchlist, remove_from_watchlist,
    save_sentiment, get_sentiment_history,
)
from backend.ticker_db import init_ticker_db, search_tickers_with_yfinance_fallback, get_ticker_count
from backend.stock_data import get_stock_info, get_fundamentals, get_price_history, history_to_list
from backend.technicals import compute_technicals
from backend.ai_analysis import (
    analyze_fundamentals_ai, analyze_technicals_ai,
    combined_ai_analysis, get_available_models,
    analyze_sentiment_ai,
)
from backend.news_sentiment import fetch_all_news
from backend.alphavantage import (
    # Core
    get_global_quote, get_daily_ohlcv, get_intraday_ohlcv, search_symbol,
    # Fundamentals
    get_company_overview, get_income_statement, get_balance_sheet,
    get_cash_flow, get_earnings, get_dividends, get_splits,
    get_earnings_calendar, get_full_av_data,
    # Alpha Intelligence
    get_news_sentiment,
    # Technicals
    get_av_technicals, get_rsi, get_macd, get_bbands,
    get_sma, get_adx, get_stoch, get_cci, get_aroon, get_obv,
    # Macro
    get_usd_inr, get_economic_indicator,
    # Util
    get_api_usage, VANTAGE_KEY,
)

init_db()
init_ticker_db()

app = FastAPI(title="Indian Stock Analyzer — Alpha Vantage Edition", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


# ── Request Models ────────────────────────────────────────────────────────────

class AIAnalysisRequest(BaseModel):
    symbol: str
    exchange: str = "NSE"
    analysis_type: str = "combined"
    model: str = "gemma3n:e4b"


class WatchlistRequest(BaseModel):
    symbol: str
    exchange: str = "NSE"
    notes: Optional[str] = None


# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=(FRONTEND_DIR / "index.html").read_text(encoding="utf-8"))


# ── TICKER SEARCH ─────────────────────────────────────────────────────────────

@app.get("/api/search/tickers")
async def search_tickers_api(q: str = Query("", description="Search query"), limit: int = 15):
    """Search for tickers by symbol or company name. Uses local DB + yfinance fallback."""
    if not q or len(q.strip()) < 1:
        return {"results": [], "total_tickers": get_ticker_count()}
    results = search_tickers_with_yfinance_fallback(q.strip(), limit=limit)
    return {"results": results, "total_tickers": get_ticker_count()}


@app.get("/api/health")
async def health():
    av_status = {
        "configured": VANTAGE_KEY != "VANTAGE_KEY",
        "usage": get_api_usage(),
    }
    return {"status": "ok", "version": "3.0.0", "alphavantage": av_status}


@app.get("/api/models")
async def list_models():
    return {"models": get_available_models()}


# ── YFINANCE STOCK DATA (existing) ────────────────────────────────────────────

@app.get("/api/stock/{exchange}/{symbol}")
async def stock_overview(exchange: str, symbol: str):
    info = get_stock_info(symbol.upper(), exchange.upper())
    err = info.get("error", "")
    if err:
        if "429" in err or "Too Many Requests" in err:
            raise HTTPException(status_code=429, detail="Yahoo Finance rate limit exceeded. Please wait a moment and try again.")
        if not info.get("company_name"):
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found on {exchange}")
    return {"info": info}


@app.get("/api/stock/{exchange}/{symbol}/fundamentals")
async def stock_fundamentals(exchange: str, symbol: str):
    data = get_fundamentals(symbol.upper(), exchange.upper())
    if "error" in data:
        raise HTTPException(status_code=404, detail=data["error"])
    return data


@app.get("/api/stock/{exchange}/{symbol}/technicals")
async def stock_technicals(exchange: str, symbol: str):
    data = compute_technicals(symbol.upper(), exchange.upper())
    if "error" in data:
        raise HTTPException(status_code=404, detail=data["error"])
    return data


@app.get("/api/stock/{exchange}/{symbol}/history")
async def stock_history(exchange: str, symbol: str, period: str = "1y"):
    if period not in ["1mo", "3mo", "6mo", "1y", "2y", "5y"]:
        period = "1y"
    df = get_price_history(symbol.upper(), exchange.upper(), period=period)
    if df.empty:
        raise HTTPException(status_code=404, detail="No price history found")
    return {"data": history_to_list(df), "period": period}


# ── ALPHA VANTAGE: STATUS & QUOTA ─────────────────────────────────────────────

@app.get("/api/alphavantage/status")
async def av_status():
    """Check API key status and daily quota usage."""
    usage = get_api_usage()
    return {
        "key_configured": VANTAGE_KEY != "VANTAGE_KEY",
        "key_placeholder": VANTAGE_KEY == "VANTAGE_KEY",
        "usage": usage,
        "free_tier_limit": 25,
        "note": "Replace VANTAGE_KEY in backend/alphavantage.py with your real key from alphavantage.co",
    }


# ── ALPHA VANTAGE: CORE DATA ──────────────────────────────────────────────────

@app.get("/api/alphavantage/{exchange}/{symbol}/quote")
async def av_quote(exchange: str, symbol: str):
    """Real-time quote from Alpha Vantage."""
    return get_global_quote(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/daily")
async def av_daily(exchange: str, symbol: str, size: str = "compact"):
    """Daily OHLCV from Alpha Vantage (compact=100 days, full=20yr)."""
    return get_daily_ohlcv(symbol.upper(), exchange.upper(), outputsize=size)


@app.get("/api/alphavantage/{exchange}/{symbol}/intraday")
async def av_intraday(exchange: str, symbol: str, interval: str = "5min", size: str = "compact"):
    """Intraday OHLCV from Alpha Vantage."""
    return get_intraday_ohlcv(symbol.upper(), exchange.upper(), interval=interval, outputsize=size)


@app.get("/api/alphavantage/search")
async def av_search(q: str = Query(..., description="Search keywords")):
    """Symbol search via Alpha Vantage."""
    return {"results": search_symbol(q)}


# ── ALPHA VANTAGE: FUNDAMENTAL DATA ──────────────────────────────────────────

@app.get("/api/alphavantage/{exchange}/{symbol}/overview")
async def av_overview(exchange: str, symbol: str):
    """Company overview + 50 financial ratios from Alpha Vantage."""
    return get_company_overview(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/income")
async def av_income(exchange: str, symbol: str):
    """Income statement (5 years annual + quarterly)."""
    return get_income_statement(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/balance")
async def av_balance(exchange: str, symbol: str):
    """Balance sheet (5 years annual + quarterly)."""
    return get_balance_sheet(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/cashflow")
async def av_cashflow(exchange: str, symbol: str):
    """Cash flow statement (5 years annual + quarterly)."""
    return get_cash_flow(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/earnings")
async def av_earnings(exchange: str, symbol: str):
    """EPS history with analyst estimates & earnings surprise %."""
    return get_earnings(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/dividends")
async def av_dividends(exchange: str, symbol: str):
    return {"dividends": get_dividends(symbol.upper(), exchange.upper())}


@app.get("/api/alphavantage/{exchange}/{symbol}/splits")
async def av_splits(exchange: str, symbol: str):
    return {"splits": get_splits(symbol.upper(), exchange.upper())}


@app.get("/api/alphavantage/{exchange}/{symbol}/earnings-calendar")
async def av_earnings_calendar(exchange: str, symbol: str):
    return {"upcoming_earnings": get_earnings_calendar(symbol.upper(), exchange.upper())}


@app.get("/api/alphavantage/{exchange}/{symbol}/full-fundamentals")
async def av_full_fundamentals(exchange: str, symbol: str):
    """
    Fetch ALL fundamental data in one call (uses ~5 API credits).
    Returns: overview + income statement + balance sheet + cash flow + earnings + dividends + splits
    """
    return get_full_av_data(symbol.upper(), exchange.upper())


# ── ALPHA VANTAGE: ALPHA INTELLIGENCE ────────────────────────────────────────

@app.get("/api/alphavantage/{exchange}/{symbol}/news")
async def av_news_sentiment(exchange: str, symbol: str, limit: int = 50):
    """
    AI-powered news & sentiment from Alpha Vantage.
    Articles are scored with relevance to the specific ticker.
    """
    return get_news_sentiment(symbol.upper(), exchange.upper(), limit=limit)


# ── ALPHA VANTAGE: TECHNICAL INDICATORS ──────────────────────────────────────

@app.get("/api/alphavantage/{exchange}/{symbol}/technicals")
async def av_technicals_all(exchange: str, symbol: str):
    """All AV technical indicators: RSI, MACD, BBands, ADX, STOCH, CCI, AROON, OBV."""
    return get_av_technicals(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/rsi")
async def av_rsi(exchange: str, symbol: str, period: int = 14):
    return get_rsi(symbol.upper(), exchange.upper(), period=period)


@app.get("/api/alphavantage/{exchange}/{symbol}/macd")
async def av_macd(exchange: str, symbol: str):
    return get_macd(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/bbands")
async def av_bbands(exchange: str, symbol: str, period: int = 20):
    return get_bbands(symbol.upper(), exchange.upper(), period=period)


@app.get("/api/alphavantage/{exchange}/{symbol}/sma")
async def av_sma(exchange: str, symbol: str, period: int = 50):
    return get_sma(symbol.upper(), exchange.upper(), period=period)


@app.get("/api/alphavantage/{exchange}/{symbol}/adx")
async def av_adx_route(exchange: str, symbol: str, period: int = 14):
    return get_adx(symbol.upper(), exchange.upper(), period=period)


@app.get("/api/alphavantage/{exchange}/{symbol}/stoch")
async def av_stoch_route(exchange: str, symbol: str):
    return get_stoch(symbol.upper(), exchange.upper())


@app.get("/api/alphavantage/{exchange}/{symbol}/cci")
async def av_cci_route(exchange: str, symbol: str, period: int = 20):
    return get_cci(symbol.upper(), exchange.upper(), period=period)


@app.get("/api/alphavantage/{exchange}/{symbol}/aroon")
async def av_aroon_route(exchange: str, symbol: str, period: int = 25):
    return get_aroon(symbol.upper(), exchange.upper(), period=period)


@app.get("/api/alphavantage/{exchange}/{symbol}/obv")
async def av_obv_route(exchange: str, symbol: str):
    return get_obv(symbol.upper(), exchange.upper())


# ── ALPHA VANTAGE: MACRO & FOREX ─────────────────────────────────────────────

@app.get("/api/alphavantage/macro/usd-inr")
async def av_usd_inr():
    """Live USD/INR exchange rate."""
    return get_usd_inr()


@app.get("/api/alphavantage/macro/{indicator}")
async def av_macro(indicator: str):
    """
    Economic indicators: REAL_GDP, INFLATION, CPI, FEDERAL_FUNDS_RATE,
    UNEMPLOYMENT, RETAIL_SALES, CONSUMER_SENTIMENT, NONFARM_PAYROLL
    """
    allowed = [
        "REAL_GDP", "REAL_GDP_PER_CAPITA", "INFLATION", "CPI",
        "FEDERAL_FUNDS_RATE", "UNEMPLOYMENT", "RETAIL_SALES",
        "CONSUMER_SENTIMENT", "NONFARM_PAYROLL", "DURABLES"
    ]
    if indicator.upper() not in allowed:
        raise HTTPException(status_code=400, detail=f"Unknown indicator. Allowed: {allowed}")
    return get_economic_indicator(indicator.upper())


# ── NEWS & MULTI-SOURCE SENTIMENT ─────────────────────────────────────────────

@app.get("/api/stock/{exchange}/{symbol}/news")
async def stock_news(exchange: str, symbol: str, refresh: bool = False):
    sym, exc = symbol.upper(), exchange.upper()
    info = get_stock_info(sym, exc)
    company = info.get("company_name", sym)
    data = fetch_all_news(sym, exc, company, use_cache=not refresh)
    agg = data.get("aggregate", {})
    if agg.get("total", 0) > 0:
        save_sentiment(sym, exc, agg.get("label", "Neutral"), agg.get("score", 0),
                       article_count=agg.get("total", 0))
    return data


@app.get("/api/stock/{exchange}/{symbol}/sentiment-history")
async def sentiment_history_route(exchange: str, symbol: str):
    return {"history": get_sentiment_history(symbol.upper(), exchange.upper(), limit=30)}


# ── AI ANALYSIS ───────────────────────────────────────────────────────────────

@app.post("/api/analyze/ai")
async def ai_analyze(request: AIAnalysisRequest):
    sym, exc = request.symbol.upper(), request.exchange.upper()
    info = get_stock_info(sym, exc)
    name = info.get("company_name", sym)

    if request.analysis_type == "fundamental":
        funds = get_fundamentals(sym, exc)
        result = analyze_fundamentals_ai(sym, name, funds, request.model)
    elif request.analysis_type == "technical":
        techs = compute_technicals(sym, exc)
        result = analyze_technicals_ai(sym, name, techs, request.model)
    elif request.analysis_type == "sentiment":
        news_data = fetch_all_news(sym, exc, name, use_cache=True)
        result = analyze_sentiment_ai(sym, name, news_data, request.model)
        agg = news_data.get("aggregate", {})
        save_sentiment(sym, exc, agg.get("label", "N/A"), agg.get("score", 0),
                       ai_analysis=result, model=request.model,
                       article_count=agg.get("total", 0))
    else:
        funds = get_fundamentals(sym, exc)
        techs = compute_technicals(sym, exc)
        result = combined_ai_analysis(sym, name, funds, techs, info, request.model)

    save_analysis(sym, exc, request.analysis_type + "_ai", result, model=request.model)
    return {"analysis": result, "type": request.analysis_type, "symbol": sym}


@app.get("/api/analyze/ai/{exchange}/{symbol}")
async def get_cached_ai_analysis(exchange: str, symbol: str, analysis_type: str = "combined_ai"):
    """Get the most recent cached AI analysis for a stock."""
    sym, exc = symbol.upper(), exchange.upper()
    history = get_analysis_history(sym, exc, limit=5)
    # Filter by analysis_type if specified
    if analysis_type:
        matching = [h for h in history if h.get("analysis_type") == analysis_type]
        if matching:
            return {"cached": True, "data": matching[0]}
    # Return most recent of any type if no exact match
    if history:
        return {"cached": True, "data": history[0]}
    return {"cached": False, "data": None}


# ── WATCHLIST ─────────────────────────────────────────────────────────────────

@app.get("/api/watchlist")
async def list_watchlist():
    items = get_watchlist()
    enriched = []
    for item in items:
        info = get_stock_info(item["symbol"], item["exchange"])
        prev = info.get("previous_close") or 1
        curr = info.get("current_price") or 0
        enriched.append({
            **item,
            "current_price": curr,
            "company_name": info.get("company_name"),
            "change_pct": round((curr - prev) / prev * 100, 2) if prev else None
        })
    return {"watchlist": enriched}


@app.post("/api/watchlist")
async def add_watchlist(request: WatchlistRequest):
    success = add_to_watchlist(request.symbol.upper(), request.exchange.upper(), request.notes)
    return {"message": "Added to watchlist" if success else "Already in watchlist"}


@app.delete("/api/watchlist/{exchange}/{symbol}")
async def delete_watchlist(exchange: str, symbol: str):
    remove_from_watchlist(symbol.upper(), exchange.upper())
    return {"message": "Removed from watchlist"}


# ── POPULAR ───────────────────────────────────────────────────────────────────

@app.get("/api/popular")
async def popular_stocks():
    return {"stocks": [
        {"symbol": "RELIANCE",   "exchange": "NSE", "name": "Reliance Industries"},
        {"symbol": "TCS",        "exchange": "NSE", "name": "Tata Consultancy Services"},
        {"symbol": "HDFCBANK",   "exchange": "NSE", "name": "HDFC Bank"},
        {"symbol": "INFY",       "exchange": "NSE", "name": "Infosys"},
        {"symbol": "HINDUNILVR", "exchange": "NSE", "name": "Hindustan Unilever"},
        {"symbol": "ICICIBANK",  "exchange": "NSE", "name": "ICICI Bank"},
        {"symbol": "SBIN",       "exchange": "NSE", "name": "State Bank of India"},
        {"symbol": "WIPRO",      "exchange": "NSE", "name": "Wipro"},
        {"symbol": "BHARTIARTL", "exchange": "NSE", "name": "Bharti Airtel"},
        {"symbol": "ITC",        "exchange": "NSE", "name": "ITC Limited"},
        {"symbol": "TATAMOTORS", "exchange": "NSE", "name": "Tata Motors"},
        {"symbol": "KOTAKBANK",  "exchange": "NSE", "name": "Kotak Mahindra Bank"},
        {"symbol": "LT",         "exchange": "NSE", "name": "Larsen & Toubro"},
        {"symbol": "AXISBANK",   "exchange": "NSE", "name": "Axis Bank"},
        {"symbol": "SUNPHARMA",  "exchange": "NSE", "name": "Sun Pharma"},
        {"symbol": "TITAN",      "exchange": "NSE", "name": "Titan Company"},
        {"symbol": "BAJFINANCE", "exchange": "NSE", "name": "Bajaj Finance"},
        {"symbol": "ASIANPAINT", "exchange": "NSE", "name": "Asian Paints"},
        {"symbol": "MARUTI",     "exchange": "NSE", "name": "Maruti Suzuki"},
        {"symbol": "NESTLEIND",  "exchange": "NSE", "name": "Nestle India"},
    ]}
