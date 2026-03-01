import yfinance as yf
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime, timedelta
from backend.database import cache_get, cache_set

logger = logging.getLogger(__name__)


def _retry_yf_call(func, max_retries=3, base_delay=2):
    """Retry a yfinance call with exponential backoff on 429 rate-limit errors."""
    last_exception = None
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            err_str = str(e)
            # Check for rate-limiting (429) errors
            if "429" in err_str or "Too Many Requests" in err_str:
                last_exception = e
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Yahoo Finance rate-limited (attempt {attempt + 1}/{max_retries}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise  # Non-rate-limit errors should propagate immediately
    raise last_exception


def get_ticker_symbol(symbol: str, exchange: str) -> str:
    """Convert symbol to yfinance format for Indian markets."""
    symbol = symbol.upper().strip()
    if exchange.upper() == "NSE":
        return f"{symbol}.NS"
    elif exchange.upper() == "BSE":
        return f"{symbol}.BO"
    return symbol


def get_stock_info(symbol: str, exchange: str) -> dict:
    """Fetch comprehensive stock info with caching."""
    cached = cache_get(symbol, exchange, "info")
    if cached:
        return cached

    ticker_sym = get_ticker_symbol(symbol, exchange)
    ticker = yf.Ticker(ticker_sym)

    try:
        info = _retry_yf_call(lambda: ticker.info)
        if not info or info.get("regularMarketPrice") is None:
            # Try opposite exchange
            alt_exchange = "BSE" if exchange == "NSE" else "NSE"
            alt_sym = get_ticker_symbol(symbol, alt_exchange)
            alt_ticker = yf.Ticker(alt_sym)
            info = _retry_yf_call(lambda: alt_ticker.info)

        data = {
            "symbol": symbol,
            "exchange": exchange,
            "ticker_symbol": ticker_sym,
            "company_name": info.get("longName", info.get("shortName", symbol)),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "website": info.get("website", ""),
            "description": info.get("longBusinessSummary", ""),
            "current_price": info.get("currentPrice") or info.get("regularMarketPrice", 0),
            "previous_close": info.get("previousClose", 0),
            "open": info.get("open", 0),
            "day_high": info.get("dayHigh", 0),
            "day_low": info.get("dayLow", 0),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh", 0),
            "fifty_two_week_low": info.get("fiftyTwoWeekLow", 0),
            "volume": info.get("volume", 0),
            "avg_volume": info.get("averageVolume", 0),
            "market_cap": info.get("marketCap", 0),
            "currency": info.get("currency", "INR"),
            "exchange_info": info.get("exchange", exchange),
        }

        cache_set(symbol, exchange, "info", data, ttl_hours=1)
        return data

    except Exception as e:
        return {"error": str(e), "symbol": symbol, "exchange": exchange}


def get_price_history(symbol: str, exchange: str, period: str = "1y") -> pd.DataFrame:
    """Fetch OHLCV historical data."""
    ticker_sym = get_ticker_symbol(symbol, exchange)
    ticker = yf.Ticker(ticker_sym)

    try:
        hist = _retry_yf_call(lambda: ticker.history(period=period, interval="1d"))
        if hist.empty:
            alt_exchange = "BSE" if exchange == "NSE" else "NSE"
            alt_sym = get_ticker_symbol(symbol, alt_exchange)
            alt_ticker = yf.Ticker(alt_sym)
            hist = _retry_yf_call(lambda: alt_ticker.history(period=period, interval="1d"))

        hist.index = hist.index.tz_localize(None)
        return hist
    except Exception as e:
        return pd.DataFrame()


def get_fundamentals(symbol: str, exchange: str) -> dict:
    """Fetch detailed fundamental data."""
    cached = cache_get(symbol, exchange, "fundamentals")
    if cached:
        return cached

    ticker_sym = get_ticker_symbol(symbol, exchange)
    ticker = yf.Ticker(ticker_sym)

    try:
        info = _retry_yf_call(lambda: ticker.info)

        # Financial statements
        try:
            income_stmt = ticker.income_stmt
            balance_sheet = ticker.balance_sheet
            cashflow = ticker.cashflow
        except Exception:
            income_stmt = pd.DataFrame()
            balance_sheet = pd.DataFrame()
            cashflow = pd.DataFrame()

        def safe_get(d, key, default=None):
            val = d.get(key, default)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return default
            return val

        def df_val(df, row, col_idx=0):
            try:
                if df.empty or row not in df.index:
                    return None
                val = df.loc[row].iloc[col_idx]
                if pd.isna(val):
                    return None
                return float(val)
            except Exception:
                return None

        # Valuation ratios
        pe_ratio = safe_get(info, "trailingPE")
        forward_pe = safe_get(info, "forwardPE")
        pb_ratio = safe_get(info, "priceToBook")
        ps_ratio = safe_get(info, "priceToSalesTrailing12Months")
        ev_ebitda = safe_get(info, "enterpriseToEbitda")
        ev_revenue = safe_get(info, "enterpriseToRevenue")

        # Profitability
        roe = safe_get(info, "returnOnEquity")
        roa = safe_get(info, "returnOnAssets")
        profit_margin = safe_get(info, "profitMargins")
        operating_margin = safe_get(info, "operatingMargins")
        gross_margin = safe_get(info, "grossMargins")

        # Growth
        revenue_growth = safe_get(info, "revenueGrowth")
        earnings_growth = safe_get(info, "earningsGrowth")
        earnings_quarterly_growth = safe_get(info, "earningsQuarterlyGrowth")

        # Debt & Liquidity
        debt_to_equity = safe_get(info, "debtToEquity")
        current_ratio = safe_get(info, "currentRatio")
        quick_ratio = safe_get(info, "quickRatio")
        total_debt = safe_get(info, "totalDebt")
        free_cashflow = safe_get(info, "freeCashflow")

        # Per share metrics
        eps = safe_get(info, "trailingEps")
        forward_eps = safe_get(info, "forwardEps")
        book_value = safe_get(info, "bookValue")
        dividend_yield = safe_get(info, "dividendYield")
        dividend_rate = safe_get(info, "dividendRate")
        payout_ratio = safe_get(info, "payoutRatio")

        # Size metrics
        total_revenue = safe_get(info, "totalRevenue")
        ebitda = safe_get(info, "ebitda")
        total_assets = safe_get(info, "totalAssets")
        total_stockholders_equity = safe_get(info, "totalStockholderEquity")

        # Income statement (annual)
        revenue_history = []
        eps_history = []
        try:
            if not income_stmt.empty:
                for col in income_stmt.columns[:4]:
                    rev = df_val(income_stmt, "Total Revenue", list(income_stmt.columns).index(col))
                    net_income = df_val(income_stmt, "Net Income", list(income_stmt.columns).index(col))
                    revenue_history.append({
                        "period": str(col.date() if hasattr(col, 'date') else col),
                        "revenue": rev,
                        "net_income": net_income
                    })
        except Exception:
            pass

        data = {
            "valuation": {
                "pe_ratio": pe_ratio,
                "forward_pe": forward_pe,
                "pb_ratio": pb_ratio,
                "ps_ratio": ps_ratio,
                "ev_ebitda": ev_ebitda,
                "ev_revenue": ev_revenue,
            },
            "profitability": {
                "roe": roe,
                "roa": roa,
                "profit_margin": profit_margin,
                "operating_margin": operating_margin,
                "gross_margin": gross_margin,
            },
            "growth": {
                "revenue_growth": revenue_growth,
                "earnings_growth": earnings_growth,
                "earnings_quarterly_growth": earnings_quarterly_growth,
            },
            "financial_health": {
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
                "total_debt": total_debt,
                "free_cashflow": free_cashflow,
            },
            "per_share": {
                "eps": eps,
                "forward_eps": forward_eps,
                "book_value": book_value,
                "dividend_yield": dividend_yield,
                "dividend_rate": dividend_rate,
                "payout_ratio": payout_ratio,
            },
            "size": {
                "total_revenue": total_revenue,
                "ebitda": ebitda,
                "total_assets": total_assets,
                "market_cap": safe_get(info, "marketCap"),
                "enterprise_value": safe_get(info, "enterpriseValue"),
            },
            "history": revenue_history,
            "analyst": {
                "target_high": safe_get(info, "targetHighPrice"),
                "target_low": safe_get(info, "targetLowPrice"),
                "target_mean": safe_get(info, "targetMeanPrice"),
                "target_median": safe_get(info, "targetMedianPrice"),
                "recommendation": safe_get(info, "recommendationKey"),
                "number_of_analyst_opinions": safe_get(info, "numberOfAnalystOpinions"),
            }
        }

        cache_set(symbol, exchange, "fundamentals", data, ttl_hours=12)
        return data

    except Exception as e:
        return {"error": str(e)}


def history_to_list(df: pd.DataFrame) -> list:
    """Convert DataFrame to list of dicts for JSON."""
    if df.empty:
        return []
    records = []
    for idx, row in df.iterrows():
        records.append({
            "date": str(idx.date()),
            "open": round(float(row.get("Open", 0)), 2),
            "high": round(float(row.get("High", 0)), 2),
            "low": round(float(row.get("Low", 0)), 2),
            "close": round(float(row.get("Close", 0)), 2),
            "volume": int(row.get("Volume", 0)),
        })
    return records
