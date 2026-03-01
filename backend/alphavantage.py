"""
Alpha Vantage API Integration for Indian Stock Analyzer
========================================================
Set your API key in the .app_keys file at the project root:
  ALPHA_VANTAGE_KEY=your_key_here

Get a free API key from: https://www.alphavantage.co/support/#api-key

Free tier limits: 25 requests per day, no intraday data.
This module caches aggressively (24h default) to preserve your daily quota.

Endpoints covered:
  ── Core Data ────────────────────────────────────────────────
  GLOBAL_QUOTE          Real-time / latest quote
  TIME_SERIES_DAILY     Daily OHLCV (up to 20 years)
  SYMBOL_SEARCH         Symbol search + exchange lookup

  ── Fundamental Data ─────────────────────────────────────────
  OVERVIEW              Company overview + 50+ financial ratios
  INCOME_STATEMENT      Annual & quarterly P&L (5 years)
  BALANCE_SHEET         Annual & quarterly balance sheet
  CASH_FLOW             Annual & quarterly cash flows
  EARNINGS              EPS history with analyst estimates & surprise %
  EARNINGS_CALENDAR     Upcoming earnings dates
  DIVIDENDS             Full dividend history
  SPLITS                Stock split history

  ── Alpha Intelligence ───────────────────────────────────────
  NEWS_SENTIMENT        AI-scored market news with relevance scores

  ── Technical Indicators (pre-computed) ──────────────────────
  RSI                   Relative Strength Index
  MACD                  MACD + Signal + Histogram
  BBANDS                Bollinger Bands
  SMA / EMA             Moving averages (any window)
  ADX                   Average Directional Index
  STOCH                 Stochastic Oscillator
  OBV                   On-Balance Volume
  CCI                   Commodity Channel Index
  AROON                 Aroon Indicator (momentum)

  ── Macro / Commodities ──────────────────────────────────────
  REAL_GDP              India-relevant macro (global proxy)
  INFLATION             CPI / Inflation
  FEDERAL_FUNDS_RATE    US rate (impacts FII flows)
  CURRENCY_EXCHANGE_RATE  USD/INR live rate
"""

import json
import os
import urllib.request
import urllib.parse
import urllib.error
import csv
import io
from datetime import datetime, timezone, date
from typing import Optional
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────

def _load_env_file():
    """Load .app_keys file from project root if it exists."""
    env_path = Path(__file__).parent.parent / ".app_keys"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value

_load_env_file()

VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"
TIMEOUT = 15

# Cache TTLs (hours) — tune to preserve your 25 req/day quota
TTL = {
    "quote":         1,     # price data — refresh hourly
    "daily_ohlcv":   24,    # historical prices
    "overview":      24,    # fundamentals rarely change intraday
    "income":        48,    # financials update quarterly
    "balance":       48,
    "cashflow":      48,
    "earnings":      24,    # EPS history
    "earnings_cal":  12,    # upcoming earnings
    "dividends":     48,
    "splits":        72,
    "news":          1,     # news is time-sensitive
    "technical":     4,     # indicators
    "macro":         24,    # economic indicators
    "fx":            1,     # exchange rates
}

# ── DAILY QUOTA TRACKER ───────────────────────────────────────────────────────

_call_count_today = {"date": None, "count": 0}


def _track_call():
    """Track API calls; raise if over free-tier limit."""
    today = date.today().isoformat()
    if _call_count_today["date"] != today:
        _call_count_today["date"] = today
        _call_count_today["count"] = 0
    _call_count_today["count"] += 1
    return _call_count_today["count"]


def get_api_usage() -> dict:
    today = date.today().isoformat()
    if _call_count_today["date"] != today:
        return {"date": today, "calls_today": 0, "remaining": 25}
    used = _call_count_today["count"]
    return {
        "date": today,
        "calls_today": used,
        "remaining": max(0, 25 - used),
        "key_configured": VANTAGE_KEY != "VANTAGE_KEY",
    }


# ── HTTP + CACHE HELPERS ──────────────────────────────────────────────────────

def _av_get(params: dict, cache_key_parts: list, ttl_hours: int) -> dict:
    """
    Fetch from Alpha Vantage with caching.
    cache_key_parts: list of strings that form a unique cache key.
    """
    from backend.database import cache_get, cache_set

    # Build cache key (use data_type field)
    cache_sym = cache_key_parts[0] if cache_key_parts else "AV"
    cache_exc = "AV"
    cache_dt = "_".join(str(p) for p in cache_key_parts)

    cached = cache_get(cache_sym, cache_exc, cache_dt)
    if cached:
        return {**cached, "_from_cache": True}

    if VANTAGE_KEY == "VANTAGE_KEY":
        return {"error": "Alpha Vantage API key not configured. Set VANTAGE_KEY in backend/alphavantage.py"}

    params["apikey"] = VANTAGE_KEY
    url = BASE_URL + "?" + urllib.parse.urlencode(params)

    call_num = _track_call()

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "StockAnalyzer/2.0 (+github.com/stock-analyzer)"}
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)

            # Alpha Vantage error/info responses
            if "Error Message" in data:
                return {"error": data["Error Message"]}
            if "Note" in data:
                return {"error": "API rate limit reached. Free tier allows 25 requests/day.", "rate_limited": True}
            if "Information" in data:
                # E.g. Premium required or API limit
                err_msg = data["Information"]
                is_rate_limit = "rate limit" in err_msg.lower() or "limit" in err_msg.lower()
                return {"error": err_msg, "rate_limited": is_rate_limit, "premium_required": "premium" in err_msg.lower()}
            
            if not data:
                return {"error": "Alpha Vantage returned empty data. This indicator or stock (e.g. non-US) may require a premium API key."}

            data["_fetched_at"] = datetime.now(timezone.utc).isoformat()
            data["_api_call_num"] = call_num
            cache_set(cache_sym, cache_exc, cache_dt, data, ttl_hours=ttl_hours)
            return data

    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


def _av_get_csv(params: dict, cache_key_parts: list, ttl_hours: int) -> list:
    """Fetch CSV response (used by EARNINGS_CALENDAR)."""
    from backend.database import cache_get, cache_set

    cache_sym = cache_key_parts[0] if cache_key_parts else "AV"
    cache_dt = "_".join(str(p) for p in cache_key_parts)

    cached = cache_get(cache_sym, "AV", cache_dt)
    if cached:
        return cached

    if VANTAGE_KEY == "VANTAGE_KEY":
        return []

    params["apikey"] = VANTAGE_KEY
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    _track_call()

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "StockAnalyzer/2.0"})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(raw))
            rows = [dict(r) for r in reader]
            cache_set(cache_sym, "AV", cache_dt, rows, ttl_hours=ttl_hours)
            return rows
    except Exception:
        return []


def _nse_to_av(symbol: str, exchange: str) -> str:
    """
    Convert NSE/BSE symbol to Alpha Vantage format.
    Alpha Vantage supports Indian stocks via the .BSE standard suffix:
      e.g. 500180.BSE for HDFCBANK, 500325.BSE for RELIANCE
    For NSE symbols, we look up the BSE scrip code from the ticker DB.
    Falls back to SYMBOL.BSE or SYMBOL.NSE if no BSE code found.
    """
    sym = symbol.upper().strip()
    exc = exchange.upper().strip()

    try:
        from backend.ticker_db import get_bse_code
        bse_code = get_bse_code(sym, exc)
        if bse_code:
            return f"{bse_code}.BSE"
    except Exception:
        pass

    # Fallback if ticker DB not available or no BSE code found
    if exc == "BSE":
        return f"{sym}.BSE"
    return f"{sym}.NSE"


def _safe(val, default=None):
    """Return None if value is 'None' string or empty."""
    if val in (None, "None", "N/A", "-", ""):
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return val


def _safe_float(val) -> Optional[float]:
    try:
        v = float(val)
        return None if v == 0 else v
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  CORE QUOTE & PRICE
# ─────────────────────────────────────────────────────────────────────────────

def get_global_quote(symbol: str, exchange: str) -> dict:
    """Latest price, change, volume from Alpha Vantage GLOBAL_QUOTE."""
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "GLOBAL_QUOTE", "symbol": av_sym},
        [symbol, "AV_QUOTE"],
        TTL["quote"]
    )
    if "error" in data:
        return data

    q = data.get("Global Quote", {})
    return {
        "symbol": q.get("01. symbol", av_sym),
        "open": _safe_float(q.get("02. open")),
        "high": _safe_float(q.get("03. high")),
        "low": _safe_float(q.get("04. low")),
        "price": _safe_float(q.get("05. price")),
        "volume": _safe_float(q.get("06. volume")),
        "latest_trading_day": q.get("07. latest trading day"),
        "previous_close": _safe_float(q.get("08. previous close")),
        "change": _safe_float(q.get("09. change")),
        "change_pct": q.get("10. change percent", "").replace("%", "").strip(),
        "_fetched_at": data.get("_fetched_at"),
        "_from_cache": data.get("_from_cache", False),
    }


def get_daily_ohlcv(symbol: str, exchange: str, outputsize: str = "compact") -> dict:
    """
    Daily OHLCV time series. 
    outputsize='compact' → last 100 days (saves quota)
    outputsize='full'    → 20+ years
    """
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "TIME_SERIES_DAILY", "symbol": av_sym,
         "outputsize": outputsize, "datatype": "json"},
        [symbol, f"AV_DAILY_{outputsize}"],
        TTL["daily_ohlcv"]
    )
    if "error" in data:
        return data

    series = data.get("Time Series (Daily)", {})
    records = []
    for dt_str, ohlcv in sorted(series.items(), reverse=True):
        records.append({
            "date": dt_str,
            "open":   _safe_float(ohlcv.get("1. open")),
            "high":   _safe_float(ohlcv.get("2. high")),
            "low":    _safe_float(ohlcv.get("3. low")),
            "close":  _safe_float(ohlcv.get("4. close")),
            "volume": _safe_float(ohlcv.get("5. volume")),
        })
    return {
        "symbol": av_sym,
        "records": records,
        "count": len(records),
        "meta": data.get("Meta Data", {}),
        "_fetched_at": data.get("_fetched_at"),
    }

def get_intraday_ohlcv(symbol: str, exchange: str, interval: str = "5min", outputsize: str = "compact") -> dict:
    """
    Intraday OHLCV time series.
    interval: '1min', '5min', '15min', '30min', '60min'
    """
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "TIME_SERIES_INTRADAY", "symbol": av_sym,
         "interval": interval, "outputsize": outputsize, "datatype": "json"},
        [symbol, f"AV_INTRA_{interval}_{outputsize}"],
        TTL["daily_ohlcv"]  # reuse same TTL or configure one
    )
    if "error" in data:
        return data

    series_key = f"Time Series ({interval})"
    series = data.get(series_key, {})
    records = []
    for dt_str, ohlcv in sorted(series.items(), reverse=True):
        records.append({
            "date": dt_str,
            "open":   _safe_float(ohlcv.get("1. open")),
            "high":   _safe_float(ohlcv.get("2. high")),
            "low":    _safe_float(ohlcv.get("3. low")),
            "close":  _safe_float(ohlcv.get("4. close")),
            "volume": _safe_float(ohlcv.get("5. volume")),
        })
    return {
        "symbol": av_sym,
        "records": records,
        "count": len(records),
        "meta": data.get("Meta Data", {}),
        "_fetched_at": data.get("_fetched_at"),
    }


def search_symbol(keywords: str) -> list:
    """Search for ticker symbols — useful to verify NSE/BSE symbol existence."""
    data = _av_get(
        {"function": "SYMBOL_SEARCH", "keywords": keywords},
        ["SEARCH", keywords[:20]],
        ttl_hours=24
    )
    if "error" in data:
        return []
    return data.get("bestMatches", [])


# ─────────────────────────────────────────────────────────────────────────────
#  FUNDAMENTAL DATA
# ─────────────────────────────────────────────────────────────────────────────

def get_company_overview(symbol: str, exchange: str) -> dict:
    """
    OVERVIEW endpoint: 50+ fundamental metrics in one call.
    Includes: P/E, EPS, revenue, profit margins, beta, 52W high/low, market cap,
              dividend yield, book value, ROE, ROA, D/E, analyst targets, etc.
    """
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "OVERVIEW", "symbol": av_sym},
        [symbol, "AV_OVERVIEW"],
        TTL["overview"]
    )
    if "error" in data:
        return data

    def sf(key): return _safe_float(data.get(key))
    def ss(key): return _safe(data.get(key), "N/A")

    return {
        # Identity
        "symbol":               data.get("Symbol"),
        "name":                 data.get("Name"),
        "description":          data.get("Description", ""),
        "cik":                  data.get("CIK"),
        "exchange":             data.get("Exchange"),
        "currency":             data.get("Currency"),
        "country":              data.get("Country"),
        "sector":               data.get("Sector"),
        "industry":             data.get("Industry"),
        "address":              data.get("Address"),
        "fiscal_year_end":      data.get("FiscalYearEnd"),
        "latest_quarter":       data.get("LatestQuarter"),

        # Valuation
        "market_cap":           sf("MarketCapitalization"),
        "ebitda":               sf("EBITDA"),
        "pe_ratio":             sf("PERatio"),
        "peg_ratio":            sf("PEGRatio"),
        "book_value":           sf("BookValue"),
        "dividend_per_share":   sf("DividendPerShare"),
        "dividend_yield":       sf("DividendYield"),
        "eps":                  sf("EPS"),
        "revenue_per_share_ttm": sf("RevenuePerShareTTM"),
        "profit_margin":        sf("ProfitMargin"),
        "operating_margin_ttm": sf("OperatingMarginTTM"),
        "return_on_assets_ttm": sf("ReturnOnAssetsTTM"),
        "return_on_equity_ttm": sf("ReturnOnEquityTTM"),
        "revenue_ttm":          sf("RevenueTTM"),
        "gross_profit_ttm":     sf("GrossProfitTTM"),
        "diluted_eps_ttm":      sf("DilutedEPSTTM"),
        "qtrly_earnings_growth_yoy": sf("QuarterlyEarningsGrowthYOY"),
        "qtrly_revenue_growth_yoy":  sf("QuarterlyRevenueGrowthYOY"),
        "analyst_target_price": sf("AnalystTargetPrice"),
        "analyst_rating_strong_buy": ss("AnalystRatingStrongBuy"),
        "analyst_rating_buy":   ss("AnalystRatingBuy"),
        "analyst_rating_hold":  ss("AnalystRatingHold"),
        "analyst_rating_sell":  ss("AnalystRatingSell"),
        "analyst_rating_strong_sell": ss("AnalystRatingStrongSell"),
        "trailing_pe":          sf("TrailingPE"),
        "forward_pe":           sf("ForwardPE"),
        "price_to_sales_ttm":   sf("PriceToSalesRatioTTM"),
        "price_to_book":        sf("PriceToBookRatio"),
        "ev_to_revenue":        sf("EVToRevenue"),
        "ev_to_ebitda":         sf("EVToEBITDA"),
        "beta":                 sf("Beta"),
        "52_week_high":         sf("52WeekHigh"),
        "52_week_low":          sf("52WeekLow"),
        "50_day_ma":            sf("50DayMovingAverage"),
        "200_day_ma":           sf("200DayMovingAverage"),
        "shares_outstanding":   sf("SharesOutstanding"),
        "shares_float":         sf("SharesFloat"),
        "shares_short":         sf("SharesShort"),
        "short_ratio":          sf("ShortRatio"),
        "dividend_date":        ss("DividendDate"),
        "ex_dividend_date":     ss("ExDividendDate"),

        "_fetched_at":          data.get("_fetched_at"),
        "_from_cache":          data.get("_from_cache", False),
    }


def get_income_statement(symbol: str, exchange: str) -> dict:
    """Annual and quarterly income statements (5 years)."""
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "INCOME_STATEMENT", "symbol": av_sym},
        [symbol, "AV_INCOME"],
        TTL["income"]
    )
    if "error" in data:
        return data

    def parse_reports(reports: list) -> list:
        out = []
        for r in reports[:8]:  # last 8 periods
            out.append({
                "period":               r.get("fiscalDateEnding"),
                "currency":             r.get("reportedCurrency"),
                "total_revenue":        _safe_float(r.get("totalRevenue")),
                "gross_profit":         _safe_float(r.get("grossProfit")),
                "cost_of_revenue":      _safe_float(r.get("costOfRevenue")),
                "operating_income":     _safe_float(r.get("operatingIncome")),
                "ebit":                 _safe_float(r.get("ebit")),
                "ebitda":               _safe_float(r.get("ebitda")),
                "net_income":           _safe_float(r.get("netIncome")),
                "net_income_from_cont": _safe_float(r.get("netIncomeFromContinuingOperations")),
                "income_before_tax":    _safe_float(r.get("incomeBeforeTax")),
                "income_tax":           _safe_float(r.get("incomeTaxExpense")),
                "interest_expense":     _safe_float(r.get("interestExpense")),
                "interest_income":      _safe_float(r.get("interestIncome")),
                "rd_expense":           _safe_float(r.get("researchAndDevelopment")),
                "sga_expense":          _safe_float(r.get("sellingGeneralAdministrative")),
                "operating_expense":    _safe_float(r.get("operatingExpenses")),
                "depreciation":         _safe_float(r.get("depreciationAndAmortization")),
                "eps_basic":            _safe_float(r.get("reportedEPS")),
                "shares_diluted":       _safe_float(r.get("commonStockSharesOutstanding")),
            })
        return out

    return {
        "symbol": data.get("symbol", symbol),
        "annual":    parse_reports(data.get("annualReports", [])),
        "quarterly": parse_reports(data.get("quarterlyReports", [])),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_balance_sheet(symbol: str, exchange: str) -> dict:
    """Annual and quarterly balance sheets."""
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "BALANCE_SHEET", "symbol": av_sym},
        [symbol, "AV_BALANCE"],
        TTL["balance"]
    )
    if "error" in data:
        return data

    def parse_reports(reports: list) -> list:
        out = []
        for r in reports[:8]:
            out.append({
                "period":                   r.get("fiscalDateEnding"),
                "currency":                 r.get("reportedCurrency"),
                "total_assets":             _safe_float(r.get("totalAssets")),
                "current_assets":           _safe_float(r.get("totalCurrentAssets")),
                "cash_and_equivalents":     _safe_float(r.get("cashAndCashEquivalentsAtCarryingValue")),
                "short_term_investments":   _safe_float(r.get("shortTermInvestments")),
                "receivables":              _safe_float(r.get("currentNetReceivables")),
                "inventory":                _safe_float(r.get("inventory")),
                "non_current_assets":       _safe_float(r.get("totalNonCurrentAssets")),
                "ppe_net":                  _safe_float(r.get("propertyPlantEquipmentNet")),
                "goodwill":                 _safe_float(r.get("goodwill")),
                "intangible_assets":        _safe_float(r.get("intangibleAssets")),
                "long_term_investments":    _safe_float(r.get("longTermInvestments")),
                "total_liabilities":        _safe_float(r.get("totalLiabilities")),
                "current_liabilities":      _safe_float(r.get("totalCurrentLiabilities")),
                "short_term_debt":          _safe_float(r.get("shortTermDebt")),
                "accounts_payable":         _safe_float(r.get("currentAccountsPayable")),
                "non_current_liabilities":  _safe_float(r.get("totalNonCurrentLiabilities")),
                "long_term_debt":           _safe_float(r.get("longTermDebt")),
                "total_equity":             _safe_float(r.get("totalShareholderEquity")),
                "retained_earnings":        _safe_float(r.get("retainedEarnings")),
                "common_stock":             _safe_float(r.get("commonStock")),
                "shares_outstanding":       _safe_float(r.get("commonStockSharesOutstanding")),
                "book_value_per_share":     _safe_float(r.get("bookValue")),
            })
        return out

    return {
        "symbol": data.get("symbol", symbol),
        "annual":    parse_reports(data.get("annualReports", [])),
        "quarterly": parse_reports(data.get("quarterlyReports", [])),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_cash_flow(symbol: str, exchange: str) -> dict:
    """Annual and quarterly cash flow statements."""
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "CASH_FLOW", "symbol": av_sym},
        [symbol, "AV_CASHFLOW"],
        TTL["cashflow"]
    )
    if "error" in data:
        return data

    def parse_reports(reports: list) -> list:
        out = []
        for r in reports[:8]:
            out.append({
                "period":                       r.get("fiscalDateEnding"),
                "currency":                     r.get("reportedCurrency"),
                "operating_cashflow":           _safe_float(r.get("operatingCashflow")),
                "capex":                        _safe_float(r.get("capitalExpenditures")),
                "free_cashflow":                _safe_float(r.get("freeCashFlow")),
                "investing_cashflow":           _safe_float(r.get("cashflowFromInvestment")),
                "financing_cashflow":           _safe_float(r.get("cashflowFromFinancing")),
                "net_income":                   _safe_float(r.get("netIncome")),
                "depreciation_amortization":    _safe_float(r.get("depreciationDepletionAndAmortization")),
                "change_in_receivables":        _safe_float(r.get("changeInReceivables")),
                "change_in_inventory":          _safe_float(r.get("changeInInventory")),
                "change_in_operating_liabilities": _safe_float(r.get("changeInOperatingLiabilities")),
                "dividend_payout":              _safe_float(r.get("dividendPayout")),
                "dividends_paid_common":        _safe_float(r.get("dividendPayoutCommonStock")),
                "proceeds_from_issuance":       _safe_float(r.get("proceedsFromIssuanceOfCommonStock")),
                "repurchase_of_equity":         _safe_float(r.get("paymentsForRepurchaseOfCommonStock")),
                "debt_repayment":               _safe_float(r.get("repaymentOfLongTermDebt")),
                "change_in_cash":               _safe_float(r.get("changeInCashAndCashEquivalents")),
            })
        return out

    return {
        "symbol": data.get("symbol", symbol),
        "annual":    parse_reports(data.get("annualReports", [])),
        "quarterly": parse_reports(data.get("quarterlyReports", [])),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_earnings(symbol: str, exchange: str) -> dict:
    """
    EPS history with analyst estimates and earnings surprise %.
    One of the most valuable endpoints for tracking earnings quality.
    """
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "EARNINGS", "symbol": av_sym},
        [symbol, "AV_EARNINGS"],
        TTL["earnings"]
    )
    if "error" in data:
        return data

    def parse_annual(reports):
        return [{
            "year":            r.get("fiscalDateEnding", "")[:4],
            "period":          r.get("fiscalDateEnding"),
            "reported_eps":    _safe_float(r.get("reportedEPS")),
        } for r in reports[:8]]

    def parse_quarterly(reports):
        return [{
            "period":           r.get("fiscalDateEnding"),
            "reported_date":    r.get("reportedDate"),
            "reported_eps":     _safe_float(r.get("reportedEPS")),
            "estimated_eps":    _safe_float(r.get("estimatedEPS")),
            "surprise":         _safe_float(r.get("surprise")),
            "surprise_pct":     _safe_float(r.get("surprisePercentage")),
        } for r in reports[:12]]

    return {
        "symbol":    data.get("symbol", symbol),
        "annual":    parse_annual(data.get("annualEarnings", [])),
        "quarterly": parse_quarterly(data.get("quarterlyEarnings", [])),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_dividends(symbol: str, exchange: str) -> list:
    """Full dividend history."""
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "DIVIDENDS", "symbol": av_sym},
        [symbol, "AV_DIVIDENDS"],
        TTL["dividends"]
    )
    if "error" in data:
        return []

    divs = data.get("data", [])
    return [{
        "ex_dividend_date":  d.get("ex_dividend_date"),
        "declaration_date":  d.get("declaration_date"),
        "record_date":       d.get("record_date"),
        "payment_date":      d.get("payment_date"),
        "amount":            _safe_float(d.get("amount")),
        "currency":          d.get("currency"),
    } for d in divs[:20]]


def get_splits(symbol: str, exchange: str) -> list:
    """Stock split history."""
    av_sym = _nse_to_av(symbol, exchange)
    data = _av_get(
        {"function": "SPLITS", "symbol": av_sym},
        [symbol, "AV_SPLITS"],
        TTL["splits"]
    )
    if "error" in data:
        return []

    splits = data.get("data", [])
    return [{
        "effective_date":  s.get("effective_date"),
        "split_factor":    s.get("split_factor"),
    } for s in splits[:10]]


def get_earnings_calendar(symbol: str, exchange: str) -> list:
    """Upcoming earnings dates for the stock."""
    av_sym = _nse_to_av(symbol, exchange)
    rows = _av_get_csv(
        {"function": "EARNINGS_CALENDAR", "symbol": av_sym, "horizon": "12month"},
        [symbol, "AV_EARNINGS_CAL"],
        TTL["earnings_cal"]
    )
    return [{
        "symbol":            r.get("symbol"),
        "name":              r.get("name"),
        "report_date":       r.get("reportDate"),
        "fiscal_year_end":   r.get("fiscalDateEnding"),
        "estimate":          _safe_float(r.get("estimate")),
        "currency":          r.get("currency"),
    } for r in rows[:5]]


# ─────────────────────────────────────────────────────────────────────────────
#  ALPHA INTELLIGENCE — NEWS & SENTIMENT
# ─────────────────────────────────────────────────────────────────────────────

def get_news_sentiment(symbol: str, exchange: str, limit: int = 50) -> dict:
    """
    AI-powered news sentiment from Alpha Vantage.
    Returns articles with relevance scores and per-ticker sentiment.
    Much more targeted than generic RSS — AV scores each article for 
    relevance to the specific ticker.
    """
    av_sym = _nse_to_av(symbol, exchange)
    # AV NEWS_SENTIMENT uses plain symbol for some tickers
    # Try both formats
    data = _av_get(
        {
            "function": "NEWS_SENTIMENT",
            "tickers": av_sym,
            "sort": "LATEST",
            "limit": str(limit),
        },
        [symbol, "AV_NEWS"],
        TTL["news"]
    )
    if "error" in data:
        # Retry with plain symbol (some AV setups)
        data = _av_get(
            {
                "function": "NEWS_SENTIMENT",
                "tickers": symbol,
                "sort": "LATEST",
                "limit": str(limit),
            },
            [symbol, "AV_NEWS_PLAIN"],
            TTL["news"]
        )

    if "error" in data:
        return {"error": data["error"], "articles": [], "aggregate": {}}

    feed = data.get("feed", [])
    articles = []
    sentiment_scores = []

    for item in feed:
        title = item.get("title", "")
        url = item.get("url", "")
        time_published = item.get("time_published", "")  # "20240115T143000"
        source = item.get("source", "")
        summary = item.get("summary", "")
        overall_sent = item.get("overall_sentiment_score", 0)
        overall_label = item.get("overall_sentiment_label", "Neutral")
        banner_image = item.get("banner_image", "")

        # Parse timestamp
        pub_dt = ""
        pub_display = ""
        if time_published:
            try:
                dt = datetime.strptime(time_published, "%Y%m%dT%H%M%S")
                dt = dt.replace(tzinfo=timezone.utc)
                pub_dt = dt.isoformat()
                now = datetime.now(timezone.utc)
                diff_min = int((now - dt).total_seconds() / 60)
                if diff_min < 60:
                    pub_display = f"{diff_min}m ago"
                elif diff_min < 1440:
                    pub_display = f"{diff_min // 60}h ago"
                else:
                    pub_display = dt.strftime("%d %b")
            except Exception:
                pub_dt = time_published
                pub_display = time_published[:10]

        # Find this ticker's specific relevance + sentiment score
        ticker_relevance = 0.0
        ticker_sentiment = float(overall_sent)
        for ts in item.get("ticker_sentiment", []):
            ts_sym = ts.get("ticker", "").upper()
            if ts_sym in (symbol.upper(), av_sym.upper()):
                ticker_relevance = float(ts.get("relevance_score", 0))
                ticker_sentiment = float(ts.get("ticker_sentiment_score", overall_sent))
                break

        # Map AV sentiment score (-1 to +1) to our color system
        score = round(ticker_sentiment, 4)
        if score >= 0.35:
            label = "Very Bullish"
            color = "strong_bull"
        elif score >= 0.05:
            label = "Bullish"
            color = "bull"
        elif score <= -0.35:
            label = "Very Bearish"
            color = "strong_bear"
        elif score <= -0.05:
            label = "Bearish"
            color = "bear"
        else:
            label = "Neutral"
            color = "neutral"

        sentiment_scores.append(score)
        articles.append({
            "title":          title,
            "url":            url,
            "source":         f"AV/{source}",
            "published":      pub_dt,
            "published_display": pub_display,
            "summary":        summary[:350],
            "banner_image":   banner_image,
            "sentiment": {
                "score":    score,
                "label":    label,
                "color":    color,
                "categories": _av_topics_to_categories(item.get("topics", [])),
                "bull_signals": 0,
                "bear_signals": 0,
            },
            "relevance_score": round(ticker_relevance, 4),
            "av_overall_label": overall_label,
        })

    # Sort by relevance score (most relevant first)
    articles.sort(key=lambda x: x["relevance_score"], reverse=True)

    # Aggregate
    if sentiment_scores:
        avg = round(sum(sentiment_scores) / len(sentiment_scores), 4)
    else:
        avg = 0.0

    bull = sum(1 for s in sentiment_scores if s > 0.05)
    bear = sum(1 for s in sentiment_scores if s < -0.05)
    neut = len(sentiment_scores) - bull - bear
    total = max(len(sentiment_scores), 1)

    aggregate = {
        "score": avg,
        "label": "Very Bullish" if avg >= 0.35 else "Bullish" if avg >= 0.05 else
                 "Very Bearish" if avg <= -0.35 else "Bearish" if avg <= -0.05 else "Neutral",
        "total": len(articles),
        "bull_pct": round(bull / total * 100),
        "bear_pct": round(bear / total * 100),
        "neutral_pct": round(neut / total * 100),
        "source": "Alpha Vantage AI",
    }

    return {
        "articles": articles,
        "aggregate": aggregate,
        "av_sentiment_score_definition": "Alpha Vantage AI-scored: -1.0 (Very Bearish) to +1.0 (Very Bullish)",
        "_fetched_at": data.get("_fetched_at"),
    }


def _av_topics_to_categories(topics: list) -> list:
    """Map AV topic tags to our category system."""
    mapping = {
        "earnings": "Earnings",
        "ipo": "Deals & Contracts",
        "mergers_and_acquisitions": "Deals & Contracts",
        "financial_markets": "Market Action",
        "economy_macro": "Macro & Sector",
        "economy_fiscal": "Macro & Sector",
        "economy_monetary": "Macro & Sector",
        "finance": "Market Action",
        "life_sciences": "Regulatory",
        "manufacturing": "Macro & Sector",
        "real_estate": "Macro & Sector",
        "retail_wholesale": "Macro & Sector",
        "technology": "Macro & Sector",
    }
    cats = []
    for t in topics:
        topic = t.get("topic", "").lower()
        rel = float(t.get("relevance_score", 0))
        if rel > 0.3:
            cat = mapping.get(topic, "General")
            if cat not in cats:
                cats.append(cat)
    return cats[:2] if cats else ["General"]


# ─────────────────────────────────────────────────────────────────────────────
#  TECHNICAL INDICATORS (pre-computed by AV)
# ─────────────────────────────────────────────────────────────────────────────

def _get_technical(function: str, symbol: str, exchange: str,
                   interval: str = "daily", time_period: int = 14,
                   series_type: str = "close", extra_params: dict = None) -> dict:
    """Generic technical indicator fetcher."""
    av_sym = _nse_to_av(symbol, exchange)
    params = {
        "function": function,
        "symbol": av_sym,
        "interval": interval,
        "time_period": str(time_period),
        "series_type": series_type,
    }
    if extra_params:
        params.update(extra_params)

    data = _av_get(
        params,
        [symbol, f"AV_TECH_{function}_{time_period}"],
        TTL["technical"]
    )
    return data


def get_rsi(symbol: str, exchange: str, period: int = 14) -> dict:
    data = _get_technical("RSI", symbol, exchange, time_period=period)
    if "error" in data:
        return data
    series = data.get(f"Technical Analysis: RSI", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    all_values = [(dt, float(v["RSI"])) for dt, v in sorted(series.items(), reverse=True)[:30]]
    return {
        "latest_date": latest_date,
        "rsi": _safe_float(latest.get("RSI")),
        "history": [{"date": d, "rsi": v} for d, v in all_values],
        "_fetched_at": data.get("_fetched_at"),
    }


def get_macd(symbol: str, exchange: str) -> dict:
    data = _get_technical(
        "MACD", symbol, exchange,
        time_period=12,  # ignored for MACD but required by API
        extra_params={"fastperiod": "12", "slowperiod": "26", "signalperiod": "9"}
    )
    if "error" in data:
        return data
    series = data.get("Technical Analysis: MACD", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "latest_date": latest_date,
        "macd":       _safe_float(latest.get("MACD")),
        "signal":     _safe_float(latest.get("MACD_Signal")),
        "histogram":  _safe_float(latest.get("MACD_Hist")),
        "history": [
            {"date": dt, "macd": float(v.get("MACD", 0)),
             "signal": float(v.get("MACD_Signal", 0)),
             "hist": float(v.get("MACD_Hist", 0))}
            for dt, v in sorted(series.items(), reverse=True)[:30]
        ],
        "_fetched_at": data.get("_fetched_at"),
    }


def get_bbands(symbol: str, exchange: str, period: int = 20) -> dict:
    data = _get_technical(
        "BBANDS", symbol, exchange, time_period=period,
        extra_params={"nbdevup": "2", "nbdevdn": "2", "matype": "0"}
    )
    if "error" in data:
        return data
    series = data.get("Technical Analysis: BBANDS", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "latest_date": latest_date,
        "upper":  _safe_float(latest.get("Real Upper Band")),
        "middle": _safe_float(latest.get("Real Middle Band")),
        "lower":  _safe_float(latest.get("Real Lower Band")),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_sma(symbol: str, exchange: str, period: int = 50) -> dict:
    data = _get_technical("SMA", symbol, exchange, time_period=period)
    if "error" in data:
        return data
    series = data.get("Technical Analysis: SMA", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "period": period,
        "latest_date": latest_date,
        "sma": _safe_float(latest.get("SMA")),
        "history": [{"date": dt, "sma": float(v.get("SMA", 0))}
                    for dt, v in sorted(series.items(), reverse=True)[:50]],
        "_fetched_at": data.get("_fetched_at"),
    }


def get_adx(symbol: str, exchange: str, period: int = 14) -> dict:
    data = _get_technical("ADX", symbol, exchange, time_period=period)
    if "error" in data:
        return data
    series = data.get("Technical Analysis: ADX", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "latest_date": latest_date,
        "adx": _safe_float(latest.get("ADX")),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_stoch(symbol: str, exchange: str) -> dict:
    data = _get_technical(
        "STOCH", symbol, exchange,
        extra_params={"fastkperiod": "14", "slowkperiod": "3", "slowdperiod": "3"}
    )
    if "error" in data:
        return data
    series = data.get("Technical Analysis: STOCH", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "latest_date": latest_date,
        "slow_k": _safe_float(latest.get("SlowK")),
        "slow_d": _safe_float(latest.get("SlowD")),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_cci(symbol: str, exchange: str, period: int = 20) -> dict:
    data = _get_technical("CCI", symbol, exchange, time_period=period)
    if "error" in data:
        return data
    series = data.get("Technical Analysis: CCI", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "latest_date": latest_date,
        "cci": _safe_float(latest.get("CCI")),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_aroon(symbol: str, exchange: str, period: int = 25) -> dict:
    data = _get_technical("AROON", symbol, exchange, time_period=period)
    if "error" in data:
        return data
    series = data.get("Technical Analysis: AROON", {})
    latest = next(iter(series.values()), {}) if series else {}
    latest_date = next(iter(series.keys()), None) if series else None
    return {
        "latest_date": latest_date,
        "aroon_up":   _safe_float(latest.get("Aroon Up")),
        "aroon_down": _safe_float(latest.get("Aroon Down")),
        "_fetched_at": data.get("_fetched_at"),
    }


def get_obv(symbol: str, exchange: str) -> dict:
    data = _get_technical("OBV", symbol, exchange)
    if "error" in data:
        return data
    series = data.get("Technical Analysis: OBV", {})
    dates_sorted = sorted(series.keys(), reverse=True)[:5]
    values = [float(series[d]["OBV"]) for d in dates_sorted if "OBV" in series[d]]
    trend = "N/A"
    if len(values) >= 2:
        trend = "Rising" if values[0] > values[-1] else "Falling"
    return {
        "latest": values[0] if values else None,
        "trend": trend,
        "_fetched_at": data.get("_fetched_at"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  MACRO / FOREX
# ─────────────────────────────────────────────────────────────────────────────

def get_usd_inr() -> dict:
    """Live USD/INR exchange rate — relevant for FII impact analysis."""
    data = _av_get(
        {"function": "CURRENCY_EXCHANGE_RATE", "from_currency": "USD", "to_currency": "INR"},
        ["USDINR", "AV_FX"],
        TTL["fx"]
    )
    if "error" in data:
        return data

    fx = data.get("Realtime Currency Exchange Rate", {})
    return {
        "from":            fx.get("1. From_Currency Code"),
        "to":              fx.get("3. To_Currency Code"),
        "rate":            _safe_float(fx.get("5. Exchange Rate")),
        "bid":             _safe_float(fx.get("8. Bid Price")),
        "ask":             _safe_float(fx.get("9. Ask Price")),
        "last_refreshed":  fx.get("6. Last Refreshed"),
        "_fetched_at":     data.get("_fetched_at"),
    }


def get_economic_indicator(indicator: str) -> dict:
    """
    Macro indicators: REAL_GDP, INFLATION, FEDERAL_FUNDS_RATE, CPI, 
                      UNEMPLOYMENT, RETAIL_SALES, DURABLES, CONSUMER_SENTIMENT
    """
    interval_map = {
        "REAL_GDP": "annual",
        "REAL_GDP_PER_CAPITA": "annual",
        "INFLATION": "annual",
        "CPI": "monthly",
        "FEDERAL_FUNDS_RATE": "monthly",
        "UNEMPLOYMENT": "monthly",
        "RETAIL_SALES": "monthly",
        "DURABLES": "monthly",
        "CONSUMER_SENTIMENT": "monthly",
        "NONFARM_PAYROLL": "monthly",
    }
    params = {"function": indicator}
    if indicator in interval_map:
        params["interval"] = interval_map[indicator]

    data = _av_get(params, ["MACRO", indicator], TTL["macro"])
    if "error" in data:
        return data

    series = data.get("data", [])[:12]
    return {
        "indicator": indicator,
        "name": data.get("name", indicator),
        "unit": data.get("unit", ""),
        "data": [{"date": d.get("date"), "value": _safe_float(d.get("value"))} for d in series],
        "_fetched_at": data.get("_fetched_at"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  AGGREGATE: FULL AV PACKAGE FOR A STOCK
# ─────────────────────────────────────────────────────────────────────────────

def get_full_av_data(symbol: str, exchange: str) -> dict:
    """
    Fetch all AV fundamental data in one bundled call.
    Uses ThreadPoolExecutor to parallelize requests (but note: each is a separate API call
    — free tier has 25/day, so this function uses ~4 calls at once).
    Returns: overview + income + balance + cashflow + earnings
    """
    import concurrent.futures

    tasks = {
        "overview":  lambda: get_company_overview(symbol, exchange),
        "income":    lambda: get_income_statement(symbol, exchange),
        "balance":   lambda: get_balance_sheet(symbol, exchange),
        "cashflow":  lambda: get_cash_flow(symbol, exchange),
        "earnings":  lambda: get_earnings(symbol, exchange),
        "dividends": lambda: get_dividends(symbol, exchange),
        "splits":    lambda: get_splits(symbol, exchange),
    }

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        future_map = {ex.submit(fn): name for name, fn in tasks.items()}
        for fut in concurrent.futures.as_completed(future_map, timeout=60):
            name = future_map[fut]
            try:
                results[name] = fut.result(timeout=20)
            except Exception as e:
                results[name] = {"error": str(e)}

    results["api_usage"] = get_api_usage()
    results["fetched_at"] = datetime.now(timezone.utc).isoformat()
    return results


def get_av_technicals(symbol: str, exchange: str) -> dict:
    """Fetch AV pre-computed technical indicators (~5 API calls)."""
    import concurrent.futures

    tasks = {
        "rsi":   lambda: get_rsi(symbol, exchange),
        "macd":  lambda: get_macd(symbol, exchange),
        "bbands": lambda: get_bbands(symbol, exchange),
        "adx":   lambda: get_adx(symbol, exchange),
        "stoch": lambda: get_stoch(symbol, exchange),
        "cci":   lambda: get_cci(symbol, exchange),
        "aroon": lambda: get_aroon(symbol, exchange),
        "obv":   lambda: get_obv(symbol, exchange),
    }

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        future_map = {ex.submit(fn): name for name, fn in tasks.items()}
        for fut in concurrent.futures.as_completed(future_map, timeout=60):
            name = future_map[fut]
            try:
                results[name] = fut.result(timeout=20)
            except Exception as e:
                results[name] = {"error": str(e)}

    results["api_usage"] = get_api_usage()
    return results
