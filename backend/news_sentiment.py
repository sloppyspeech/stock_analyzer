"""
Multi-source news aggregator & sentiment analyzer for Indian stocks.

Sources:
  1.  yfinance ticker.news
  2.  Google News RSS  (company name + symbol searches)
  3.  Yahoo Finance RSS
  4.  Economic Times Markets RSS
  5.  Moneycontrol RSS
  6.  Business Standard RSS
  7.  LiveMint RSS
  8.  BSE Corporate Filings API (free, no key)
  9.  NSE Corporate Actions (free, no key)
 10.  Reddit r/IndianStockMarket (public JSON API)
 11.  Screener.in recent news (HTML scrape)
 12.  NDTV Profit RSS
 13.  Financial Express RSS
 14.  CNBC TV18 RSS
 15.  Investing.com India RSS
 16.  Trendlyne RSS / news
 17.  Pulse by Zerodha RSS
 18.  Ticker Tape RSS
"""

import re
import json
import time
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional
import yfinance as yf

from backend.database import cache_get, cache_set

# ── Timeout for all HTTP requests ────────────────────────────────────────────
REQUEST_TIMEOUT = 8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


# ─────────────────────────────────────────────────────────────────────────────
#  LEXICON-BASED SENTIMENT SCORING
# ─────────────────────────────────────────────────────────────────────────────

BULLISH_STRONG = [
    "record profit", "record revenue", "beat estimates", "beat expectations",
    "strong results", "strong earnings", "profit surge", "revenue surge",
    "massive rally", "all-time high", "multi-year high", "upgraded", "outperform",
    "dividend declared", "buyback", "share repurchase", "special dividend",
    "strong buy", "new contract", "major order", "large order", "acquisition completed",
    "merger approved", "capacity expansion", "new facility", "strategic partnership",
    "fii buying", "dii buying", "block deal buy", "promoter buying",
    "debt free", "zero debt", "cash rich", "margin expansion",
]

BULLISH_MILD = [
    "profit", "revenue growth", "growth", "expansion", "positive", "upbeat",
    "optimistic", "order win", "new order", "partnership", "launch",
    "strong demand", "recovery", "turnaround", "improved", "rose", "gained",
    "up", "higher", "increase", "jump", "surge", "rally", "bullish",
    "upgrade", "target raised", "raised target", "overweight", "accumulate",
    "buy rating", "positive outlook", "robust", "strong", "healthy",
    "beat", "above estimate", "guidance raised", "outpaced", "market share",
    "export order", "government order", "pli scheme", "approved",
]

BEARISH_STRONG = [
    "fraud", "scam", "default", "bankruptcy", "insolvency", "sebi probe",
    "sebi notice", "sebi order", "ed probe", "enforcement directorate",
    "cbi probe", "income tax raid", "arrested", "scandal", "corruption",
    "massive loss", "record loss", "profit crash", "revenue collapse",
    "promoter selling", "promoter pledge", "loan default", "npa",
    "debt restructuring", "write-off", "write off", "impairment",
    "forced delisting", "trading suspended", "circuit breaker",
    "class action", "lawsuit filed", "penalty imposed", "ban",
    "plant shutdown", "factory fire", "major accident",
]

BEARISH_MILD = [
    "loss", "decline", "miss", "below estimate", "disappointing",
    "weak results", "profit fall", "revenue decline", "slowdown",
    "cut", "downgrade", "underperform", "reduce", "sell rating",
    "negative", "concern", "worried", "risk", "challenge", "pressure",
    "down", "lower", "fell", "drop", "slump", "correction", "bearish",
    "target cut", "target lowered", "guidance cut", "layoffs", "job cuts",
    "margin squeeze", "cost pressure", "inflation impact",
    "competition", "market share loss", "regulatory hurdle",
    "fii selling", "dii selling", "block deal sell",
]

CATEGORY_KEYWORDS = {
    "Earnings": ["profit", "revenue", "earnings", "quarterly", "q1", "q2", "q3", "q4",
                 "annual", "results", "ebitda", "margin", "eps", "net income", "loss"],
    "Regulatory": ["sebi", "rbi", "nse", "bse", "regulatory", "compliance", "notice",
                   "penalty", "fine", "probe", "investigation", "order", "approved", "rejected"],
    "Management": ["ceo", "cfo", "md", "director", "board", "appointed", "resigned",
                   "promoter", "insider", "buyback", "dividend", "agm", "egm"],
    "Deals & Contracts": ["contract", "order", "deal", "partnership", "acquisition",
                           "merger", "takeover", "joint venture", "mou", "agreement"],
    "Market Action": ["rally", "crash", "circuit", "block deal", "bulk deal", "fii", "dii",
                      "mutual fund", "nifty", "sensex", "index", "52-week", "support", "resistance"],
    "Macro & Sector": ["economy", "gdp", "inflation", "interest rate", "budget", "policy",
                        "industry", "sector", "global", "crude", "rupee", "dollar", "export", "import"],
}


def score_text(text: str) -> dict:
    """Score text sentiment using keyword lexicon. Returns score -1.0 to +1.0."""
    text_l = text.lower()

    bs_score = sum(2 for kw in BULLISH_STRONG if kw in text_l)
    bm_score = sum(1 for kw in BULLISH_MILD if kw in text_l)
    bs_neg = sum(2 for kw in BEARISH_STRONG if kw in text_l)
    bm_neg = sum(1 for kw in BEARISH_MILD if kw in text_l)

    bull = bs_score + bm_score
    bear = bs_neg + bm_neg
    total = bull + bear

    if total == 0:
        normalized = 0.0
    else:
        normalized = round((bull - bear) / (total + 2), 3)  # dampened

    if normalized >= 0.4:
        label = "Very Bullish"
        color = "strong_bull"
    elif normalized >= 0.1:
        label = "Bullish"
        color = "bull"
    elif normalized <= -0.4:
        label = "Very Bearish"
        color = "strong_bear"
    elif normalized <= -0.1:
        label = "Bearish"
        color = "bear"
    else:
        label = "Neutral"
        color = "neutral"

    # Detect categories
    cats = [cat for cat, kws in CATEGORY_KEYWORDS.items()
            if any(kw in text_l for kw in kws)]

    return {
        "score": normalized,
        "label": label,
        "color": color,
        "bull_signals": bull,
        "bear_signals": bear,
        "categories": cats[:2] if cats else ["General"],
    }


def aggregate_sentiment(articles: list) -> dict:
    """Compute aggregate sentiment from a list of articles."""
    if not articles:
        return {"score": 0, "label": "Neutral", "total": 0, "distribution": {}}

    scores = [a.get("sentiment", {}).get("score", 0) for a in articles]
    avg = round(sum(scores) / len(scores), 3)

    dist = {"Very Bullish": 0, "Bullish": 0, "Neutral": 0, "Bearish": 0, "Very Bearish": 0}
    for a in articles:
        lbl = a.get("sentiment", {}).get("label", "Neutral")
        dist[lbl] = dist.get(lbl, 0) + 1

    if avg >= 0.35:
        label = "Very Bullish"
    elif avg >= 0.1:
        label = "Bullish"
    elif avg <= -0.35:
        label = "Very Bearish"
    elif avg <= -0.1:
        label = "Bearish"
    else:
        label = "Neutral"

    # Source breakdown
    by_source = {}
    for a in articles:
        src = a.get("source", "Unknown")
        if src not in by_source:
            by_source[src] = []
        by_source[src].append(a.get("sentiment", {}).get("score", 0))

    source_sentiment = {
        src: round(sum(v) / len(v), 3)
        for src, v in by_source.items()
    }

    return {
        "score": avg,
        "label": label,
        "total": len(articles),
        "distribution": dist,
        "by_source": source_sentiment,
        "bull_pct": round((dist["Very Bullish"] + dist["Bullish"]) / len(articles) * 100),
        "bear_pct": round((dist["Very Bearish"] + dist["Bearish"]) / len(articles) * 100),
        "neutral_pct": round(dist["Neutral"] / len(articles) * 100),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_url(url: str, as_json: bool = False, timeout: int = REQUEST_TIMEOUT):
    """Fetch URL with custom headers. Returns (content, error)."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if as_json:
                return json.loads(raw), None
            return raw.decode("utf-8", errors="replace"), None
    except Exception as e:
        return None, str(e)


def _parse_rss(xml_text: str, source_name: str, symbol: str, company: str) -> list:
    """Parse RSS/Atom XML and return list of article dicts."""
    articles = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return articles

    # Handle both RSS 2.0 and Atom
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)

    for item in items[:15]:
        def txt(tag, ns_map=None):
            el = item.find(tag) if not ns_map else item.find(tag, ns_map)
            return (el.text or "").strip() if el is not None else ""

        title = txt("title") or txt("atom:title", ns)
        link = txt("link") or txt("atom:link", ns)
        desc = txt("description") or txt("summary") or txt("atom:summary", ns)
        pub_date = txt("pubDate") or txt("published") or txt("atom:published", ns)

        if not title:
            continue

        # Filter relevance: must mention symbol or company name
        combined = (title + " " + desc).lower()
        sym_l = symbol.lower()
        comp_words = company.lower().split()[:3]
        relevant = (
            sym_l in combined or
            any(w in combined for w in comp_words if len(w) > 3)
        )
        if not relevant and source_name not in ("Yahoo Finance", "yfinance"):
            continue

        # Parse date
        pub_dt = _parse_date(pub_date)

        # Score sentiment
        sentiment = score_text(title + " " + desc)

        articles.append({
            "title": title,
            "url": link,
            "source": source_name,
            "published": pub_dt,
            "published_display": _fmt_date(pub_dt),
            "summary": _clean_html(desc)[:300],
            "sentiment": sentiment,
        })

    return articles


def _parse_date(s: str) -> str:
    """Try to parse various date formats → ISO string."""
    if not s:
        return datetime.now(timezone.utc).isoformat()
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]:
        try:
            return datetime.strptime(s.strip(), fmt).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


def _fmt_date(iso: str) -> str:
    """Human-readable relative time."""
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - dt
        mins = int(diff.total_seconds() / 60)
        if mins < 60:
            return f"{mins}m ago"
        hours = mins // 60
        if hours < 24:
            return f"{hours}h ago"
        days = hours // 24
        if days < 7:
            return f"{days}d ago"
        return dt.strftime("%d %b")
    except Exception:
        return ""


def _clean_html(text: str) -> str:
    """Strip HTML tags."""
    return re.sub(r"<[^>]+>", " ", text).strip()


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE FETCHERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_yfinance_news(symbol: str, exchange: str) -> list:
    """yfinance built-in news for the ticker."""
    suffix = ".NS" if exchange == "NSE" else ".BO"
    try:
        ticker = yf.Ticker(symbol + suffix)
        raw_news = ticker.news or []
        articles = []
        for item in raw_news[:15]:
            title = item.get("title", "")
            link = item.get("link", "")
            ts = item.get("providerPublishTime", 0)
            publisher = item.get("publisher", "Yahoo Finance")
            summary = item.get("summary", "")

            pub_dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else datetime.now(timezone.utc).isoformat()
            sentiment = score_text(title + " " + summary)

            articles.append({
                "title": title,
                "url": link,
                "source": f"Yahoo/yfinance ({publisher})",
                "published": pub_dt,
                "published_display": _fmt_date(pub_dt),
                "summary": summary[:300],
                "sentiment": sentiment,
            })
        return articles
    except Exception:
        return []


def fetch_google_news(symbol: str, company: str) -> list:
    """Google News RSS — two queries: symbol-focused and company-focused."""
    articles = []
    queries = [
        f"{symbol} NSE stock",
        f"{company} stock India",
        f"{company} quarterly results",
    ]
    seen_titles = set()

    for q in queries:
        encoded = urllib.parse.quote(q)
        url = (
            f"https://news.google.com/rss/search"
            f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
        )
        xml, err = _fetch_url(url)
        if err or not xml:
            continue

        for art in _parse_rss(xml, "Google News", symbol, company):
            key = art["title"][:60].lower()
            if key not in seen_titles:
                seen_titles.add(key)
                articles.append(art)

    return articles[:20]


def fetch_yahoo_rss(symbol: str, exchange: str) -> list:
    """Yahoo Finance RSS headline feed."""
    suffix = ".NS" if exchange == "NSE" else ".BO"
    url = f"https://finance.yahoo.com/rss/headline?s={symbol}{suffix}"
    xml, err = _fetch_url(url)
    if err or not xml:
        return []
    arts = _parse_rss(xml, "Yahoo Finance", symbol, symbol)
    # Yahoo RSS is always relevant — don't filter by company name
    return arts


def fetch_economic_times(symbol: str, company: str) -> list:
    """Economic Times Markets + company-specific search RSS."""
    articles = []
    company_slug = company.lower().replace(" ", "-").replace(".", "")

    feeds = [
        ("https://economictimes.indiatimes.com/markets/stocks/rss.cms", "Economic Times Markets"),
        (f"https://economictimes.indiatimes.com/topic/{company_slug}/rssfeeds/1715249553.cms", "Economic Times"),
        ("https://economictimes.indiatimes.com/markets/expert-views/rss.cms", "ET Expert Views"),
    ]

    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))

    return articles


def fetch_moneycontrol(symbol: str, company: str) -> list:
    """Moneycontrol RSS feeds."""
    feeds = [
        ("https://www.moneycontrol.com/rss/buzzingstocks.xml", "Moneycontrol Buzzing"),
        ("https://www.moneycontrol.com/rss/marketreports.xml", "Moneycontrol Markets"),
        ("https://www.moneycontrol.com/rss/business.xml", "Moneycontrol Business"),
        ("https://www.moneycontrol.com/rss/results.xml", "Moneycontrol Results"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_business_standard(symbol: str, company: str) -> list:
    """Business Standard RSS."""
    feeds = [
        ("https://www.business-standard.com/rss/markets-106.rss", "Business Standard"),
        ("https://www.business-standard.com/rss/companies-101.rss", "BS Companies"),
        ("https://www.business-standard.com/rss/finance-103.rss", "BS Finance"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_livemint(symbol: str, company: str) -> list:
    """LiveMint RSS."""
    feeds = [
        ("https://www.livemint.com/rss/companies", "LiveMint Companies"),
        ("https://www.livemint.com/rss/markets", "LiveMint Markets"),
        ("https://www.livemint.com/rss/money", "LiveMint Money"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_bse_filings(symbol: str) -> list:
    """
    BSE Corporate Filings via BSE India public API.
    Returns recent exchange announcements/filings.
    """
    # BSE scrip code lookup - we'll use the search API
    search_url = (
        f"https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w"
        f"?categ=0&pageno=1&strSearch={symbol}&trade_date=&type=0"
    )
    data, err = _fetch_url(search_url, as_json=True)
    articles = []

    if err or not data:
        # Try alternative BSE news endpoint
        return _fetch_bse_announcements_alt(symbol)

    try:
        results = data.get("Table", []) or []
        for item in results[:10]:
            title = item.get("HEADLINE", "")
            date_str = item.get("NEWS_DT", "")
            link = item.get("ATTACHMENTNAME", "")
            if not title:
                continue

            pub_dt = _parse_date(date_str)
            sentiment = score_text(title)
            articles.append({
                "title": title,
                "url": f"https://www.bseindia.com{link}" if link else "https://www.bseindia.com",
                "source": "BSE Filing",
                "published": pub_dt,
                "published_display": _fmt_date(pub_dt),
                "summary": title,
                "sentiment": sentiment,
            })
    except Exception:
        pass

    return articles


def _fetch_bse_announcements_alt(symbol: str) -> list:
    """Alternative BSE announcements scrape."""
    url = f"https://www.bseindia.com/stock-share-price/companyinfo/{symbol.lower()}/announcements/"
    # This would need HTML parsing, skip for now
    return []


def fetch_nse_corporate_actions(symbol: str) -> list:
    """
    NSE Corporate Actions API (free, no auth).
    Dividends, splits, bonuses, rights issues.
    """
    url = f"https://www.nseindia.com/api/corporates-corporateActions?index=equities&symbol={symbol}"
    # NSE requires specific session cookies - use their public data endpoint instead
    # Try the NSE quote API which has recent corporate data
    url2 = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info"

    articles = []

    # Try NSE CA endpoint
    ca_url = f"https://www.nseindia.com/api/corporates-corporateActions?index=equities&symbol={urllib.parse.quote(symbol)}"
    headers_nse = {
        **HEADERS,
        "Referer": "https://www.nseindia.com/",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        req = urllib.request.Request(ca_url, headers=headers_nse)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read())
            for item in (data or [])[:8]:
                purpose = item.get("purpose", "")
                ex_date = item.get("exDate", "")
                record_date = item.get("recordDate", "")
                bc_start = item.get("bcStartDate", "")

                if not purpose:
                    continue

                title = f"NSE Corporate Action: {purpose} — {symbol}"
                if ex_date:
                    title += f" (Ex-Date: {ex_date})"

                sentiment = score_text(purpose)
                pub_dt = _parse_date(ex_date or record_date or bc_start)

                articles.append({
                    "title": title,
                    "url": f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
                    "source": "NSE Corporate Action",
                    "published": pub_dt,
                    "published_display": _fmt_date(pub_dt),
                    "summary": f"Purpose: {purpose} | Ex-Date: {ex_date} | Record Date: {record_date}",
                    "sentiment": sentiment,
                })
    except Exception:
        pass

    return articles


def fetch_reddit_india_stocks(symbol: str, company: str) -> list:
    """Reddit r/IndianStockMarket and r/IndiaInvestments via public JSON API."""
    articles = []
    subreddits = ["IndianStockMarket", "IndiaInvestments", "nifty50"]
    seen = set()

    for sub in subreddits:
        q = urllib.parse.quote(symbol)
        url = f"https://www.reddit.com/r/{sub}/search.json?q={q}&sort=new&limit=8&restrict_sr=1"
        data, err = _fetch_url(url, as_json=True)
        if err or not data:
            continue

        try:
            posts = data.get("data", {}).get("children", [])
            for post in posts:
                d = post.get("data", {})
                title = d.get("title", "")
                selftext = d.get("selftext", "")[:200]
                url_post = "https://reddit.com" + d.get("permalink", "")
                created = d.get("created_utc", 0)
                score_val = d.get("score", 0)
                num_comments = d.get("num_comments", 0)
                flair = d.get("link_flair_text", "")

                if not title:
                    continue

                combined = (title + " " + selftext).lower()
                sym_l = symbol.lower()
                comp_words = company.lower().split()[:3]
                relevant = sym_l in combined or any(
                    w in combined for w in comp_words if len(w) > 3
                )
                if not relevant:
                    continue

                key = title[:60].lower()
                if key in seen:
                    continue
                seen.add(key)

                pub_dt = datetime.fromtimestamp(
                    created, tz=timezone.utc
                ).isoformat() if created else datetime.now(timezone.utc).isoformat()

                sentiment = score_text(title + " " + selftext)

                summary = selftext[:200] or f"Reddit post — {score_val} upvotes, {num_comments} comments"
                if flair:
                    summary = f"[{flair}] " + summary

                articles.append({
                    "title": title,
                    "url": url_post,
                    "source": f"Reddit r/{sub}",
                    "published": pub_dt,
                    "published_display": _fmt_date(pub_dt),
                    "summary": summary,
                    "sentiment": sentiment,
                    "reddit_score": score_val,
                    "reddit_comments": num_comments,
                })
        except Exception:
            continue

    return articles


def fetch_screener_news(symbol: str) -> list:
    """
    Screener.in concall/annual report notes — HTML scrape.
    Lightweight fetch of the company page for key observations.
    """
    url = f"https://www.screener.in/company/{symbol}/consolidated/"
    html, err = _fetch_url(url)
    if err or not html:
        url = f"https://www.screener.in/company/{symbol}/"
        html, err = _fetch_url(url)

    articles = []
    if err or not html:
        return articles

    # Extract announcements/notes (simple regex for screener structure)
    # Look for <li> items in the announcements section
    ann_section = re.search(
        r'<section[^>]*id="announcements"[^>]*>(.*?)</section>',
        html, re.DOTALL
    )
    if ann_section:
        items = re.findall(r'<li[^>]*>(.*?)</li>', ann_section.group(1), re.DOTALL)
        for item in items[:8]:
            clean = _clean_html(item).strip()
            if len(clean) < 20:
                continue
            sentiment = score_text(clean)
            articles.append({
                "title": clean[:120],
                "url": url,
                "source": "Screener.in",
                "published": datetime.now(timezone.utc).isoformat(),
                "published_display": "recent",
                "summary": clean[:300],
                "sentiment": sentiment,
            })

    return articles[:5]


def fetch_ndtv_profit(symbol: str, company: str) -> list:
    """NDTV Profit / NDTV Business RSS feeds."""
    feeds = [
        ("https://feeds.feedburner.com/ndtvprofit-latest", "NDTV Profit"),
        ("https://www.ndtvprofit.com/rss/markets", "NDTV Markets"),
        ("https://www.ndtvprofit.com/rss/companies", "NDTV Companies"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_financial_express(symbol: str, company: str) -> list:
    """Financial Express (sister pub of Indian Express) RSS."""
    feeds = [
        ("https://www.financialexpress.com/market/feed/", "Financial Express Markets"),
        ("https://www.financialexpress.com/industry/feed/", "FE Industry"),
        ("https://www.financialexpress.com/investing/feed/", "FE Investing"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_cnbc_tv18(symbol: str, company: str) -> list:
    """CNBC TV18 RSS — live market coverage + corporate news."""
    feeds = [
        ("https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market-buzz.xml", "CNBC TV18 Market Buzz"),
        ("https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market-earnings.xml", "CNBC TV18 Earnings"),
        ("https://www.cnbctv18.com/commonfeeds/v1/cne/rss/business.xml", "CNBC TV18 Business"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_investing_india(symbol: str, company: str) -> list:
    """Investing.com India RSS — global + India market analysis."""
    feeds = [
        ("https://in.investing.com/rss/news.rss", "Investing.com India"),
        ("https://in.investing.com/rss/market_overview.rss", "Investing.com Overview"),
    ]
    articles = []
    for url, name in feeds:
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        articles.extend(_parse_rss(xml, name, symbol, company))
    return articles


def fetch_trendlyne(symbol: str, company: str) -> list:
    """
    Trendlyne — popular Indian stock research platform.
    Scrapes their news/analysis via Google News with site filter.
    """
    articles = []
    queries = [
        f"site:trendlyne.com {symbol} stock",
        f"site:trendlyne.com {company}",
    ]
    seen = set()
    for q in queries:
        encoded = urllib.parse.quote(q)
        url = (
            f"https://news.google.com/rss/search"
            f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
        )
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        for art in _parse_rss(xml, "Trendlyne", symbol, company):
            key = art["title"][:60].lower()
            if key not in seen:
                seen.add(key)
                articles.append(art)
    return articles[:10]


def fetch_zerodha_pulse(symbol: str, company: str) -> list:
    """Pulse by Zerodha — aggregated broker research and news."""
    articles = []
    queries = [
        f"site:pulse.zerodha.com {symbol}",
        f"site:pulse.zerodha.com {company}",
    ]
    seen = set()
    for q in queries:
        encoded = urllib.parse.quote(q)
        url = (
            f"https://news.google.com/rss/search"
            f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
        )
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        for art in _parse_rss(xml, "Pulse (Zerodha)", symbol, company):
            key = art["title"][:60].lower()
            if key not in seen:
                seen.add(key)
                articles.append(art)
    return articles[:10]


def fetch_tickertape_news(symbol: str, company: str) -> list:
    """Ticker Tape — Smallcase research platform news via Google RSS."""
    articles = []
    queries = [
        f"site:tickertape.in {symbol}",
        f"site:tickertape.in {company} stock",
    ]
    seen = set()
    for q in queries:
        encoded = urllib.parse.quote(q)
        url = (
            f"https://news.google.com/rss/search"
            f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
        )
        xml, err = _fetch_url(url)
        if err or not xml:
            continue
        for art in _parse_rss(xml, "Ticker Tape", symbol, company):
            key = art["title"][:60].lower()
            if key not in seen:
                seen.add(key)
                articles.append(art)
    return articles[:10]


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN AGGREGATOR
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_news(symbol: str, exchange: str, company: str, use_cache: bool = True) -> dict:
    """
    Aggregate news from all sources, deduplicate, score sentiment.
    Returns structured response with articles + sentiment summary.
    """
    cache_key = f"news_{symbol}_{exchange}"
    if use_cache:
        cached = cache_get(symbol, exchange, cache_key)
        if cached:
            return cached

    all_articles = []
    errors = []

    # ── Fetch from all sources concurrently via threading ────────────────────
    import concurrent.futures

    sources = [
        ("yfinance", lambda: fetch_yfinance_news(symbol, exchange)),
        ("google_news", lambda: fetch_google_news(symbol, company)),
        ("yahoo_rss", lambda: fetch_yahoo_rss(symbol, exchange)),
        ("economic_times", lambda: fetch_economic_times(symbol, company)),
        ("moneycontrol", lambda: fetch_moneycontrol(symbol, company)),
        ("business_standard", lambda: fetch_business_standard(symbol, company)),
        ("livemint", lambda: fetch_livemint(symbol, company)),
        ("bse_filings", lambda: fetch_bse_filings(symbol)),
        ("nse_ca", lambda: fetch_nse_corporate_actions(symbol)),
        ("reddit", lambda: fetch_reddit_india_stocks(symbol, company)),
        ("screener", lambda: fetch_screener_news(symbol)),
        # ── New sources ──
        ("ndtv_profit", lambda: fetch_ndtv_profit(symbol, company)),
        ("financial_express", lambda: fetch_financial_express(symbol, company)),
        ("cnbc_tv18", lambda: fetch_cnbc_tv18(symbol, company)),
        ("investing_india", lambda: fetch_investing_india(symbol, company)),
        ("trendlyne", lambda: fetch_trendlyne(symbol, company)),
        ("zerodha_pulse", lambda: fetch_zerodha_pulse(symbol, company)),
        ("tickertape", lambda: fetch_tickertape_news(symbol, company)),
    ]

    source_counts = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_map = {executor.submit(fn): name for name, fn in sources}
        for future in concurrent.futures.as_completed(future_map, timeout=30):
            name = future_map[future]
            try:
                arts = future.result(timeout=12)
                source_counts[name] = len(arts)
                all_articles.extend(arts)
            except Exception as e:
                errors.append(f"{name}: {str(e)[:80]}")
                source_counts[name] = 0

    # ── Deduplicate by title similarity ──────────────────────────────────────
    seen_titles = set()
    unique = []
    for art in all_articles:
        key = re.sub(r"[^a-z0-9]", "", art["title"][:50].lower())
        if key and key not in seen_titles:
            seen_titles.add(key)
            unique.append(art)

    # ── Sort by published date (newest first) ────────────────────────────────
    def sort_key(a):
        try:
            return datetime.fromisoformat(a["published"]).timestamp()
        except Exception:
            return 0

    unique.sort(key=sort_key, reverse=True)

    # ── Compute aggregate sentiment ───────────────────────────────────────────
    aggregate = aggregate_sentiment(unique)

    # ── Category breakdown ────────────────────────────────────────────────────
    cat_counts = {}
    for art in unique:
        for cat in art["sentiment"].get("categories", ["General"]):
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

    # ── Sentiment over time (last 7 days, grouped by day) ────────────────────
    timeline = build_sentiment_timeline(unique)

    result = {
        "articles": unique[:60],
        "total": len(unique),
        "aggregate": aggregate,
        "categories": cat_counts,
        "timeline": timeline,
        "source_counts": source_counts,
        "errors": errors,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # Cache for 30 minutes (news is time-sensitive)
    cache_set(symbol, exchange, cache_key, result, ttl_hours=0)
    # Use shorter TTL via direct DB update — 30 mins
    try:
        from backend.database import get_db
        conn = get_db()
        conn.execute(
            "UPDATE stock_cache SET expires_at = datetime('now', '+30 minutes') "
            "WHERE symbol=? AND exchange=? AND data_type=?",
            (symbol, exchange, cache_key)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    return result


def build_sentiment_timeline(articles: list) -> list:
    """Group articles by day and compute daily sentiment."""
    by_day = {}
    now = datetime.now(timezone.utc)

    for art in articles:
        try:
            dt = datetime.fromisoformat(art["published"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            days_ago = (now - dt).days
            if days_ago > 30:
                continue
            day_key = dt.strftime("%Y-%m-%d")
            if day_key not in by_day:
                by_day[day_key] = []
            by_day[day_key].append(art["sentiment"]["score"])
        except Exception:
            pass

    timeline = []
    for day, scores in sorted(by_day.items(), reverse=True)[:14]:
        avg = round(sum(scores) / len(scores), 3)
        timeline.append({
            "date": day,
            "score": avg,
            "count": len(scores),
        })

    return sorted(timeline, key=lambda x: x["date"])


# ─────────────────────────────────────────────────────────────────────────────
#  AI SENTIMENT ANALYSIS (called from ai_analysis.py)
# ─────────────────────────────────────────────────────────────────────────────

def build_sentiment_prompt(symbol: str, company: str, news_data: dict) -> str:
    """Build the AI prompt for sentiment analysis."""
    aggregate = news_data.get("aggregate", {})
    articles = news_data.get("articles", [])[:30]
    categories = news_data.get("categories", {})
    timeline = news_data.get("timeline", [])

    # Group top headlines by sentiment
    bullish_arts = [a for a in articles if a["sentiment"]["score"] > 0.1][:8]
    bearish_arts = [a for a in articles if a["sentiment"]["score"] < -0.1][:8]
    neutral_arts = [a for a in articles if -0.1 <= a["sentiment"]["score"] <= 0.1][:4]

    def art_line(a):
        return f"  [{a['source']}] {a['title']} ({a['published_display']})"

    prompt = f"""You are a market analyst specializing in Indian equities. Analyze the news sentiment for {company} ({symbol}).

=== QUANTITATIVE SENTIMENT SUMMARY ===
Overall Sentiment: {aggregate.get('label', 'N/A')} (Score: {aggregate.get('score', 0):+.3f})
Total Articles Analyzed: {aggregate.get('total', 0)}
Bullish Articles: {aggregate.get('bull_pct', 0)}% | Bearish: {aggregate.get('bear_pct', 0)}% | Neutral: {aggregate.get('neutral_pct', 0)}%

News Categories Detected: {', '.join(f"{k}({v})" for k, v in sorted(categories.items(), key=lambda x: -x[1])[:6])}

=== POSITIVE/BULLISH HEADLINES ===
{chr(10).join(art_line(a) for a in bullish_arts) or "  None found"}

=== NEGATIVE/BEARISH HEADLINES ===
{chr(10).join(art_line(a) for a in bearish_arts) or "  None found"}

=== NEUTRAL / CORPORATE ACTIONS ===
{chr(10).join(art_line(a) for a in neutral_arts) or "  None found"}

=== SENTIMENT TREND (Recent Days) ===
{chr(10).join(f"  {t['date']}: {t['score']:+.3f} ({t['count']} articles)" for t in timeline[-7:]) or "  No timeline data"}

Please provide a comprehensive news sentiment analysis covering:

## 1. Sentiment Overview
What is the dominant market mood? Is news flow positive, negative, or mixed?

## 2. Key Positive Catalysts
What are the main bullish narratives in recent news? Any earnings beats, new orders, upgrades?

## 3. Key Risks & Negative Catalysts
What are the main concerns? Any regulatory issues, earnings misses, management changes?

## 4. Corporate Actions & Events
Any dividends, buybacks, splits, bonus issues, rights offerings announced?

## 5. Analyst & Media Coverage
What is the general media/analyst tone? Any notable upgrades or downgrades?

## 6. Reddit/Social Sentiment
What is retail investor sentiment? Any notable discussions or trending themes?

## 7. Sentiment Trend
Is sentiment improving, deteriorating, or stable over recent days?

## 8. Trading Implication
Based purely on news sentiment, what is the near-term directional bias?
SENTIMENT VERDICT: [VERY BULLISH / BULLISH / NEUTRAL / BEARISH / VERY BEARISH]
Confidence: [HIGH / MEDIUM / LOW] — with 1-2 sentence reasoning.
"""
    return prompt
