"""
Ticker database - comprehensive list of NSE/BSE stocks with local caching.

On first run, downloads the full NSE equity list (~2000 stocks) and BSE
equity list (~4800 stocks with scrip codes), merges them via ISIN, and stores
in SQLite. For unknown tickers, falls back to yfinance search and caches
results for future lookups.
"""
import sqlite3
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "stock_analyzer.db"


def _get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_ticker_db():
    """Create the tickers table and populate if empty."""
    conn = _get_db()
    cursor = conn.cursor()
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS tickers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            company_name TEXT NOT NULL,
            exchange TEXT NOT NULL DEFAULT 'NSE',
            series TEXT,
            isin TEXT,
            bse_code TEXT,
            sector TEXT,
            industry TEXT,
            source TEXT DEFAULT 'nse_list',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, exchange)
        );

        CREATE INDEX IF NOT EXISTS idx_tickers_symbol ON tickers(symbol);
        CREATE INDEX IF NOT EXISTS idx_tickers_name ON tickers(company_name);
        CREATE INDEX IF NOT EXISTS idx_tickers_isin ON tickers(isin);
        CREATE INDEX IF NOT EXISTS idx_tickers_bse_code ON tickers(bse_code);
    """)
    conn.commit()

    # Add bse_code column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute("SELECT bse_code FROM tickers LIMIT 1")
    except Exception:
        cursor.execute("ALTER TABLE tickers ADD COLUMN bse_code TEXT")
        conn.commit()

    # Check if table is empty -> seed it
    cursor.execute("SELECT COUNT(*) as cnt FROM tickers")
    count = cursor.fetchone()["cnt"]
    conn.close()

    if count == 0:
        print("[*] Ticker database is empty -- downloading NSE & BSE equity lists...")
        _seed_all_tickers()
    else:
        # Check if bse_code data is missing (migration)
        conn2 = _get_db()
        cursor2 = conn2.cursor()
        cursor2.execute("SELECT COUNT(*) as cnt FROM tickers WHERE bse_code IS NOT NULL AND bse_code != ''")
        bse_count = cursor2.fetchone()["cnt"]
        conn2.close()
        if bse_count == 0:
            print("[*] BSE codes missing -- downloading BSE equity list...")
            _update_bse_codes()
        else:
            print(f"[OK] Ticker DB ready: {count} tickers ({bse_count} with BSE codes)")


def _seed_all_tickers():
    """Download NSE and BSE equity lists, merge via ISIN, and insert."""
    # Step 1: Download NSE tickers
    nse_tickers = []
    try:
        nse_tickers = _download_nse_csv()
        print(f"  [v] Downloaded {len(nse_tickers)} NSE tickers")
    except Exception as e:
        print(f"  [!] Failed to download NSE CSV: {e}")

    if not nse_tickers:
        print("  [i] Using embedded stock list as fallback.")
        nse_tickers = _get_fallback_tickers()

    # Step 2: Download BSE tickers (with scrip codes)
    bse_map = {}  # ISIN -> bse_code
    try:
        bse_data = _download_bse_csv()
        for item in bse_data:
            isin = item.get("isin", "").strip()
            bse_code = item.get("bse_code", "").strip()
            if isin and bse_code:
                bse_map[isin] = bse_code
        print(f"  [v] Downloaded {len(bse_data)} BSE tickers ({len(bse_map)} with ISIN mapping)")
    except Exception as e:
        print(f"  [!] Failed to download BSE CSV: {e}")

    # Step 3: Merge -- add bse_code to NSE tickers via ISIN
    matched = 0
    for t in nse_tickers:
        isin = t.get("isin", "").strip()
        if isin and isin in bse_map:
            t["bse_code"] = bse_map[isin]
            matched += 1

    if nse_tickers:
        _bulk_insert_tickers(nse_tickers)
        print(f"  [OK] Seeded {len(nse_tickers)} tickers ({matched} linked to BSE codes)")

    # Step 4: Insert BSE-only tickers (not already in NSE list)
    nse_isins = {t.get("isin", "") for t in nse_tickers if t.get("isin")}
    bse_only = []
    try:
        bse_data_full = _download_bse_csv()
        for item in bse_data_full:
            isin = item.get("isin", "").strip()
            if isin and isin not in nse_isins:
                bse_only.append(item)
        if bse_only:
            _bulk_insert_tickers(bse_only)
            print(f"  [OK] Added {len(bse_only)} BSE-only tickers")
    except Exception:
        pass


def _download_nse_csv():
    """Download the equity list CSV from NSE India."""
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    # NSE provides equity list at this URL
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    import csv
    import io

    reader = csv.DictReader(io.StringIO(resp.text))
    tickers = []

    for row in reader:
        symbol = row.get("SYMBOL", "").strip()
        name = row.get("NAME OF COMPANY", "").strip()
        series = row.get(" SERIES", row.get("SERIES", "")).strip()
        isin = row.get(" ISIN NUMBER", row.get("ISIN NUMBER", "")).strip()

        if symbol and name:
            tickers.append({
                "symbol": symbol,
                "company_name": name,
                "exchange": "NSE",
                "series": series,
                "isin": isin,
                "source": "nse_csv",
            })

    return tickers


def _download_bse_csv():
    """Download the BSE equity list with scrip codes."""
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.bseindia.com/",
        "Origin": "https://www.bseindia.com",
    }

    # BSE API endpoint for equity list
    url = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    tickers = []
    if isinstance(data, list):
        for item in data:
            code = str(item.get("SCRIP_CD", "") or "").strip()
            name = (item.get("Issuer_Name", "") or item.get("Scrip_Name", "") or "").strip()
            isin = (item.get("ISIN_NUMBER", "") or "").strip()
            industry = (item.get("INDUSTRY", "") or "").strip()
            group = (item.get("GROUP", "") or "").strip()

            if code and name:
                tickers.append({
                    "symbol": code,
                    "company_name": name,
                    "exchange": "BSE",
                    "isin": isin,
                    "bse_code": code,
                    "industry": industry,
                    "series": group,
                    "source": "bse_api",
                })

    return tickers


def _update_bse_codes():
    """Update existing NSE tickers with BSE codes via ISIN cross-reference."""
    try:
        bse_data = _download_bse_csv()
        bse_map = {}  # ISIN -> bse_code
        for item in bse_data:
            isin = item.get("isin", "").strip()
            bse_code = item.get("bse_code", "").strip()
            if isin and bse_code:
                bse_map[isin] = bse_code

        if not bse_map:
            print("  [!] No BSE data to update")
            return

        conn = _get_db()
        cursor = conn.cursor()
        updated = 0
        for isin, bse_code in bse_map.items():
            cursor.execute(
                "UPDATE tickers SET bse_code = ? WHERE isin = ? AND (bse_code IS NULL OR bse_code = '')",
                (bse_code, isin)
            )
            updated += cursor.rowcount
        conn.commit()
        conn.close()
        print(f"  [OK] Updated {updated} tickers with BSE codes")
    except Exception as e:
        print(f"  [!] BSE code update failed: {e}")


def _get_fallback_tickers():
    """Comprehensive fallback list of major Indian stocks (NIFTY 500+)."""
    # This covers NIFTY 50, NIFTY Next 50, and other major stocks
    stocks = [
        # NIFTY 50
        ("RELIANCE", "Reliance Industries"), ("TCS", "Tata Consultancy Services"),
        ("HDFCBANK", "HDFC Bank"), ("INFY", "Infosys"), ("ICICIBANK", "ICICI Bank"),
        ("HINDUNILVR", "Hindustan Unilever"), ("SBIN", "State Bank of India"),
        ("BHARTIARTL", "Bharti Airtel"), ("ITC", "ITC Limited"),
        ("KOTAKBANK", "Kotak Mahindra Bank"), ("LT", "Larsen & Toubro"),
        ("AXISBANK", "Axis Bank"), ("WIPRO", "Wipro"), ("SUNPHARMA", "Sun Pharma"),
        ("TATAMOTORS", "Tata Motors"), ("TITAN", "Titan Company"),
        ("BAJFINANCE", "Bajaj Finance"), ("ASIANPAINT", "Asian Paints"),
        ("MARUTI", "Maruti Suzuki"), ("NESTLEIND", "Nestle India"),
        ("HCLTECH", "HCL Technologies"), ("ULTRACEMCO", "UltraTech Cement"),
        ("NTPC", "NTPC Limited"), ("POWERGRID", "Power Grid Corporation"),
        ("M&M", "Mahindra & Mahindra"), ("TECHM", "Tech Mahindra"),
        ("ONGC", "Oil & Natural Gas Corp"), ("ADANIENT", "Adani Enterprises"),
        ("ADANIPORTS", "Adani Ports"), ("TATASTEEL", "Tata Steel"),
        ("BAJAJFINSV", "Bajaj Finserv"), ("JSWSTEEL", "JSW Steel"),
        ("DRREDDY", "Dr. Reddy's Laboratories"), ("CIPLA", "Cipla"),
        ("COALINDIA", "Coal India"), ("BPCL", "Bharat Petroleum"),
        ("DIVISLAB", "Divi's Laboratories"), ("GRASIM", "Grasim Industries"),
        ("EICHERMOT", "Eicher Motors"), ("APOLLOHOSP", "Apollo Hospitals"),
        ("HEROMOTOCO", "Hero MotoCorp"), ("BRITANNIA", "Britannia Industries"),
        ("TATACONSUM", "Tata Consumer Products"), ("INDUSINDBK", "IndusInd Bank"),
        ("HINDALCO", "Hindalco Industries"), ("SBILIFE", "SBI Life Insurance"),
        ("HDFCLIFE", "HDFC Life Insurance"), ("BAJAJ-AUTO", "Bajaj Auto"),
        ("LTIM", "LTIMindtree"), ("SHRIRAMFIN", "Shriram Finance"),
        # NIFTY Next 50
        ("BANKBARODA", "Bank of Baroda"), ("IOC", "Indian Oil Corporation"),
        ("VEDL", "Vedanta"), ("GAIL", "GAIL India"), ("HAVELLS", "Havells India"),
        ("DLF", "DLF Limited"), ("GODREJCP", "Godrej Consumer Products"),
        ("PIDILITIND", "Pidilite Industries"), ("DABUR", "Dabur India"),
        ("MARICO", "Marico Limited"), ("AMBUJACEM", "Ambuja Cements"),
        ("ACC", "ACC Limited"), ("BERGEPAINT", "Berger Paints"),
        ("TRENT", "Trent Limited"), ("ZOMATO", "Zomato Limited"),
        ("PAYTM", "One 97 Communications"), ("NYKAA", "FSN E-Commerce (Nykaa)"),
        ("DMART", "Avenue Supermarts"), ("SIEMENS", "Siemens India"),
        ("ABB", "ABB India"), ("BOSCHLTD", "Bosch India"),
        ("COLPAL", "Colgate-Palmolive India"), ("PEL", "Piramal Enterprises"),
        ("MCDOWELL-N", "United Spirits"), ("MUTHOOTFIN", "Muthoot Finance"),
        ("CHOLAFIN", "Cholamandalam Finance"), ("CANBK", "Canara Bank"),
        ("PNB", "Punjab National Bank"), ("FEDERALBNK", "Federal Bank"),
        ("IDFCFIRSTB", "IDFC First Bank"), ("BANDHANBNK", "Bandhan Bank"),
        ("AUBANK", "AU Small Finance Bank"), ("MAXHEALTH", "Max Healthcare"),
        ("TORNTPHARM", "Torrent Pharma"), ("BIOCON", "Biocon"),
        ("ALKEM", "Alkem Laboratories"), ("LUPIN", "Lupin"),
        ("AUROPHARMA", "Aurobindo Pharma"), ("IPCALAB", "IPCA Laboratories"),
        ("GLAXO", "GlaxoSmithKline Pharma"), ("ABBOTINDIA", "Abbott India"),
        # Large & Mid Caps
        ("ADANIGREEN", "Adani Green Energy"), ("ADANIPOWER", "Adani Power"),
        ("ATGL", "Adani Total Gas"), ("RECLTD", "REC Limited"),
        ("PFC", "Power Finance Corp"), ("NHPC", "NHPC Limited"),
        ("IRCTC", "IRCTC"), ("IRFC", "Indian Railway Finance"),
        ("HAL", "Hindustan Aeronautics"), ("BEL", "Bharat Electronics"),
        ("BHEL", "Bharat Heavy Electricals"), ("SAIL", "Steel Authority of India"),
        ("NMDC", "NMDC Limited"), ("NATIONALUM", "National Aluminium"),
        ("HINDZINC", "Hindustan Zinc"), ("JINDALSTEL", "Jindal Steel & Power"),
        ("TATAPOWER", "Tata Power"), ("TORNTPOWER", "Torrent Power"),
        ("CUMMINSIND", "Cummins India"), ("VOLTAS", "Voltas"),
        ("WHIRLPOOL", "Whirlpool India"), ("CROMPTON", "Crompton Greaves"),
        ("POLYCAB", "Polycab India"), ("ASTRAL", "Astral"),
        ("SUPREMEIND", "Supreme Industries"), ("BALKRISIND", "Balkrishna Industries"),
        ("APOLLOTYRE", "Apollo Tyres"), ("MRF", "MRF Limited"),
        ("CEAT", "CEAT Tyres"), ("MOTHERSON", "Motherson Sumi Wiring"),
        ("ESCORTS", "Escorts Kubota"), ("ASHOKLEY", "Ashok Leyland"),
        ("TVSMOTOR", "TVS Motor"), ("BHARATFORG", "Bharat Forge"),
        ("INDIGO", "InterGlobe Aviation"), ("CONCOR", "Container Corporation"),
        ("PAGEIND", "Page Industries"), ("RELAXO", "Relaxo Footwears"),
        ("BATAINDIA", "Bata India"), ("TATACOMM", "Tata Communications"),
        ("PERSISTENT", "Persistent Systems"), ("MPHASIS", "Mphasis"),
        ("COFORGE", "Coforge"), ("LTTS", "L&T Technology Services"),
        ("OFSS", "Oracle Financial Services"), ("NAUKRI", "Info Edge (Naukri)"),
        ("DEEPAKNTR", "Deepak Nitrite"), ("PIIND", "PI Industries"),
        ("UPL", "UPL Limited"), ("AARTIIND", "Aarti Industries"),
        ("SRF", "SRF Limited"), ("FLUOROCHEM", "Gujarat Fluorochemicals"),
        ("SYNGENE", "Syngene International"), ("LALPATHLAB", "Dr. Lal PathLabs"),
        ("METROPOLIS", "Metropolis Healthcare"), ("FORTIS", "Fortis Healthcare"),
        ("STAR", "Star Health Insurance"), ("ICICIPRULI", "ICICI Prudential Life"),
        ("ICICIGI", "ICICI Lombard"), ("HDFCAMC", "HDFC AMC"),
        ("NIACL", "New India Assurance"), ("SBICARD", "SBI Cards"),
        ("MANAPPURAM", "Manappuram Finance"), ("BAJAJHLDNG", "Bajaj Holdings"),
        ("LICHSGFIN", "LIC Housing Finance"), ("CANFINHOME", "Can Fin Homes"),
        ("RVNL", "Rail Vikas Nigam"), ("IREDA", "Indian Renewable Energy Dev"),
        ("SUZLON", "Suzlon Energy"), ("IDEA", "Vodafone Idea"),
        ("YESBANK", "Yes Bank"), ("RBLBANK", "RBL Bank"),
        ("MFSL", "Max Financial Services"), ("LICI", "Life Insurance Corp"),
        ("LODHA", "Macrotech Developers"), ("OBEROIRLTY", "Oberoi Realty"),
        ("GODREJPROP", "Godrej Properties"), ("PHOENIXLTD", "Phoenix Mills"),
        ("PRESTIGE", "Prestige Estates"), ("SOBHA", "Sobha Limited"),
        ("JUBLFOOD", "Jubilant Foodworks"), ("DEVYANI", "Devyani International"),
        ("TATAELXSI", "Tata Elxsi"), ("KPITTECH", "KPIT Technologies"),
        ("DIXON", "Dixon Technologies"), ("KAYNES", "Kaynes Technology"),
        ("AIAENG", "AIA Engineering"), ("SKFINDIA", "SKF India"),
        ("SCHAEFFLER", "Schaeffler India"), ("TIMKEN", "Timken India"),
        ("HONAUT", "Honeywell Automation"), ("3MINDIA", "3M India"),
        ("GRINDWELL", "Grindwell Norton"), ("CARBORUNIV", "Carborundum Universal"),
        ("SUNDARMFIN", "Sundaram Finance"), ("IIFL", "IIFL Finance"),
        ("ANGELONE", "Angel One"), ("BSE", "BSE Limited"),
        ("MCX", "Multi Commodity Exchange"), ("CDSL", "CDSL"),
        ("CAMS", "Computer Age Management"), ("KFINTECH", "KFin Technologies"),
        ("CLEAN", "Clean Science and Technology"), ("HAPPSTMNDS", "Happiest Minds"),
        ("ZYDUSLIFE", "Zydus Lifesciences"), ("MANKIND", "Mankind Pharma"),
        ("JIOFIN", "Jio Financial Services"), ("PAYTM", "Paytm"),
        ("POLICYBZR", "PB Fintech"), ("CARTRADE", "CarTrade Tech"),
        ("DELHIVERY", "Delhivery"), ("MAPMYINDIA", "CE Info Systems"),
        ("CAMPUS", "Campus Activewear"), ("MEDANTA", "Global Health (Medanta)"),
        ("RAINBOW", "Rainbow Children's Medicare"), ("KAYNES", "Kaynes Technology"),
        ("SOLARINDS", "Solar Industries"), ("CENTRALBK", "Central Bank of India"),
        ("UNIONBANK", "Union Bank of India"), ("INDIANB", "Indian Bank"),
        ("MAHABANK", "Bank of Maharashtra"), ("IOB", "Indian Overseas Bank"),
        ("UCOBANK", "UCO Bank"), ("J&KBANK", "Jammu & Kashmir Bank"),
        ("IDBI", "IDBI Bank"),
    ]

    return [
        {"symbol": s, "company_name": n, "exchange": "NSE", "source": "fallback"}
        for s, n in stocks
    ]


def _bulk_insert_tickers(tickers: list):
    """Bulk insert tickers into the database."""
    conn = _get_db()
    cursor = conn.cursor()
    for t in tickers:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO tickers
                    (symbol, company_name, exchange, series, isin, bse_code, industry, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t.get("symbol", ""),
                t.get("company_name", ""),
                t.get("exchange", "NSE"),
                t.get("series", ""),
                t.get("isin", ""),
                t.get("bse_code", ""),
                t.get("industry", ""),
                t.get("source", ""),
            ))
        except Exception:
            pass
    conn.commit()
    conn.close()


def search_tickers(query: str, limit: int = 15) -> list:
    """
    Search tickers from local DB. Matches on:
    - symbol prefix (highest priority)
    - symbol substring
    - company name substring
    Returns up to `limit` results sorted by relevance.
    """
    if not query or len(query) < 1:
        return []

    q = query.strip().upper()
    conn = _get_db()
    cursor = conn.cursor()

    # Exact symbol match first, then prefix, then substring, then name match
    cursor.execute("""
        SELECT symbol, company_name, exchange, sector, industry, bse_code,
            CASE
                WHEN UPPER(symbol) = ? THEN 0
                WHEN UPPER(symbol) LIKE ? THEN 1
                WHEN UPPER(symbol) LIKE ? THEN 2
                WHEN UPPER(company_name) LIKE ? THEN 3
                ELSE 4
            END as relevance
        FROM tickers
        WHERE UPPER(symbol) LIKE ? OR UPPER(company_name) LIKE ?
        ORDER BY relevance, LENGTH(symbol), symbol
        LIMIT ?
    """, (q, f"{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", limit))

    results = [dict(r) for r in cursor.fetchall()]
    conn.close()

    # Remove the relevance column from results
    for r in results:
        r.pop("relevance", None)

    return results


def search_tickers_with_yfinance_fallback(query: str, limit: int = 15) -> list:
    """
    Search local DB first. If fewer than 3 results, supplement with
    yfinance search and cache any new discoveries.
    """
    local_results = search_tickers(query, limit)

    if len(local_results) >= 3:
        return local_results[:limit]

    # Fall back to yfinance search for Indian tickers
    try:
        yf_results = _yfinance_search(query)
        if yf_results:
            _bulk_insert_tickers(yf_results)

            # Re-query to get combined results
            local_results = search_tickers(query, limit)
    except Exception as e:
        logger.warning(f"yfinance search fallback failed: {e}")

    return local_results[:limit]


def _yfinance_search(query: str) -> list:
    """Search Yahoo Finance for Indian tickers and return results."""
    try:
        import yfinance as yf
        import requests

        # Use Yahoo Finance's search API directly for better results
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        url = f"https://query2.finance.yahoo.com/v1/finance/search"
        params = {
            "q": query,
            "quotesCount": 20,
            "newsCount": 0,
            "enableFuzzyQuery": True,
            "quotesQueryId": "tss_match_phrase_query",
        }

        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for quote in data.get("quotes", []):
            symbol = quote.get("symbol", "")
            name = quote.get("longname") or quote.get("shortname", "")
            exchange = quote.get("exchange", "")

            # Filter for Indian stocks (NSE = .NS, BSE = .BO)
            if symbol.endswith(".NS"):
                results.append({
                    "symbol": symbol.replace(".NS", ""),
                    "company_name": name,
                    "exchange": "NSE",
                    "source": "yfinance_search",
                })
            elif symbol.endswith(".BO"):
                results.append({
                    "symbol": symbol.replace(".BO", ""),
                    "company_name": name,
                    "exchange": "BSE",
                    "source": "yfinance_search",
                })

        return results
    except Exception as e:
        logger.warning(f"Yahoo Finance search failed: {e}")
        return []


def get_ticker_count() -> int:
    """Return the number of tickers in the database."""
    conn = _get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as cnt FROM tickers")
    count = cursor.fetchone()["cnt"]
    conn.close()
    return count


def refresh_ticker_db():
    """Force re-download and refresh the ticker database."""
    conn = _get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tickers")
    conn.commit()
    conn.close()
    _seed_all_tickers()


def get_bse_code(symbol: str, exchange: str = "NSE") -> str:
    """
    Look up the BSE scrip code for a given symbol.
    Returns the BSE code string (e.g. '500180' for HDFCBANK) or None.
    """
    conn = _get_db()
    cursor = conn.cursor()

    if exchange.upper() == "BSE":
        # The symbol itself might be a BSE code
        cursor.execute(
            "SELECT bse_code FROM tickers WHERE symbol = ? AND exchange = 'BSE'",
            (symbol.upper(),)
        )
        row = cursor.fetchone()
        conn.close()
        return row["bse_code"] if row and row["bse_code"] else symbol

    # For NSE symbols, look up the bse_code
    cursor.execute(
        "SELECT bse_code FROM tickers WHERE UPPER(symbol) = ? AND exchange = 'NSE' AND bse_code IS NOT NULL AND bse_code != ''",
        (symbol.upper(),)
    )
    row = cursor.fetchone()
    conn.close()
    return row["bse_code"] if row else None
