#!/usr/bin/env python3
"""
Earnings Results — recent quarterly earnings for portfolio stocks.
Fetches income statements from FMP for each PORTFOLIO_TICKER, filters
to those that reported within the last 60 days, then enriches with
post-earnings price reaction and company profile.

Endpoint: GET /api/earnings-results
Cache TTL: 600 seconds (10 min)
"""
import json
import os
import time
import datetime
import threading
import urllib.request
import urllib.error
import concurrent.futures

# ── API Keys ──────────────────────────────────────────────
FMP_KEY = os.environ.get("FMP_KEY", "EINiL3Pzp1f0YjvQgcnm8t3hBBShCdMd")

# ── Portfolio ─────────────────────────────────────────────
PORTFOLIO_TICKERS = [
    "SLV","IREN","CIFR","NBIS","SOFI","IAU","APLD","RKLB","TMDX",
    "WULF","PALL","DCTH","WGMI","GOOGL","VRT","NVDA","ACHR","FLNC",
    "GLXY","MRVL","PLTR","HUT","JOBY","BITF","CLSK","NET","PYPL",
    "SERV","UUUU","COIN","SHOP","CRWV","HIMS","MP","HOOD","NU"
]

# ETFs/trusts that won't have income statements — skip gracefully
ETF_SET = {"SLV", "IAU", "PALL", "WGMI", "GLD", "GDX", "GDXJ"}

# How many days back counts as "recently reported"
RECENT_DAYS = 60

# ── Cache ─────────────────────────────────────────────────
_cache_lock          = threading.Lock()
_cache               = {"data": None, "ts": 0}
_CACHE_TTL           = 600   # 10 minutes
_refresh_in_progress = False


# ── HTTP helpers ──────────────────────────────────────────
def _http_get(url, timeout=13):
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _safe_json(url, timeout=13):
    try:
        return json.loads(_http_get(url, timeout))
    except Exception:
        return None


# ── Date helpers ──────────────────────────────────────────
def _parse_date(s):
    """Parse YYYY-MM-DD into a date object, or return None."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _date_str(d, delta_days=0):
    """Return a date string YYYY-MM-DD for d + delta_days."""
    return (d + datetime.timedelta(days=delta_days)).strftime("%Y-%m-%d")


# ── Per-ticker fetchers ───────────────────────────────────

def _fetch_income_statement(ticker):
    """Fetch the latest 2 quarterly income statement entries."""
    url = (
        f"https://financialmodelingprep.com/stable/income-statement"
        f"?symbol={ticker}&period=quarter&limit=2&apikey={FMP_KEY}"
    )
    data = _safe_json(url, timeout=13)
    if isinstance(data, list):
        return data
    return []


def _fetch_price_history(ticker, report_date):
    """
    Fetch daily EOD price history around the earnings report date.
    Returns a dict keyed by date string "YYYY-MM-DD".
    """
    date_from = _date_str(report_date, -3)
    date_to   = _date_str(report_date,  4)
    url = (
        f"https://financialmodelingprep.com/stable/historical-price-eod/full"
        f"?symbol={ticker}&from={date_from}&to={date_to}&apikey={FMP_KEY}"
    )
    data = _safe_json(url, timeout=13)
    prices = {}
    if not data:
        return prices
    # Response can be list or dict with "historical" key
    historical = data if isinstance(data, list) else data.get("historical", [])
    for row in (historical or []):
        d = row.get("date", "")
        if d:
            prices[d[:10]] = {
                "open":  row.get("open",  0),
                "close": row.get("close", 0),
                "high":  row.get("high",  0),
                "low":   row.get("low",   0),
            }
    return prices


def _fetch_profile(ticker):
    """Fetch company profile — name, sector, isEtf."""
    url = (
        f"https://financialmodelingprep.com/stable/profile"
        f"?symbol={ticker}&apikey={FMP_KEY}"
    )
    data = _safe_json(url, timeout=13)
    if isinstance(data, list) and data:
        p = data[0]
        return {
            "name":    p.get("companyName", ticker),
            "sector":  p.get("sector",      ""),
            "isEtf":   p.get("isEtf",       False),
        }
    return {"name": ticker, "sector": "", "isEtf": False}


# ── Post-earnings price reaction ──────────────────────────

def _price_reaction(prices, report_date):
    """
    Calculate the post-earnings price move.
    Uses close on the day before report vs close on the day after report
    (or closest available trading days within a ±3 day window).
    Returns (before_close, after_close, reaction_pct) or (None, None, None).
    """
    sorted_dates = sorted(prices.keys())
    report_str   = report_date.strftime("%Y-%m-%d")

    # Find the last trading day BEFORE the report date
    before_close = None
    before_date  = None
    for d in sorted_dates:
        if d < report_str:
            before_date  = d
            before_close = prices[d]["close"]
        else:
            break

    # Find the first trading day ON or AFTER the report date
    after_close = None
    after_date  = None
    for d in sorted_dates:
        if d >= report_str:
            after_date  = d
            after_close = prices[d]["close"]
            break

    if before_close and after_close and before_close != 0:
        pct = round((after_close - before_close) / before_close * 100, 2)
        return before_close, after_close, pct, before_date, after_date
    return None, None, None, before_date, after_date


# ── Revenue growth helpers ────────────────────────────────

def _pct_change(current, previous):
    """Safe percentage change."""
    if previous is None or previous == 0 or current is None:
        return None
    return round((current - previous) / abs(previous) * 100, 2)


# ── Core per-ticker processor ─────────────────────────────

def _process_ticker(ticker):
    """
    Full pipeline for one ticker: fetch IS, check recency, enrich.
    Returns a result dict or None if not recently reported / ETF / error.
    """
    if ticker in ETF_SET:
        return None

    try:
        statements = _fetch_income_statement(ticker)
    except Exception as e:
        print(f"[EarningsResults] {ticker} income-statement error: {e}")
        return None

    if not statements:
        return None

    latest = statements[0]
    report_date_str = latest.get("date", "")
    report_date = _parse_date(report_date_str)
    if not report_date:
        return None

    # Check if within RECENT_DAYS
    today = datetime.date.today()
    if (today - report_date).days > RECENT_DAYS:
        return None

    # ── Income metrics ──────────────────────────────────
    revenue      = latest.get("revenue")
    eps          = latest.get("eps")
    eps_diluted  = latest.get("epsDiluted")
    net_income   = latest.get("netIncome")
    net_margin   = latest.get("netIncomeRatio")
    gross_profit = latest.get("grossProfit")
    gross_margin = latest.get("grossProfitRatio")
    ebitda       = latest.get("ebitda")
    period       = latest.get("period", "")
    fiscal_year  = latest.get("calendarYear", "")

    # Revenue QoQ growth
    revenue_qoq = None
    prev = statements[1] if len(statements) >= 2 else None
    if prev:
        revenue_qoq = _pct_change(revenue, prev.get("revenue"))

    # ── Profile (name, sector) ──────────────────────────
    try:
        profile = _fetch_profile(ticker)
    except Exception:
        profile = {"name": ticker, "sector": "", "isEtf": False}

    # Skip if FMP says it's an ETF
    if profile.get("isEtf"):
        return None

    # ── Price reaction ──────────────────────────────────
    before_close = after_close = reaction_pct = None
    before_date = after_date = None
    try:
        prices = _fetch_price_history(ticker, report_date)
        if prices:
            before_close, after_close, reaction_pct, before_date, after_date = \
                _price_reaction(prices, report_date)
    except Exception as e:
        print(f"[EarningsResults] {ticker} price history error: {e}")

    return {
        "symbol":        ticker,
        "name":          profile.get("name", ticker),
        "sector":        profile.get("sector", ""),
        "reportDate":    report_date_str,
        "period":        period,
        "fiscalYear":    fiscal_year,
        # Revenue
        "revenue":       revenue,
        "revenueQoQ":    revenue_qoq,
        # Profitability
        "grossProfit":   gross_profit,
        "grossMargin":   round(gross_margin * 100, 2) if gross_margin is not None else None,
        "netIncome":     net_income,
        "netMargin":     round(net_margin * 100, 2)   if net_margin   is not None else None,
        "ebitda":        ebitda,
        # EPS
        "eps":           eps,
        "epsDiluted":    eps_diluted,
        # Price reaction
        "priceBeforeEarnings":  before_close,
        "priceAfterEarnings":   after_close,
        "priceReactionPct":     reaction_pct,
        "priceDateBefore":      before_date,
        "priceDateAfter":       after_date,
    }


# ── Core builder ─────────────────────────────────────────

def _build_earnings_results():
    """Fetch and process all portfolio tickers in parallel."""
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_process_ticker, t): t for t in PORTFOLIO_TICKERS}
        for future in concurrent.futures.as_completed(futures, timeout=120):
            ticker = futures[future]
            try:
                result = future.result(timeout=30)
                if result:
                    results.append(result)
            except Exception as e:
                print(f"[EarningsResults] {ticker} processing error: {e}")

    # Sort by report date descending (most recent first)
    results.sort(key=lambda r: r.get("reportDate", ""), reverse=True)

    return {
        "updated_at":    datetime.datetime.utcnow().isoformat() + "Z",
        "count":         len(results),
        "earnings":      results,
        "status":        "ok",
    }


# ── Background refresh ────────────────────────────────────

def _trigger_refresh():
    """Start a background thread to refresh the cache if not already running."""
    global _refresh_in_progress
    if _refresh_in_progress:
        return
    _refresh_in_progress = True

    def _do():
        global _refresh_in_progress
        try:
            fresh = _build_earnings_results()
            with _cache_lock:
                _cache["data"] = fresh
                _cache["ts"]   = time.time()
            print(f"[EarningsResults] Cache refreshed — {fresh.get('count', 0)} stocks recently reported")
        except Exception as e:
            print(f"[EarningsResults] Refresh failed: {e}")
        finally:
            _refresh_in_progress = False

    t = threading.Thread(target=_do, daemon=True)
    t.start()


# ── Public export ─────────────────────────────────────────

def get_earnings_results():
    """
    Return cached earnings results, or trigger a warm-up if the cache
    is empty, or kick off a background refresh if the data is stale.
    """
    with _cache_lock:
        cached = _cache["data"]

    if not cached:
        _trigger_refresh()
        return {
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "count":      0,
            "earnings":   [],
            "status":     "warming",
        }

    # Trigger background refresh if stale
    with _cache_lock:
        age = time.time() - _cache["ts"]
    if age > _CACHE_TTL:
        _trigger_refresh()

    return cached
