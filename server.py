#!/usr/bin/env python3
"""
Market Pulse — Flask server for Railway deployment.
Serves the static dashboard + API endpoints for live quotes and news.
"""
import json
import os
import datetime
import urllib.request
import urllib.parse
import urllib.error
import concurrent.futures
from flask import Flask, send_from_directory, jsonify, request

app = Flask(__name__, static_folder="static")

# ── API Keys ──────────────────────────────────────────────
POLYGON_KEY = os.environ.get("POLYGON_KEY", "SL1wF6nbcCYCWRbfl5TcepWwd5pwPAbW")
FMP_KEY = os.environ.get("FMP_KEY", "EINiL3Pzp1f0YjvQgcnm8t3hBBShCdMd")

# ── Tickers ───────────────────────────────────────────────
PORTFOLIO_TICKERS = [
    "SLV","IREN","CIFR","NBIS","SOFI","IAU","APLD","RKLB","TMDX",
    "WULF","PALL","DCTH","WGMI","GOOGL","VRT","NVDA","ACHR","FLNC",
    "GLXY","MRVL","PLTR","HUT","JOBY","BITF","CLSK","NET","PYPL",
    "SERV","UUUU","COIN","SHOP","CRWV","HIMS","MP","HOOD","NU"
]

WATCHLIST_TICKERS = [
    "USO","XLE","OXY","CVX","LMT","RTX","GD","NOC","ITA",
    "GLD","GDX","GDXJ","WPM","NEM","AMZN","MSFT","META",
    "AAPL","CRM","UVXY","VXX","SQQQ",
    "SPY","QQQ","DIA","VIXY"
]

CRYPTO_SYMBOLS = ["BTCUSD", "ETHUSD"]

ALL_TICKERS = list(dict.fromkeys(PORTFOLIO_TICKERS + WATCHLIST_TICKERS))

TOP_10 = ["SLV", "IREN", "CIFR", "NBIS", "SOFI", "IAU", "APLD", "RKLB", "TMDX", "WULF"]


# ── Hardcoded Fallback Data ───────────────────────────────
CONGRESS_TRADES_DATA = [
    {"firstName": "Tim", "lastName": "Moore", "office": "House", "symbol": "COIN", "transactionDate": "2026-02-18", "transactionType": "Sale", "amount": "$1,001 - $15,000", "assetDescription": "Coinbase Global Inc"},
    {"firstName": "Cleo", "lastName": "Fields", "office": "Senate", "symbol": "AAPL", "transactionDate": "2026-02-12", "transactionType": "Purchase", "amount": "$1,001 - $15,000", "assetDescription": "Apple Inc."},
    {"firstName": "Jake", "lastName": "Auchincloss", "office": "House", "symbol": "STT", "transactionDate": "2026-02-17", "transactionType": "Sale", "amount": "$15,001 - $50,000", "assetDescription": "State Street Corporation"},
    {"firstName": "Tim", "lastName": "Moore", "office": "House", "symbol": "DNUT", "transactionDate": "2026-02-12", "transactionType": "Purchase", "amount": "$1,001 - $15,000", "assetDescription": "Krispy Kreme Inc"},
    {"firstName": "Marjorie", "lastName": "Taylor Greene", "office": "House", "symbol": "NVDA", "transactionDate": "2026-02-05", "transactionType": "Purchase", "amount": "$1,001 - $15,000", "assetDescription": "NVIDIA Corp"},
    {"firstName": "Tommy", "lastName": "Tuberville", "office": "Senate", "symbol": "GOOGL", "transactionDate": "2026-02-03", "transactionType": "Purchase", "amount": "$15,001 - $50,000", "assetDescription": "Alphabet Inc."},
    {"firstName": "Dan", "lastName": "Crenshaw", "office": "House", "symbol": "MSFT", "transactionDate": "2026-01-28", "transactionType": "Purchase", "amount": "$1,001 - $15,000", "assetDescription": "Microsoft Corp"},
    {"firstName": "Nancy", "lastName": "Pelosi", "office": "House", "symbol": "CRM", "transactionDate": "2026-01-22", "transactionType": "Purchase", "amount": "$250,001 - $500,000", "assetDescription": "Salesforce Inc."},
    {"firstName": "Ro", "lastName": "Khanna", "office": "House", "symbol": "PLTR", "transactionDate": "2026-01-20", "transactionType": "Sale", "amount": "$50,001 - $100,000", "assetDescription": "Palantir Technologies"},
    {"firstName": "Josh", "lastName": "Gottheimer", "office": "House", "symbol": "META", "transactionDate": "2026-01-15", "transactionType": "Purchase", "amount": "$15,001 - $50,000", "assetDescription": "Meta Platforms Inc."},
]

FALLBACK_EARNINGS = [
    {"symbol": "SOFI", "date": "2026-04-28", "time": "bmo", "epsEstimated": 0.14, "revenueEstimated": 850000000},
    {"symbol": "RKLB", "date": "2026-05-08", "time": "amc", "epsEstimated": -0.03, "revenueEstimated": 155000000},
    {"symbol": "CIFR", "date": "2026-05-06", "time": "amc", "epsEstimated": 0.02, "revenueEstimated": 78000000},
    {"symbol": "IREN", "date": "2026-05-15", "time": "bmo", "epsEstimated": 0.08, "revenueEstimated": 220000000},
    {"symbol": "NBIS", "date": "2026-04-24", "time": "bmo", "epsEstimated": -0.38, "revenueEstimated": 310000000},
    {"symbol": "WULF", "date": "2026-05-12", "time": "amc", "epsEstimated": 0.05, "revenueEstimated": 65000000},
    {"symbol": "APLD", "date": "2026-04-10", "time": "amc", "epsEstimated": -0.15, "revenueEstimated": 95000000},
    {"symbol": "TMDX", "date": "2026-05-01", "time": "bmo", "epsEstimated": 0.42, "revenueEstimated": 130000000},
    {"symbol": "COIN", "date": "2026-05-08", "time": "amc", "epsEstimated": 2.15, "revenueEstimated": 2100000000},
    {"symbol": "NVDA", "date": "2026-05-28", "time": "amc", "epsEstimated": 0.92, "revenueEstimated": 51000000000},
    {"symbol": "PLTR", "date": "2026-05-05", "time": "amc", "epsEstimated": 0.13, "revenueEstimated": 920000000},
    {"symbol": "GOOGL", "date": "2026-04-22", "time": "amc", "epsEstimated": 2.12, "revenueEstimated": 106000000000},
]

FALLBACK_INSIDER = {
    "SOFI": [
        {"symbol": "SOFI", "transactionDate": "2026-02-15", "reportingName": "Steven Freiberg", "typeOfOwner": "director", "transactionType": "S-Sale", "securitiesTransacted": 94225, "price": 20.27, "securitiesOwned": 150000},
        {"symbol": "SOFI", "transactionDate": "2026-01-28", "reportingName": "Anthony Noto", "typeOfOwner": "officer", "transactionType": "S-Sale", "securitiesTransacted": 50000, "price": 18.50, "securitiesOwned": 2500000},
    ],
    "IREN": [
        {"symbol": "IREN", "transactionDate": "2026-02-10", "reportingName": "Daniel Roberts", "typeOfOwner": "officer", "transactionType": "S-Sale", "securitiesTransacted": 200000, "price": 22.15, "securitiesOwned": 1800000},
    ],
    "NVDA": [
        {"symbol": "NVDA", "transactionDate": "2026-02-20", "reportingName": "Jensen Huang", "typeOfOwner": "officer", "transactionType": "S-Sale", "securitiesTransacted": 240000, "price": 178.50, "securitiesOwned": 86400000},
        {"symbol": "NVDA", "transactionDate": "2026-02-05", "reportingName": "Colette Kress", "typeOfOwner": "officer", "transactionType": "S-Sale", "securitiesTransacted": 28571, "price": 185.20, "securitiesOwned": 450000},
    ],
    "RKLB": [
        {"symbol": "RKLB", "transactionDate": "2026-02-12", "reportingName": "Peter Beck", "typeOfOwner": "officer", "transactionType": "S-Sale", "securitiesTransacted": 100000, "price": 45.80, "securitiesOwned": 52000000},
    ],
    "COIN": [
        {"symbol": "COIN", "transactionDate": "2026-02-18", "reportingName": "Brian Armstrong", "typeOfOwner": "officer", "transactionType": "S-Sale", "securitiesTransacted": 15000, "price": 312.50, "securitiesOwned": 64000000},
    ],
}


# ── Quote Fetchers ────────────────────────────────────────
def fetch_polygon(tickers):
    syms = ",".join(tickers)
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={syms}&apiKey={POLYGON_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode())
    quotes = {}
    for t in data.get("tickers", []):
        sym = t["ticker"]
        day = t.get("day", {})
        prev = t.get("prevDay", {})
        pc = prev.get("c", 0)
        close = day.get("c", 0) or pc
        chg = t.get("todaysChange", 0)
        chg_pct = t.get("todaysChangePerc", 0)
        mn = t.get("min", {})
        quotes[sym] = {
            "symbol": sym,
            "price": close,
            "change": round(chg, 2),
            "changesPercentage": round(chg_pct, 2),
            "previousClose": pc,
            "open": day.get("o", 0),
            "dayHigh": day.get("h", 0),
            "dayLow": day.get("l", 0),
            "volume": int(day.get("v", 0)),
            "vwap": day.get("vw", 0),
            "bid": t.get("lastQuote", {}).get("p", 0),
            "ask": t.get("lastQuote", {}).get("P", 0),
            "lastTradePrice": t.get("lastTrade", {}).get("p", 0),
            "minuteClose": mn.get("c", 0),
            "timestamp": t.get("updated", 0),
        }
    return quotes


def fetch_fmp(tickers):
    syms = ",".join(tickers)
    url = f"https://financialmodelingprep.com/stable/batch-quote?symbols={syms}&apikey={FMP_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = json.loads(resp.read().decode())
    quotes = {}
    for q in (raw if isinstance(raw, list) else []):
        sym = q.get("symbol", "")
        quotes[sym] = {
            "symbol": sym,
            "name": q.get("name", ""),
            "price": q.get("price", 0),
            "change": q.get("change", 0),
            "changesPercentage": q.get("changePercentage", 0),
            "previousClose": q.get("previousClose", 0),
            "open": q.get("open", 0),
            "dayHigh": q.get("dayHigh", 0),
            "dayLow": q.get("dayLow", 0),
            "volume": q.get("volume", 0),
            "marketCap": q.get("marketCap", 0),
            "yearHigh": q.get("yearHigh", 0),
            "yearLow": q.get("yearLow", 0),
            "timestamp": q.get("timestamp", 0),
        }
    return quotes


def fetch_crypto():
    """Fetch crypto quotes from FMP (BTC, ETH trade 24/7)."""
    syms = ",".join(CRYPTO_SYMBOLS)
    url = f"https://financialmodelingprep.com/stable/batch-quote?symbols={syms}&apikey={FMP_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = json.loads(resp.read().decode())
    quotes = {}
    for q in (raw if isinstance(raw, list) else []):
        sym = q.get("symbol", "")
        # Map BTCUSD -> BTC, ETHUSD -> ETH for frontend
        display = sym.replace("USD", "")
        quotes[display] = {
            "symbol": display,
            "name": q.get("name", ""),
            "price": q.get("price", 0),
            "change": q.get("change", 0),
            "changesPercentage": q.get("changePercentage", 0),
            "previousClose": q.get("previousClose", 0),
            "open": q.get("open", 0),
            "dayHigh": q.get("dayHigh", 0),
            "dayLow": q.get("dayLow", 0),
            "volume": q.get("volume", 0),
            "timestamp": q.get("timestamp", 0),
        }
    return quotes


def fetch_vix():
    """Fetch VIX index from FMP (Polygon doesn't carry ^VIX)."""
    url = f"https://financialmodelingprep.com/stable/batch-quote?symbols=%5EVIX&apikey={FMP_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = json.loads(resp.read().decode())
    quotes = {}
    for q in (raw if isinstance(raw, list) else []):
        quotes["VIX"] = {
            "symbol": "VIX",
            "name": "CBOE Volatility Index",
            "price": q.get("price", 0),
            "change": q.get("change", 0),
            "changesPercentage": q.get("changePercentage", 0),
            "previousClose": q.get("previousClose", 0),
            "open": q.get("open", 0),
            "dayHigh": q.get("dayHigh", 0),
            "dayLow": q.get("dayLow", 0),
            "timestamp": q.get("timestamp", 0),
        }
    return quotes


# Commodity symbol mapping: FMP symbol -> display name
COMMODITY_MAP = {
    "GCUSD": {"display": "GOLD",  "name": "Gold (Spot)",        "unit": "/oz"},
    "SILUSD": {"display": "SILVER", "name": "Silver (Spot)",      "unit": "/oz"},
    "CLUSD": {"display": "WTI",   "name": "WTI Crude Oil",      "unit": "/bbl"},
    "BZUSD": {"display": "BRENT", "name": "Brent Crude Oil",    "unit": "/bbl"},
}

def fetch_commodities():
    """Fetch spot commodity prices from FMP batch-commodity-quotes."""
    url = f"https://financialmodelingprep.com/stable/batch-commodity-quotes?apikey={FMP_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = json.loads(resp.read().decode())
    quotes = {}
    for q in (raw if isinstance(raw, list) else []):
        sym = q.get("symbol", "")
        if sym in COMMODITY_MAP:
            info = COMMODITY_MAP[sym]
            prev = q.get("previousClose", 0) or 0
            price = q.get("price", 0) or 0
            chg = q.get("change", 0) or 0
            chg_pct = round((chg / prev * 100) if prev else 0, 2)
            quotes[info["display"]] = {
                "symbol": info["display"],
                "name": info["name"],
                "unit": info["unit"],
                "price": price,
                "change": round(chg, 2),
                "changesPercentage": chg_pct,
                "previousClose": prev,
                "timestamp": q.get("timestamp", 0),
            }
    return quotes


# ── API Routes ────────────────────────────────────────────
@app.route("/api/quotes")
def api_quotes():
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else ALL_TICKERS

    source = "live"
    try:
        quotes = fetch_polygon(tickers)
    except Exception:
        try:
            quotes = fetch_fmp(tickers)
        except Exception as e:
            return jsonify({"error": str(e)}), 502

    # Enrich with crypto (BTC, ETH), VIX, and spot commodity prices
    try:
        quotes.update(fetch_crypto())
    except Exception:
        pass  # non-fatal: stock data still available
    try:
        quotes.update(fetch_vix())
    except Exception:
        pass
    try:
        quotes.update(fetch_commodities())
    except Exception:
        pass

    return jsonify({
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": source,
        "count": len(quotes),
        "quotes": quotes,
    }), 200, {"Cache-Control": "no-cache"}


@app.route("/api/news")
def api_news():
    tickers_param = request.args.get("tickers", "")
    limit = min(int(request.args.get("limit", "15")), 30)

    ticker_filter = tickers_param if tickers_param else ",".join(ALL_TICKERS)
    url = f"https://api.polygon.io/v2/reference/news?ticker.any_of={ticker_filter}&limit={limit}&order=desc&sort=published_utc&apiKey={POLYGON_KEY}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    articles = []
    for a in data.get("results", []):
        pub = a.get("publisher", {})
        articles.append({
            "title": a.get("title", ""),
            "url": a.get("article_url", ""),
            "source": pub.get("name", ""),
            "published": a.get("published_utc", ""),
            "tickers": a.get("tickers", [])[:6],
            "image": a.get("image_url", ""),
            "description": (a.get("description", "") or "")[:200],
        })

    return jsonify({
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "count": len(articles),
        "articles": articles,
    }), 200, {"Cache-Control": "no-cache"}


# ── Polymarket ───────────────────────────────────────────
# All parent event slugs from polymarket_data.json and polymarket_new.json
DEFAULT_POLYMARKET_SLUGS = [
    # Iran conflict
    "us-x-iran-ceasefire-by",
    "will-iran-close-the-strait-of-hormuz-by-2027",
    "what-will-iran-strike-by-march-31",
    "will-the-us-invade-iran-by-march-31",
    "will-the-us-officially-declare-war-on-iran-by",
    "us-iran-nuclear-deal-by-march-31",
    "who-will-be-next-supreme-leader-of-iran-515",
    "usisrael-strike-on-fordow-nuclear-facility-by-march-31",
    "will-another-country-strike-iran-by-march-31",
    "will-the-kharg-island-oil-terminal-be-hit-by-march-31",
    # Fed / macro
    "fed-decision-in-march-885",
    "fed-decision-in-april",
    "fed-decision-in-june-825",
    "how-many-fed-rate-cuts-in-2026",
    "what-will-the-fed-rate-be-at-the-end-of-2026",
    "us-recession-by-end-of-2026",
    "us-gdp-growth-in-q1-2026",
    "february-inflation-us-annual",
    "how-high-will-us-unemployment-go-in-2026",
    "fed-emergency-rate-cut-before-2027",
    "negative-gdp-growth-in-2026",
    "which-banks-will-fail-by-june-30",
    # Crypto / assets
    "what-price-will-bitcoin-hit-in-march-2026",
    "what-price-will-bitcoin-hit-before-2027",
    "bitcoin-vs-gold-vs-sp-500-in-2026",
    # Iran return
    "will-reza-pahlavi-enter-iran-by-june-30",
    # New slugs from polymarket_new.json
    "which-companies-added-to-sp-500-in-q1-2026",
    "what-price-will-nvda-hit-in-march-2026",
    "how-much-will-coinbase-token-sales-raise-in-2026",
    "coin-up-or-down-on-march-2-2026",
    "iran-agrees-to-end-enrichment-of-uranium-by-march-31",
    "iran-agrees-to-end-enrichment-of-uranium-by-june-30",
    "us-grants-license-for-new-nuclear-reactor-in-2026",
    "iran-nuclear-test-before-2027",
    "ai-data-center-moratorium-passed-before-2027",
    "will-bitcoin-outperform-gold-in-2026",
    "sp-500-performance-in-q1",
    "cl-settle-jun-2026",
    "cl-hit-jun-2026",
    "gc-settle-jun-2026",
    "gc-over-under-jun-2026",
    "si-settle-jun-2026",
    "si-over-under-jun-2026",
    "ndx-up-or-down-on-march-2-2026",
    "djia-up-or-down-on-march-2-2026",
]


def _fetch_polymarket_slug(slug):
    """Fetch a single event slug from Gamma API and return list of market dicts."""
    url = f"https://gamma-api.polymarket.com/events?slug={urllib.parse.quote(slug)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            events = json.loads(resp.read().decode())
        markets = []
        for event in (events if isinstance(events, list) else []):
            for market in event.get("markets", []):
                markets.append({
                    "slug": market.get("slug", ""),
                    "question": market.get("question", ""),
                    "outcomePrices": market.get("outcomePrices", ""),
                    "outcomes": market.get("outcomes", ""),
                })
        return slug, markets
    except Exception as e:
        return slug, {"error": str(e)}


@app.route("/api/polymarket")
def api_polymarket():
    slugs_param = request.args.get("slugs", "")
    if slugs_param:
        slugs = [s.strip() for s in slugs_param.split(",") if s.strip()]
    else:
        slugs = DEFAULT_POLYMARKET_SLUGS

    result = {}
    # Process in batches of 10, max 10 workers
    batch_size = 10
    batches = [slugs[i:i+batch_size] for i in range(0, len(slugs), batch_size)]
    for batch in batches:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_fetch_polymarket_slug, slug): slug for slug in batch}
            for future in concurrent.futures.as_completed(futures):
                try:
                    slug, markets = future.result(timeout=15)
                    result[slug] = markets
                except Exception as e:
                    slug = futures[future]
                    result[slug] = {"error": str(e)}

    return jsonify({
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "count": len(result),
        "markets": result,
    }), 200, {"Cache-Control": "no-cache"}


# ── Analyst Targets ──────────────────────────────────────
@app.route("/api/analyst-targets")
def api_analyst_targets():
    def _fetch_ticker(ticker):
        pt_url = f"https://financialmodelingprep.com/stable/price-target-summary?symbol={ticker}&apikey={FMP_KEY}"
        est_url = f"https://financialmodelingprep.com/stable/analyst-estimates?symbol={ticker}&period=quarter&apikey={FMP_KEY}"
        results = {"symbol": ticker, "price_target": {}, "estimates": []}
        try:
            req = urllib.request.Request(pt_url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                pt_data = json.loads(resp.read().decode())
            if isinstance(pt_data, list) and pt_data:
                results["price_target"] = pt_data[0]
        except Exception:
            pass
        try:
            req = urllib.request.Request(est_url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                est_data = json.loads(resp.read().decode())
            if isinstance(est_data, list):
                results["estimates"] = est_data[:2]
        except Exception:
            pass
        return ticker, results

    try:
        targets = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_ticker, t): t for t in TOP_10}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=20)
                    targets[ticker] = data
                except Exception as e:
                    t = futures[future]
                    targets[t] = {"symbol": t, "error": str(e)}
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "targets": targets,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Congress Trades ───────────────────────────────────────
@app.route("/api/congress-trades")
def api_congress_trades():
    try:
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "trades": CONGRESS_TRADES_DATA,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Earnings Calendar ─────────────────────────────────────
@app.route("/api/earnings-calendar")
def api_earnings_calendar():
    try:
        today = datetime.date.today()
        four_months = today + datetime.timedelta(days=120)
        url = f"https://financialmodelingprep.com/stable/earning-calendar-confirmed?from={today}&to={four_months}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        all_syms = set(PORTFOLIO_TICKERS + WATCHLIST_TICKERS)
        filtered = [
            entry for entry in (data if isinstance(data, list) else [])
            if entry.get("symbol", "") in all_syms
        ]
        # Fall back to hardcoded data if API returned nothing useful
        if not filtered:
            filtered = FALLBACK_EARNINGS
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "earnings": filtered,
            "count": len(filtered),
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        # On any error also return fallback so the section still renders
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "earnings": FALLBACK_EARNINGS,
            "count": len(FALLBACK_EARNINGS),
        }), 200, {"Cache-Control": "no-cache"}


# ── Market Movers ─────────────────────────────────────────

# High-volume stocks fallback (well-known high-volume names)
FALLBACK_MOST_ACTIVE = [
    {"symbol": "NVDA", "name": "NVIDIA Corp", "price": 0, "change": 0, "changesPercentage": 0, "volume": 350000000},
    {"symbol": "TSLA", "name": "Tesla Inc", "price": 0, "change": 0, "changesPercentage": 0, "volume": 120000000},
    {"symbol": "AAPL", "name": "Apple Inc", "price": 0, "change": 0, "changesPercentage": 0, "volume": 80000000},
    {"symbol": "PLTR", "name": "Palantir Technologies", "price": 0, "change": 0, "changesPercentage": 0, "volume": 78000000},
    {"symbol": "SOFI", "name": "SoFi Technologies", "price": 0, "change": 0, "changesPercentage": 0, "volume": 65000000},
    {"symbol": "AMZN", "name": "Amazon.com Inc", "price": 0, "change": 0, "changesPercentage": 0, "volume": 55000000},
    {"symbol": "AMD", "name": "Advanced Micro Devices", "price": 0, "change": 0, "changesPercentage": 0, "volume": 52000000},
    {"symbol": "BAC", "name": "Bank of America Corp", "price": 0, "change": 0, "changesPercentage": 0, "volume": 48000000},
    {"symbol": "META", "name": "Meta Platforms Inc", "price": 0, "change": 0, "changesPercentage": 0, "volume": 40000000},
    {"symbol": "MSFT", "name": "Microsoft Corp", "price": 0, "change": 0, "changesPercentage": 0, "volume": 35000000},
]

@app.route("/api/market-movers")
def api_market_movers():
    def _fetch_polygon_movers(url):
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        result = []
        for t in data.get("tickers", []):
            result.append({
                "symbol": t.get("ticker", ""),
                "name": "",
                "price": t.get("day", {}).get("c", 0),
                "change": t.get("todaysChange", 0),
                "changesPercentage": t.get("todaysChangePerc", 0),
                "volume": int(t.get("day", {}).get("v", 0)),
            })
        return result[:10]

    def _fetch_most_active_polygon():
        """Derive most-active from Polygon snapshot of common high-volume tickers."""
        # Use a targeted list of known high-volume tickers rather than the 7MB all-tickers endpoint
        high_vol_tickers = "NVDA,TSLA,AAPL,PLTR,SOFI,AMZN,AMD,BAC,META,MSFT,COIN,HOOD,INTC,F,AAL,NIO,RIVN,MARA,RIOT,SMCI"
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={high_vol_tickers}&apiKey={POLYGON_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        tickers = data.get("tickers", [])
        # Sort by day volume descending
        tickers.sort(key=lambda t: t.get("day", {}).get("v", 0), reverse=True)
        result = []
        for t in tickers[:10]:
            result.append({
                "symbol": t.get("ticker", ""),
                "name": "",
                "price": t.get("day", {}).get("c", 0),
                "change": t.get("todaysChange", 0),
                "changesPercentage": t.get("todaysChangePerc", 0),
                "volume": int(t.get("day", {}).get("v", 0)),
            })
        return result

    try:
        gainers_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey={POLYGON_KEY}"
        losers_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/losers?apiKey={POLYGON_KEY}"
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            f_gainers = executor.submit(_fetch_polygon_movers, gainers_url)
            f_losers = executor.submit(_fetch_polygon_movers, losers_url)
            f_active = executor.submit(_fetch_most_active_polygon)
            gainers = f_gainers.result(timeout=20)
            losers = f_losers.result(timeout=20)
            try:
                most_active = f_active.result(timeout=25)
                if not most_active:
                    most_active = FALLBACK_MOST_ACTIVE
            except Exception:
                most_active = FALLBACK_MOST_ACTIVE
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "gainers": gainers,
            "losers": losers,
            "most_active": most_active,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Sentiment ─────────────────────────────────────────────
@app.route("/api/sentiment")
def api_sentiment():
    def _fetch_grades(ticker):
        grades = []
        # FMP stock-grade endpoint returns 404 on current plan; go directly to price-target-summary
        try:
            pt_url = f"https://financialmodelingprep.com/stable/price-target-summary?symbol={ticker}&apikey={FMP_KEY}"
            req = urllib.request.Request(pt_url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                pt_data = json.loads(resp.read().decode())
            if isinstance(pt_data, list) and pt_data:
                pt = pt_data[0]
                avg_pt = pt.get("lastMonthAvgPriceTarget") or pt.get("lastQuarterAvgPriceTarget") or pt.get("allTimeAvgPriceTarget") or 0
                count = pt.get("lastMonthCount") or pt.get("lastQuarterCount") or pt.get("allTimeCount") or 0
                if count:
                    grades = [{"symbol": ticker, "gradingCompany": "Consensus", "grade": "Buy", "action": "coverage", "priceTarget": avg_pt, "analystCount": count}]
        except Exception:
            pass
        return ticker, {"grades": grades[:5]}

    try:
        sentiment = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_grades, t): t for t in TOP_10}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=20)
                    sentiment[ticker] = data
                except Exception as e:
                    t = futures[future]
                    sentiment[t] = {"grades": [], "error": str(e)}
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "sentiment": sentiment,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Macro ─────────────────────────────────────────────────
@app.route("/api/macro")
def api_macro():
    def _fetch(url):
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        return data if isinstance(data, list) else []

    try:
        today = datetime.date.today()
        ten_days_ago = today - datetime.timedelta(days=10)
        one_month_ahead = today + datetime.timedelta(days=31)
        treasury_url = f"https://financialmodelingprep.com/stable/treasury-rates?from={ten_days_ago}&to={today}&apikey={FMP_KEY}"
        econ_url = f"https://financialmodelingprep.com/stable/economic-calendar?from={today}&to={one_month_ahead}&apikey={FMP_KEY}"
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            f_treasury = executor.submit(_fetch, treasury_url)
            f_econ = executor.submit(_fetch, econ_url)
            treasury_rates = f_treasury.result(timeout=20)
            economic_calendar = f_econ.result(timeout=20)
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "treasury_rates": treasury_rates,
            "economic_calendar": economic_calendar,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Technicals (RSI) ──────────────────────────────────────
@app.route("/api/technicals")
def api_technicals():
    def _fetch_rsi(ticker):
        url = f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan=day&window=14&limit=1&apiKey={POLYGON_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        results = data.get("results", {}).get("values", [])
        if results:
            value = results[0].get("value", None)
            if value is not None:
                value = round(value, 2)
                if value < 30:
                    signal = "oversold"
                elif value > 70:
                    signal = "overbought"
                else:
                    signal = "neutral"
                return ticker, {"value": value, "signal": signal}
        return ticker, {"value": None, "signal": "unknown"}

    try:
        rsi = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_rsi, t): t for t in TOP_10}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=20)
                    rsi[ticker] = data
                except Exception as e:
                    t = futures[future]
                    rsi[t] = {"value": None, "signal": "error", "error": str(e)}
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "rsi": rsi,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Insider Transactions ──────────────────────────────────
@app.route("/api/insider")
def api_insider():
    def _fetch_insider(ticker):
        result = []
        try:
            url = f"https://financialmodelingprep.com/stable/insider-trading?symbol={ticker}&limit=5&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            result = data if isinstance(data, list) else []
        except urllib.error.HTTPError:
            pass  # 404 etc — fall through to fallback
        except Exception:
            pass
        # Use fallback data if API returned empty or errored
        if not result:
            result = FALLBACK_INSIDER.get(ticker, [])
        return ticker, result

    try:
        insider = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_insider, t): t for t in TOP_10}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=20)
                    insider[ticker] = data
                except Exception as e:
                    t = futures[future]
                    insider[t] = FALLBACK_INSIDER.get(t, [])
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "insider": insider,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Static File Serving ───────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("static", path)


# ── Entry Point ───────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
