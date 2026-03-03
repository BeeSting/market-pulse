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

# ── CORS ──────────────────────────────────────────────────
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

# ── API Keys ──────────────────────────────────────────────
POLYGON_KEY = os.environ.get("POLYGON_KEY", "SL1wF6nbcCYCWRbfl5TcepWwd5pwPAbW")
FMP_KEY = os.environ.get("FMP_KEY", "EINiL3Pzp1f0YjvQgcnm8t3hBBShCdMd")
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "22b226153fmsh05ce406581ceab8p1586d8jsn6de44ca4671f")
UW_KEY = os.environ.get("UNUSUAL_WHALES_API_KEY", "cc97a47e-9c8d-4fdb-a2b8-be4eeba08045")
MASSIVE_KEY = os.environ.get("MASSIVE_KEY", "8DLmlXSQ8eaVqjUtNPskYcbLhLasNv1I")

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

CRYPTO_SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD", "BNBUSD", "XRPUSD", "ADAUSD", "DOGEUSD", "AVAXUSD"]

ALL_TICKERS = list(dict.fromkeys(PORTFOLIO_TICKERS + WATCHLIST_TICKERS))

TOP_10 = ["SLV", "IREN", "CIFR", "NBIS", "SOFI", "IAU", "APLD", "RKLB", "TMDX", "WULF"]


# ── No hardcoded fallback data — all API-only ────────────


# ── Quote Fetchers ────────────────────────────────────────
def fetch_polygon(tickers):
    syms = ",".join(tickers)
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={syms}&apiKey={MASSIVE_KEY}"
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
            price = q.get("price", 0) or 0
            chg = q.get("change", 0) or 0
            # FMP commodity quotes don't include previousClose — derive it
            prev = q.get("previousClose", 0) or 0
            if not prev and price and chg:
                prev = price - chg
            chg_pct = round((chg / prev * 100) if prev else 0, 2)
            quotes[info["display"]] = {
                "symbol": info["display"],
                "name": info["name"],
                "unit": info["unit"],
                "price": price,
                "change": round(chg, 2),
                "changesPercentage": chg_pct,
                "previousClose": round(prev, 2),
                "timestamp": q.get("timestamp", 0),
            }
    return quotes


# ── Major Indices ─────────────────────────────────────────
INDEX_MAP = {
    "^GSPC": {"display": "SP500", "name": "S&P 500"},
    "^DJI":  {"display": "DOW",   "name": "Dow Jones"},
    "^IXIC": {"display": "NASDAQ","name": "NASDAQ Composite"},
    "^RUT":  {"display": "RUT",   "name": "Russell 2000"},
}

def fetch_indices():
    """Fetch major index quotes from FMP."""
    symbols = "%2C".join(INDEX_MAP.keys()).replace("^", "%5E")
    url = f"https://financialmodelingprep.com/stable/batch-quote?symbols=%5EGSPC,%5EDJI,%5EIXIC,%5ERUT&apikey={FMP_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = json.loads(resp.read().decode())
    quotes = {}
    for q in (raw if isinstance(raw, list) else []):
        sym = q.get("symbol", "")
        if sym in INDEX_MAP:
            info = INDEX_MAP[sym]
            quotes[info["display"]] = {
                "symbol": info["display"],
                "name": info["name"],
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


# ── Fear & Greed Indexes ──────────────────────────────────
def fetch_fear_greed():
    """Fetch CNN Fear & Greed Index via RapidAPI + Crypto Fear & Greed from alternative.me."""
    result = {}

    # CNN Fear & Greed (market)
    try:
        url = "https://fear-and-greed-index.p.rapidapi.com/v1/fgi"
        req = urllib.request.Request(url, headers={
            "x-rapidapi-key": RAPIDAPI_KEY,
            "x-rapidapi-host": "fear-and-greed-index.p.rapidapi.com",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        fgi = data.get("fgi", {})
        now = fgi.get("now", {})
        prev = fgi.get("previousClose", {})
        result["market"] = {
            "value": now.get("value", 0),
            "label": now.get("valueText", "N/A"),
            "previous": prev.get("value", 0),
            "previousLabel": prev.get("valueText", "N/A"),
            "oneWeekAgo": fgi.get("oneWeekAgo", {}).get("value", 0),
            "oneMonthAgo": fgi.get("oneMonthAgo", {}).get("value", 0),
        }
    except Exception as e:
        result["market"] = {"value": 0, "label": "N/A", "error": str(e)}

    # Crypto Fear & Greed
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        entry = data.get("data", [{}])[0]
        result["crypto"] = {
            "value": int(entry.get("value", 0)),
            "label": entry.get("value_classification", "N/A"),
        }
    except Exception as e:
        result["crypto"] = {"value": 0, "label": "N/A", "error": str(e)}

    return result


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

    # Enrich with crypto, VIX, commodities, indices, and fear & greed — in parallel
    enrichments = {}
    fear_greed = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(fetch_crypto): 'crypto',
            pool.submit(fetch_vix): 'vix',
            pool.submit(fetch_commodities): 'commodities',
            pool.submit(fetch_indices): 'indices',
            pool.submit(fetch_fear_greed): 'fear_greed',
        }
        for future in concurrent.futures.as_completed(futures, timeout=15):
            key = futures[future]
            try:
                result = future.result()
                if key == 'fear_greed':
                    fear_greed = result
                else:
                    quotes.update(result)
            except Exception:
                pass  # non-fatal

    return jsonify({
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": source,
        "count": len(quotes),
        "quotes": quotes,
        "fear_greed": fear_greed,
    }), 200, {"Cache-Control": "no-cache"}


@app.route("/api/news")
def api_news():
    tickers_param = request.args.get("tickers", "")
    limit = min(int(request.args.get("limit", "15")), 30)

    ticker_filter = tickers_param if tickers_param else ",".join(ALL_TICKERS)
    url = f"https://api.polygon.io/v2/reference/news?ticker.any_of={ticker_filter}&limit={limit}&order=desc&sort=published_utc&apiKey={MASSIVE_KEY}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    articles = []
    for a in data.get("results", []):
        pub = a.get("publisher", {})
        # Extract sentiment insights per ticker (Massive premium)
        insights = []
        for ins in (a.get("insights", []) or []):
            insights.append({
                "ticker": ins.get("ticker", ""),
                "sentiment": ins.get("sentiment", ""),
                "reasoning": (ins.get("sentiment_reasoning", "") or "")[:200],
            })
        articles.append({
            "title": a.get("title", ""),
            "url": a.get("article_url", ""),
            "source": pub.get("name", ""),
            "published": a.get("published_utc", ""),
            "tickers": a.get("tickers", [])[:6],
            "image": a.get("image_url", ""),
            "description": (a.get("description", "") or "")[:200],
            "keywords": (a.get("keywords", []) or [])[:5],
            "insights": insights,
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


# ── Congress Trades (Unusual Whales — live) ───────────────
@app.route("/api/congress-trades")
def api_congress_trades():
    """Redirect to UW live congress trades endpoint."""
    try:
        url = "https://api.unusualwhales.com/api/congress/recent-trades"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        trades = raw.get("data", [])[:20]
        cleaned = []
        for t in trades:
            cleaned.append({
                "firstName": (t.get("name", "") or "").split()[0] if t.get("name") else "",
                "lastName": " ".join((t.get("name", "") or "").split()[1:]) if t.get("name") else "",
                "office": (t.get("member_type", "") or "").title(),
                "symbol": t.get("ticker", "") or "",
                "transactionDate": t.get("transaction_date", ""),
                "transactionType": t.get("txn_type", ""),
                "amount": t.get("amounts", ""),
                "assetDescription": t.get("issuer", "") or t.get("notes", ""),
                "filedDate": t.get("filed_at_date", ""),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "trades": cleaned,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "trades": []}), 200, {"Cache-Control": "no-cache"}


# ── Earnings Calendar ─────────────────────────────────────
@app.route("/api/earnings-calendar")
def api_earnings_calendar():
    """Fetch upcoming earnings via FMP confirmed + analyst estimates for portfolio tickers."""
    try:
        today = datetime.date.today()
        four_months = today + datetime.timedelta(days=120)
        all_syms = set(PORTFOLIO_TICKERS + WATCHLIST_TICKERS)
        filtered = []
        # Try confirmed earnings calendar first
        try:
            url = f"https://financialmodelingprep.com/stable/earning-calendar-confirmed?from={today}&to={four_months}&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode())
            filtered = [
                entry for entry in (data if isinstance(data, list) else [])
                if entry.get("symbol", "") in all_syms
            ]
        except Exception:
            pass  # 404 or other error, fall through
        # If confirmed is empty, try non-confirmed
        if not filtered:
            try:
                url2 = f"https://financialmodelingprep.com/stable/earning-calendar?from={today}&to={four_months}&apikey={FMP_KEY}"
                req2 = urllib.request.Request(url2, headers={"User-Agent": "MarketPulse/1.0"})
                with urllib.request.urlopen(req2, timeout=20) as resp2:
                    data2 = json.loads(resp2.read().decode())
                filtered = [
                    entry for entry in (data2 if isinstance(data2, list) else [])
                    if entry.get("symbol", "") in all_syms
                ]
            except Exception:
                pass  # 404 or other error, fall through
        # If still empty, build from analyst estimates for top holdings
        if not filtered:
            def _fetch_est(ticker):
                try:
                    est_url = f"https://financialmodelingprep.com/stable/analyst-estimates?symbol={ticker}&period=quarter&limit=1&apikey={FMP_KEY}"
                    req = urllib.request.Request(est_url, headers={"User-Agent": "MarketPulse/1.0"})
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        est = json.loads(resp.read().decode())
                    if isinstance(est, list) and est:
                        e = est[0]
                        return {
                            "symbol": ticker,
                            "date": e.get("date", ""),
                            "epsEstimated": e.get("epsAvg"),
                            "revenueEstimated": e.get("revenueAvg"),
                        }
                except Exception:
                    pass
                return None
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                futures = {pool.submit(_fetch_est, t): t for t in TOP_10}
                for f in concurrent.futures.as_completed(futures, timeout=15):
                    try:
                        r = f.result()
                        if r:
                            filtered.append(r)
                    except Exception:
                        pass
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "earnings": filtered,
            "count": len(filtered),
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "earnings": [], "count": 0}), 200, {"Cache-Control": "no-cache"}


# ── Market Movers ─────────────────────────────────────────



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
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={high_vol_tickers}&apiKey={MASSIVE_KEY}"
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
        gainers_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey={MASSIVE_KEY}"
        losers_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/losers?apiKey={MASSIVE_KEY}"
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            f_gainers = executor.submit(_fetch_polygon_movers, gainers_url)
            f_losers = executor.submit(_fetch_polygon_movers, losers_url)
            f_active = executor.submit(_fetch_most_active_polygon)
            gainers = f_gainers.result(timeout=20)
            losers = f_losers.result(timeout=20)
            try:
                most_active = f_active.result(timeout=25)
                if not most_active:
                    most_active = []
            except Exception:
                most_active = []
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
        url = f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan=day&window=14&limit=1&apiKey={MASSIVE_KEY}"
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


# ── Fundamentals (Key Metrics + Profile) ─────────────────
@app.route("/api/fundamentals")
def api_fundamentals():
    """Fetch FMP key-metrics + profile for TOP_10 tickers."""
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else TOP_10

    def _fetch_fundamentals(ticker):
        result = {"symbol": ticker}
        # Key metrics (quarterly, latest)
        try:
            km_url = f"https://financialmodelingprep.com/stable/key-metrics?symbol={ticker}&period=quarter&limit=1&apikey={FMP_KEY}"
            req = urllib.request.Request(km_url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                km_data = json.loads(resp.read().decode())
            if isinstance(km_data, list) and km_data:
                km = km_data[0]
                result["keyMetrics"] = {
                    "peRatio": km.get("peRatio"),
                    "pbRatio": km.get("pbRatio"),
                    "priceToSalesRatio": km.get("priceToSalesRatio"),
                    "evToEbitda": km.get("enterpriseValueOverEbitda"),
                    "evToFcf": km.get("evToFreeCashFlow"),
                    "debtToEquity": km.get("debtToEquity"),
                    "currentRatio": km.get("currentRatio"),
                    "roe": km.get("returnOnEquity"),
                    "roa": km.get("returnOnAssets"),
                    "roic": km.get("returnOnCapitalEmployed"),
                    "fcfYield": km.get("freeCashFlowYield"),
                    "earningsYield": km.get("earningsYield"),
                    "dividendYield": km.get("dividendYield"),
                    "revenuePerShare": km.get("revenuePerShare"),
                    "netIncomePerShare": km.get("netIncomePerShare"),
                    "bookValuePerShare": km.get("bookValuePerShare"),
                    "marketCap": km.get("marketCap"),
                    "enterpriseValue": km.get("enterpriseValue"),
                }
        except Exception:
            result["keyMetrics"] = {}
        # Company profile
        try:
            pf_url = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_KEY}"
            req = urllib.request.Request(pf_url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                pf_data = json.loads(resp.read().decode())
            if isinstance(pf_data, list) and pf_data:
                pf = pf_data[0]
                result["profile"] = {
                    "name": pf.get("companyName", ""),
                    "sector": pf.get("sector", ""),
                    "industry": pf.get("industry", ""),
                    "beta": pf.get("beta"),
                    "marketCap": pf.get("mktCap"),
                    "employees": pf.get("fullTimeEmployees"),
                    "country": pf.get("country", ""),
                    "isEtf": pf.get("isEtf", False),
                    "description": (pf.get("description", "") or "")[:300],
                }
        except Exception:
            result["profile"] = {}
        # Income statement (quarterly, latest 2)
        try:
            is_url = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&period=quarter&limit=2&apikey={FMP_KEY}"
            req = urllib.request.Request(is_url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                is_data = json.loads(resp.read().decode())
            if isinstance(is_data, list) and is_data:
                latest = is_data[0]
                result["income"] = {
                    "revenue": latest.get("revenue"),
                    "grossProfit": latest.get("grossProfit"),
                    "grossMargin": latest.get("grossProfitRatio"),
                    "operatingIncome": latest.get("operatingIncome"),
                    "operatingMargin": latest.get("operatingIncomeRatio"),
                    "netIncome": latest.get("netIncome"),
                    "netMargin": latest.get("netIncomeRatio"),
                    "eps": latest.get("eps"),
                    "epsDiluted": latest.get("epsDiluted"),
                    "ebitda": latest.get("ebitda"),
                    "period": latest.get("period"),
                    "date": latest.get("date"),
                }
                # Revenue growth QoQ if we have 2 quarters
                if len(is_data) >= 2 and is_data[1].get("revenue"):
                    prev_rev = is_data[1]["revenue"]
                    curr_rev = latest.get("revenue", 0)
                    if prev_rev and prev_rev != 0:
                        result["income"]["revenueGrowthQoQ"] = round((curr_rev - prev_rev) / abs(prev_rev) * 100, 2)
        except Exception:
            result["income"] = {}
        return ticker, result

    try:
        fundamentals = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_fundamentals, t): t for t in tickers}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=30)
                    fundamentals[ticker] = data
                except Exception as e:
                    t = futures[future]
                    fundamentals[t] = {"symbol": t, "error": str(e)}
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "fundamentals": fundamentals,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Earnings Surprises ───────────────────────────────────
@app.route("/api/earnings-surprises")
def api_earnings_surprises():
    """Fetch recent earnings surprises for TOP_10 tickers."""
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else TOP_10

    def _fetch_surprises(ticker):
        try:
            url = f"https://financialmodelingprep.com/stable/earnings-surprises?symbol={ticker}&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            surprises = []
            for s in (data if isinstance(data, list) else [])[:4]:
                actual = s.get("actualEarningResult")
                estimated = s.get("estimatedEarning")
                surprise_pct = None
                if actual is not None and estimated is not None and estimated != 0:
                    surprise_pct = round((actual - estimated) / abs(estimated) * 100, 2)
                surprises.append({
                    "date": s.get("date"),
                    "actual": actual,
                    "estimated": estimated,
                    "surprisePct": surprise_pct,
                })
            return ticker, surprises
        except Exception:
            return ticker, []

    try:
        result = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_surprises, t): t for t in tickers}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=20)
                    result[ticker] = data
                except Exception:
                    t = futures[future]
                    result[t] = []
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "surprises": result,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Analyst Ratings (FMP Grades Consensus) ───────────────
@app.route("/api/analyst-ratings")
def api_analyst_ratings():
    """Fetch analyst consensus grades for TOP_10 tickers from FMP."""
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else TOP_10

    def _fetch_grades(ticker):
        try:
            url = f"https://financialmodelingprep.com/stable/grades-consensus?symbol={ticker}&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            if isinstance(data, list) and data:
                g = data[0]
                sb = g.get("strongBuy", 0) or 0
                b = g.get("buy", 0) or 0
                h = g.get("hold", 0) or 0
                s = g.get("sell", 0) or 0
                ss = g.get("strongSell", 0) or 0
                total = sb + b + h + s + ss
                consensus_score = round((sb * 2 + b * 1 - s * 1 - ss * 2) / total, 2) if total else 0
                return ticker, {
                    "strongBuy": sb, "buy": b, "hold": h, "sell": s, "strongSell": ss,
                    "total": total, "consensusScore": consensus_score,
                    "consensus": g.get("consensus", "N/A"),
                }
        except Exception:
            pass
        return ticker, None

    try:
        ratings = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_grades, t): t for t in tickers}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=20)
                    if data:
                        ratings[ticker] = data
                except Exception:
                    pass
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "ratings": ratings,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "ratings": {}}), 200, {"Cache-Control": "no-cache"}


# ── Market Tide (Unusual Whales) ─────────────────────────
@app.route("/api/market-tide")
def api_market_tide():
    """Fetch market-wide options flow tide from Unusual Whales."""
    date_param = request.args.get("date", "")
    try:
        url = "https://api.unusualwhales.com/api/market/market-tide"
        if date_param:
            url += f"?date={date_param}"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        records = raw.get("data", [])
        # Summarize: latest reading + aggregate stats
        summary = {}
        if records:
            last = records[-1]
            total_call = sum(float(r.get("net_call_premium", 0)) for r in records)
            total_put = sum(float(r.get("net_put_premium", 0)) for r in records)
            total_vol = sum(int(r.get("net_volume", 0)) for r in records)
            summary = {
                "latestTimestamp": last.get("timestamp", ""),
                "latestNetCallPremium": float(last.get("net_call_premium", 0)),
                "latestNetPutPremium": float(last.get("net_put_premium", 0)),
                "latestNetVolume": int(last.get("net_volume", 0)),
                "totalNetCallPremium": round(total_call, 2),
                "totalNetPutPremium": round(total_put, 2),
                "totalNetVolume": total_vol,
                "dataPoints": len(records),
                "signal": "bullish" if total_call > abs(total_put) else "bearish",
            }
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "summary": summary,
            "data": records[-20:],  # Last 20 minutes for chart
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "summary": {}, "data": []}), 200, {"Cache-Control": "no-cache"}


# ── Options Flow Alerts (Unusual Whales) ─────────────────
@app.route("/api/options-flow")
def api_options_flow():
    """Fetch unusual options flow alerts from Unusual Whales."""
    ticker_param = request.args.get("ticker", "")
    limit = min(int(request.args.get("limit", "30")), 50)
    try:
        url = f"https://api.unusualwhales.com/api/option-trades/flow-alerts"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        alerts = raw.get("data", [])
        # Filter by ticker if requested, then limit
        if ticker_param:
            alerts = [a for a in alerts if a.get("ticker", "").upper() == ticker_param.upper()]
        alerts = alerts[:limit]
        # Clean up for frontend
        cleaned = []
        for a in alerts:
            cleaned.append({
                "ticker": a.get("ticker", ""),
                "optionChain": a.get("option_chain", ""),
                "alertRule": a.get("alert_rule", ""),
                "premium": float(a.get("total_premium", 0)),
                "volume": int(a.get("volume", 0)),
                "openInterest": int(a.get("open_interest", 0)) if a.get("open_interest") else 0,
                "volumeOiRatio": float(a.get("volume_oi_ratio", 0)) if a.get("volume_oi_ratio") else 0,
                "tradeCount": int(a.get("trade_count", 0)),
                "size": int(a.get("total_size", 0)),
                "bid": a.get("bid", ""),
                "ask": a.get("ask", ""),
                "price": a.get("price", ""),
                "sector": a.get("sector", ""),
                "allOpening": a.get("all_opening_trades", False),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "count": len(cleaned),
            "alerts": cleaned,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "count": 0, "alerts": []}), 200, {"Cache-Control": "no-cache"}


# ── Dark Pool Flow (Unusual Whales) ──────────────────────
@app.route("/api/dark-pool")
def api_dark_pool():
    """Fetch recent dark pool (off-exchange) trades from Unusual Whales."""
    ticker_param = request.args.get("ticker", "")
    limit = min(int(request.args.get("limit", "30")), 100)
    try:
        if ticker_param:
            url = f"https://api.unusualwhales.com/api/darkpool/{ticker_param.upper()}"
        else:
            url = "https://api.unusualwhales.com/api/darkpool/recent"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        trades = raw.get("data", [])
        trades = trades[:limit]
        cleaned = []
        for t in trades:
            cleaned.append({
                "ticker": t.get("ticker", ""),
                "price": t.get("price", ""),
                "size": int(t.get("size", 0)),
                "premium": t.get("premium", ""),
                "volume": int(t.get("volume", 0)),
                "executedAt": t.get("executed_at", ""),
                "marketCenter": t.get("market_center", ""),
                "nbboBid": t.get("nbbo_bid", ""),
                "nbboAsk": t.get("nbbo_ask", ""),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "count": len(cleaned),
            "trades": cleaned,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "count": 0, "trades": []}), 200, {"Cache-Control": "no-cache"}


# ── Congress Trades (Unusual Whales — live) ──────────────
@app.route("/api/congress-trades-live")
def api_congress_trades_live():
    """Fetch recent congressional trades from Unusual Whales."""
    try:
        url = "https://api.unusualwhales.com/api/congress/recent-trades"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        trades = raw.get("data", [])[:20]
        cleaned = []
        for t in trades:
            cleaned.append({
                "firstName": (t.get("name", "") or "").split()[0] if t.get("name") else "",
                "lastName": " ".join((t.get("name", "") or "").split()[1:]) if t.get("name") else "",
                "office": (t.get("member_type", "") or "").title(),
                "symbol": t.get("ticker", "") or "",
                "transactionDate": t.get("transaction_date", ""),
                "transactionType": t.get("txn_type", ""),
                "amount": t.get("amounts", ""),
                "assetDescription": t.get("issuer", "") or t.get("notes", ""),
                "filedDate": t.get("filed_at_date", ""),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "trades": cleaned,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "trades": []}), 200, {"Cache-Control": "no-cache"}


# ── Ticker Options Flow (Unusual Whales) ─────────────────
@app.route("/api/ticker-flow/<ticker>")
def api_ticker_flow(ticker):
    """Fetch recent options flow for a specific ticker."""
    try:
        url = f"https://api.unusualwhales.com/api/stock/{ticker.upper()}/flow-recent"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        # Response can be a list directly
        records = raw if isinstance(raw, list) else raw.get("data", [])
        records = records[:20]
        cleaned = []
        for r in records:
            cleaned.append({
                "optionChain": r.get("option_chain_id", ""),
                "type": r.get("option_type", ""),
                "delta": r.get("delta", ""),
                "premium": float(r.get("price", 0)) * int(r.get("size", 0)) * 100 if r.get("price") and r.get("size") else 0,
                "size": int(r.get("size", 0)),
                "volume": int(r.get("volume", 0)),
                "iv": r.get("implied_volatility", ""),
                "bid": r.get("nbbo_bid", ""),
                "ask": r.get("nbbo_ask", ""),
                "price": r.get("price", ""),
                "underlyingPrice": r.get("underlying_price", ""),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "ticker": ticker.upper(),
            "count": len(cleaned),
            "flow": cleaned,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "ticker": ticker.upper(), "count": 0, "flow": []}), 200, {"Cache-Control": "no-cache"}


# ── DCF Valuations (FMP) ──────────────────────────────────
@app.route("/api/dcf")
def api_dcf():
    """Fetch DCF valuations for TOP_10 tickers from FMP."""
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else TOP_10

    def _fetch_dcf(ticker):
        try:
            url = f"https://financialmodelingprep.com/stable/discounted-cash-flow?symbol={ticker}&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            if isinstance(data, list) and data:
                d = data[0]
                dcf_val = d.get("dcf", 0)
                stock_price = d.get("Stock Price", 0)
                upside = round((dcf_val - stock_price) / stock_price * 100, 1) if stock_price else 0
                return ticker, {
                    "dcf": round(dcf_val, 2),
                    "stockPrice": stock_price,
                    "upside": upside,
                    "date": d.get("date", ""),
                }
        except Exception:
            pass
        return ticker, None

    try:
        dcf_data = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_dcf, t): t for t in tickers}
            for future in concurrent.futures.as_completed(futures, timeout=20):
                try:
                    ticker, data = future.result()
                    if data:
                        dcf_data[ticker] = data
                except Exception:
                    pass
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "dcf": dcf_data,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "dcf": {}}), 200, {"Cache-Control": "no-cache"}


# ── Sector Performance ───────────────────────────────────
@app.route("/api/sector-performance")
def api_sector_performance():
    """Fetch real-time sector performance from FMP."""
    try:
        url = f"https://financialmodelingprep.com/stable/sector-performance?apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        sectors = []
        for s in (data if isinstance(data, list) else []):
            sectors.append({
                "sector": s.get("sector", ""),
                "changesPercentage": s.get("changesPercentage", 0),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "sectors": sectors,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Stock Screener / Gainers by Sector ───────────────────
@app.route("/api/stock-screener")
def api_stock_screener():
    """Fetch top gainers per sector from FMP stock screener."""
    sector = request.args.get("sector", "Technology")
    limit = min(int(request.args.get("limit", "10")), 50)
    try:
        url = f"https://financialmodelingprep.com/stable/stock-screener?sector={urllib.parse.quote(sector)}&limit={limit}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        stocks = []
        for s in (data if isinstance(data, list) else []):
            stocks.append({
                "symbol": s.get("symbol", ""),
                "name": s.get("companyName", ""),
                "marketCap": s.get("marketCap", 0),
                "price": s.get("price", 0),
                "beta": s.get("beta", 0),
                "sector": s.get("sector", ""),
                "industry": s.get("industry", ""),
            })
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "stocks": stocks,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Insider Transactions (Unusual Whales — real SEC Form 4) ──
@app.route("/api/insider")
def api_insider():
    """Fetch recent SEC Form 4 insider transactions from Unusual Whales."""
    try:
        url = "https://api.unusualwhales.com/api/insider/transactions?limit=50"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {UW_KEY}",
            "Accept": "application/json",
            "User-Agent": "MarketPulse/1.0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        transactions = raw.get("data", [])

        # Organize by ticker for the frontend
        insider = {}
        all_portfolio = set(PORTFOLIO_TICKERS + WATCHLIST_TICKERS + TOP_10)
        for t in transactions:
            sym = t.get("ticker", "")
            if not sym:
                continue
            amount = t.get("amount", 0) or 0
            price = float(t.get("price", 0) or 0)
            abs_amount = abs(amount)
            is_buy = amount > 0
            owner_type = "officer" if t.get("is_officer") else "director" if t.get("is_director") else "10% owner" if t.get("is_ten_percent_owner") else ""
            title = t.get("officer_title", "") or owner_type

            entry = {
                "symbol": sym,
                "transactionDate": t.get("transaction_date", ""),
                "filingDate": t.get("filing_date", ""),
                "reportingName": (t.get("owner_name", "") or "").title(),
                "typeOfOwner": title,
                "transactionType": "P-Purchase" if is_buy else "S-Sale",
                "securitiesTransacted": abs_amount,
                "price": price,
                "securitiesOwned": t.get("shares_owned_after", 0) or 0,
                "transactionCode": t.get("transaction_code", ""),
                "is10b51": t.get("is_10b5_1", False),
                "sector": t.get("sector", ""),
            }
            if sym not in insider:
                insider[sym] = []
            insider[sym].append(entry)

        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "insider": insider,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "insider": {}}), 200, {"Cache-Control": "no-cache"}


# ── Enhanced News with Sentiment Insights (Massive/Polygon) ──
@app.route("/api/news-sentiment")
def api_news_sentiment():
    """Fetch news with per-ticker sentiment insights via Massive/Polygon premium."""
    tickers_param = request.args.get("tickers", "")
    limit = min(int(request.args.get("limit", "20")), 30)

    ticker_filter = tickers_param if tickers_param else ",".join(ALL_TICKERS)
    url = f"https://api.polygon.io/v2/reference/news?ticker.any_of={ticker_filter}&limit={limit}&order=desc&sort=published_utc&apiKey={MASSIVE_KEY}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    articles = []
    for a in data.get("results", []):
        pub = a.get("publisher", {})
        # Extract sentiment insights per ticker
        insights = []
        for ins in (a.get("insights", []) or []):
            insights.append({
                "ticker": ins.get("ticker", ""),
                "sentiment": ins.get("sentiment", ""),
                "reasoning": (ins.get("sentiment_reasoning", "") or "")[:200],
            })
        articles.append({
            "title": a.get("title", ""),
            "url": a.get("article_url", ""),
            "source": pub.get("name", ""),
            "published": a.get("published_utc", ""),
            "tickers": a.get("tickers", [])[:6],
            "image": a.get("image_url", ""),
            "description": (a.get("description", "") or "")[:200],
            "keywords": (a.get("keywords", []) or [])[:5],
            "insights": insights,
        })

    return jsonify({
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "count": len(articles),
        "articles": articles,
    }), 200, {"Cache-Control": "no-cache"}


# ── Technical Indicators (Massive/Polygon Stocks Advanced) ──
@app.route("/api/technicals-full")
def api_technicals_full():
    """Fetch RSI + MACD + SMA50 + SMA200 + EMA20 for TOP_10 tickers via Massive."""
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()] if tickers_param else TOP_10

    def _fetch_indicator(ticker, indicator, params=""):
        url = f"https://api.polygon.io/v1/indicators/{indicator}/{ticker}?timespan=day&limit=1{params}&apiKey={MASSIVE_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode())
        values = data.get("results", {}).get("values", [])
        return values[0] if values else None

    def _fetch_all_for_ticker(ticker):
        result = {"symbol": ticker}
        try:
            rsi = _fetch_indicator(ticker, "rsi", "&window=14")
            if rsi:
                v = round(rsi.get("value", 0), 2)
                result["rsi"] = {"value": v, "signal": "oversold" if v < 30 else "overbought" if v > 70 else "neutral"}
        except Exception:
            result["rsi"] = None
        try:
            macd = _fetch_indicator(ticker, "macd")
            if macd:
                result["macd"] = {
                    "value": round(macd.get("value", 0), 4),
                    "signal": round(macd.get("signal", 0), 4),
                    "histogram": round(macd.get("histogram", 0), 4),
                    "trend": "bullish" if macd.get("histogram", 0) > 0 else "bearish",
                }
        except Exception:
            result["macd"] = None
        try:
            sma50 = _fetch_indicator(ticker, "sma", "&window=50")
            if sma50:
                result["sma50"] = round(sma50.get("value", 0), 2)
        except Exception:
            result["sma50"] = None
        try:
            sma200 = _fetch_indicator(ticker, "sma", "&window=200")
            if sma200:
                result["sma200"] = round(sma200.get("value", 0), 2)
        except Exception:
            result["sma200"] = None
        try:
            ema20 = _fetch_indicator(ticker, "ema", "&window=20")
            if ema20:
                result["ema20"] = round(ema20.get("value", 0), 2)
        except Exception:
            result["ema20"] = None
        return ticker, result

    try:
        technicals = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_all_for_ticker, t): t for t in tickers}
            for future in concurrent.futures.as_completed(futures):
                try:
                    ticker, data = future.result(timeout=30)
                    technicals[ticker] = data
                except Exception as e:
                    t = futures[future]
                    technicals[t] = {"symbol": t, "error": str(e)}
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "technicals": technicals,
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Related Companies (Massive/Polygon Stocks Advanced) ────
@app.route("/api/related/<ticker>")
def api_related(ticker):
    """Fetch related companies for a ticker via Massive/Polygon."""
    try:
        url = f"https://api.polygon.io/v1/related-companies/{ticker.upper()}?apiKey={MASSIVE_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        results = data.get("results", [])
        # Deduplicate
        seen = set()
        related = []
        for r in results:
            t = r.get("ticker", "")
            if t and t not in seen:
                seen.add(t)
                related.append(t)
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "ticker": ticker.upper(),
            "related": related[:10],
        }), 200, {"Cache-Control": "no-cache"}
    except Exception as e:
        return jsonify({"error": str(e), "ticker": ticker.upper(), "related": []}), 200, {"Cache-Control": "no-cache"}


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
