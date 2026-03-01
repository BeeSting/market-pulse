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
