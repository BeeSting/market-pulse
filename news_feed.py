#!/usr/bin/env python3
"""
News Intelligence Aggregator — fetches from 10+ sources in parallel,
deduplicates, scores relevance, categorises, and returns a unified feed.
"""
import json
import os
import re
import datetime
import hashlib
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
import concurrent.futures

# ── Keys ──────────────────────────────────────────────────
FMP_KEY     = os.environ.get("FMP_KEY",     "EINiL3Pzp1f0YjvQgcnm8t3hBBShCdMd")
MASSIVE_KEY = os.environ.get("MASSIVE_KEY", "8DLmlXSQ8eaVqjUtNPskYcbLhLasNv1I")
MARKETAUX_KEY = os.environ.get("MARKETAUX_KEY", "qwARxbOClIMxUrt2m5iaYvkwcqLi6N8yMLiurYim")

PORTFOLIO_TICKERS = [
    "SLV","IREN","CIFR","NBIS","SOFI","IAU","APLD","RKLB","TMDX",
    "WULF","PALL","DCTH","WGMI","GOOGL","VRT","NVDA","ACHR","FLNC",
    "GLXY","MRVL","PLTR","HUT","JOBY","BITF","CLSK","NET","PYPL",
    "SERV","UUUU","COIN","SHOP","CRWV","HIMS","MP","HOOD","NU"
]
PORTFOLIO_SET = set(PORTFOLIO_TICKERS)

# Keyword sets for categorisation
MACRO_KW = {'fed','fomc','cpi','inflation','gdp','treasury','yield','interest rate',
            'rate cut','rate hike','employment','nonfarm','payroll','recession',
            'central bank','ecb','boj','monetary policy','fiscal','stimulus',
            'tariff','trade war','sanctions','debt ceiling','deficit','imf','world bank'}
GEO_KW = {'iran','russia','ukraine','china','taiwan','north korea','nato','opec',
           'strait of hormuz','middle east','war','conflict','military','nuclear',
           'sanctions','ceasefire','invasion','missile','drone','deployment'}
CRYPTO_KW = {'bitcoin','btc','ethereum','eth','solana','sol','crypto','blockchain',
             'defi','nft','altcoin','stablecoin','binance','coinbase','mining',
             'halving','memecoin','dogecoin','xrp','cardano','web3'}
COMMODITIES_KW = {'gold','silver','oil','crude','natural gas','platinum','palladium',
                  'copper','uranium','lithium','commodity','wti','brent','opec'}
AI_TECH_KW = {'ai','artificial intelligence','machine learning','gpu','data center',
              'semiconductor','chip','nvidia','cloud computing','llm','chatgpt',
              'openai','generative ai','transformer','inference'}

# ── In-memory cache ────────────────────────────────────────
import threading
_cache_lock = threading.Lock()
_cache = {"data": None, "ts": 0}
_CACHE_TTL = 120  # seconds

def _http_get(url, timeout=5):
    """Simple HTTP GET returning decoded text."""
    import socket
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    finally:
        socket.setdefaulttimeout(old_timeout)

def _safe_json(url, timeout=6):
    """GET + JSON parse, returns None on error."""
    try:
        return json.loads(_http_get(url, timeout))
    except Exception:
        return None

def _parse_rss(xml_text):
    """Parse RSS XML into list of dicts."""
    items = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            # Try to get media:content image
            image = ""
            for mc in item.iter():
                if "content" in mc.tag and mc.get("url"):
                    image = mc.get("url")
                    break
                if "thumbnail" in mc.tag and mc.get("url"):
                    image = mc.get("url")
                    break
                if "enclosure" in mc.tag and mc.get("url"):
                    image = mc.get("url")
                    break
            items.append({"title": title, "url": link, "description": desc,
                          "published": pub, "image": image, "source": ""})
    except Exception:
        pass
    return items

def _time_ago(dt_str):
    """Convert datetime string to relative time."""
    if not dt_str:
        return ""
    try:
        # Handle various date formats
        for fmt in [
            "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
            "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
        ]:
            try:
                dt = datetime.datetime.strptime(dt_str.strip(), fmt)
                break
            except ValueError:
                continue
        else:
            return dt_str[:16]
        now = datetime.datetime.utcnow()
        if dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        diff = now - dt
        mins = int(diff.total_seconds() / 60)
        if mins < 1: return "Just now"
        if mins < 60: return f"{mins}m ago"
        hrs = mins // 60
        if hrs < 24: return f"{hrs}h ago"
        days = hrs // 24
        if days < 7: return f"{days}d ago"
        return dt.strftime("%b %d")
    except Exception:
        return dt_str[:16] if dt_str else ""

def _extract_tickers(text):
    """Extract potential tickers from text."""
    if not text:
        return []
    # Match $TICKER or standalone uppercase 2-5 letter words that look like tickers
    found = set(re.findall(r'\$([A-Z]{2,5})\b', text))
    # Also check for known portfolio tickers
    upper = text.upper()
    for t in PORTFOLIO_TICKERS:
        if t in upper:
            found.add(t)
    return list(found)[:8]

def _categorise(title, description, tickers):
    """Auto-categorise article. Returns list of categories."""
    cats = []
    combined = ((title or "") + " " + (description or "")).lower()
    
    if any(kw in combined for kw in GEO_KW):
        cats.append("Geopolitical")
    if any(kw in combined for kw in MACRO_KW):
        cats.append("Macro")
    if any(kw in combined for kw in CRYPTO_KW):
        cats.append("Crypto")
    if any(kw in combined for kw in COMMODITIES_KW):
        cats.append("Commodities")
    if any(kw in combined for kw in AI_TECH_KW):
        cats.append("AI & Tech")
    
    # Portfolio match
    ticker_set = set(tickers or [])
    if ticker_set & PORTFOLIO_SET:
        cats.append("Portfolio")
    
    if not cats:
        cats.append("Market")
    return cats

def _relevance_score(title, description, tickers, categories, published_str):
    """Score 0-100 for relevance to this user's portfolio & interests."""
    score = 30  # base

    combined = ((title or "") + " " + (description or "")).lower()
    ticker_set = set(tickers or [])
    
    # Portfolio ticker mention boost
    portfolio_matches = ticker_set & PORTFOLIO_SET
    score += min(len(portfolio_matches) * 12, 36)
    
    # Category boosts for this user's interests
    if "Geopolitical" in categories:
        score += 10  # Iran/conflict relevant
    if "Crypto" in categories:
        score += 8   # BTC miners in portfolio
    if "Commodities" in categories:
        score += 8   # SLV/IAU/PALL in portfolio
    if "AI & Tech" in categories:
        score += 10  # NVDA/PLTR/VRT etc.
    if "Macro" in categories:
        score += 6
    
    # Recency boost
    try:
        for fmt in ["%Y-%m-%dT%H:%M:%SZ","%Y-%m-%dT%H:%M:%S.%fZ",
                     "%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M:%S.%f",
                     "%a, %d %b %Y %H:%M:%S %Z","%a, %d %b %Y %H:%M:%S %z"]:
            try:
                dt = datetime.datetime.strptime(published_str.strip(), fmt)
                break
            except ValueError:
                continue
        else:
            dt = None
        if dt:
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            hours_old = (datetime.datetime.utcnow() - dt).total_seconds() / 3600
            if hours_old < 2: score += 12
            elif hours_old < 6: score += 8
            elif hours_old < 24: score += 4
    except Exception:
        pass

    # Big-name source boost
    source_text = combined
    if any(s in source_text for s in ['bloomberg','reuters','wsj','financial times','ft.com','cnbc']):
        score += 5

    return min(score, 100)

def _dedup_key(title):
    """Generate a normalised key for dedup."""
    if not title:
        return ""
    # Remove punctuation, lowercase, take first N words
    clean = re.sub(r'[^a-z0-9 ]', '', title.lower()).strip()
    words = clean.split()[:6]
    return " ".join(words)

def _dedup_articles(articles):
    """Fast dedup using normalised title key + URL dedup."""
    seen_keys = set()
    seen_urls = set()
    unique = []
    for a in articles:
        # URL-based dedup (exact)
        url = (a.get("url","") or "").strip().rstrip("/")
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        # Title-based dedup (normalised first 6 words)
        key = _dedup_key(a.get("title",""))
        if not key or len(key) < 8:
            continue
        if key in seen_keys:
            continue
        seen_keys.add(key)
        # Also add a shorter 4-word key to catch near-dupes across sources
        words = key.split()
        if len(words) >= 4:
            short_key = " ".join(words[:4])
            if short_key in seen_keys and len(short_key) > 15:
                continue
            seen_keys.add(short_key)
        unique.append(a)
    return unique


# ── SOURCE FETCHERS ──────────────────────────────────────

def fetch_massive_news():
    """Polygon/Massive Reference News — stock-specific with AI sentiment."""
    articles = []
    try:
        url = f"https://api.polygon.io/v2/reference/news?limit=50&order=desc&sort=published_utc&apiKey={MASSIVE_KEY}"
        data = _safe_json(url)
        if not data:
            return articles
        for a in data.get("results", []):
            tickers = a.get("tickers", []) or []
            insights = []
            for ins in (a.get("insights", []) or []):
                insights.append({
                    "ticker": ins.get("ticker",""),
                    "sentiment": ins.get("sentiment",""),
                    "reasoning": (ins.get("sentiment_reasoning","") or "")[:200],
                })
            articles.append({
                "title": a.get("title",""),
                "url": a.get("article_url",""),
                "source": (a.get("publisher",{}) or {}).get("name","Market Data"),
                "published": a.get("published_utc",""),
                "tickers": tickers[:6],
                "image": a.get("image_url",""),
                "description": (a.get("description","") or "")[:300],
                "keywords": (a.get("keywords",[]) or [])[:5],
                "insights": insights,
                "_origin": "massive",
            })
    except Exception as e:
        print(f"[NewsFeed] Massive error: {e}")
    return articles

def fetch_benzinga_news():
    """Benzinga via Massive proxy — real-time structured articles."""
    articles = []
    try:
        url = f"https://api.polygon.io/benzinga/v2/news?apiKey={MASSIVE_KEY}&pageSize=40&sort=published&displayOutput=full"
        data = _safe_json(url)
        if not data:
            return articles
        items = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for a in items:
            # Extract tickers from title and body text
            title = a.get("title","")
            teaser = a.get("teaser","") or ""
            tickers = _extract_tickers(title + " " + teaser)
            channels = a.get("channels") or []
            if isinstance(channels, list) and channels and isinstance(channels[0], str):
                chan_names = channels
            else:
                chan_names = [c.get("name","") for c in channels if isinstance(c, dict)]
            tags = a.get("tags") or []
            if isinstance(tags, list) and tags and isinstance(tags[0], str):
                tag_names = tags
            else:
                tag_names = [t.get("name","") for t in tags if isinstance(t, dict)]
            images = a.get("images") or []
            image = ""
            if images:
                if isinstance(images[0], dict):
                    image = images[0].get("url","")
                elif isinstance(images[0], str):
                    image = images[0]
            articles.append({
                "title": title,
                "url": a.get("url",""),
                "source": "Benzinga",
                "published": a.get("published",""),
                "tickers": tickers[:6],
                "image": image,
                "description": teaser[:300],
                "keywords": (chan_names + tag_names)[:5],
                "insights": [],
                "_origin": "benzinga",
            })
    except Exception as e:
        print(f"[NewsFeed] Benzinga error: {e}")
    return articles

def fetch_fmp_stock_news():
    """FMP Stock News Latest — multi-source stock news."""
    articles = []
    try:
        url = f"https://financialmodelingprep.com/stable/news/stock-latest?page=0&limit=40&apikey={FMP_KEY}"
        data = _safe_json(url)
        if not data:
            return articles
        for a in (data if isinstance(data, list) else []):
            sym = a.get("symbol","")
            articles.append({
                "title": a.get("title",""),
                "url": a.get("url",""),
                "source": a.get("publisher","") or a.get("site","FMP"),
                "published": a.get("publishedDate",""),
                "tickers": [sym] if sym else [],
                "image": a.get("image",""),
                "description": (a.get("text","") or "")[:300],
                "keywords": [],
                "insights": [],
                "_origin": "fmp_stock",
            })
    except Exception as e:
        print(f"[NewsFeed] FMP Stock News error: {e}")
    return articles

def fetch_fmp_general_news():
    """FMP General News Latest — macro/general market news."""
    articles = []
    try:
        url = f"https://financialmodelingprep.com/stable/news/general-latest?page=0&limit=30&apikey={FMP_KEY}"
        data = _safe_json(url)
        if not data:
            return articles
        for a in (data if isinstance(data, list) else []):
            title = a.get("title","")
            text = a.get("text","") or ""
            tickers = _extract_tickers(title + " " + text)
            articles.append({
                "title": title,
                "url": a.get("url",""),
                "source": a.get("publisher","") or a.get("site",""),
                "published": a.get("publishedDate",""),
                "tickers": tickers[:6],
                "image": a.get("image",""),
                "description": text[:300],
                "keywords": [],
                "insights": [],
                "_origin": "fmp_general",
            })
    except Exception as e:
        print(f"[NewsFeed] FMP General error: {e}")
    return articles

def fetch_fmp_crypto_news():
    """FMP Crypto News Latest."""
    articles = []
    try:
        url = f"https://financialmodelingprep.com/stable/news/crypto-latest?page=0&limit=20&apikey={FMP_KEY}"
        data = _safe_json(url)
        if not data:
            return articles
        for a in (data if isinstance(data, list) else []):
            sym = a.get("symbol","")
            articles.append({
                "title": a.get("title",""),
                "url": a.get("url",""),
                "source": a.get("publisher","") or a.get("site",""),
                "published": a.get("publishedDate",""),
                "tickers": [sym] if sym else [],
                "image": a.get("image",""),
                "description": (a.get("text","") or "")[:300],
                "keywords": [],
                "insights": [],
                "_origin": "fmp_crypto",
            })
    except Exception as e:
        print(f"[NewsFeed] FMP Crypto error: {e}")
    return articles

def fetch_fmp_articles():
    """FMP proprietary articles — analyst-quality with full content."""
    articles = []
    try:
        url = f"https://financialmodelingprep.com/stable/fmp-articles?page=0&limit=20&apikey={FMP_KEY}"
        data = _safe_json(url)
        if not data:
            return articles
        for a in (data if isinstance(data, list) else []):
            # Extract tickers from "NYSE:MRK" format
            ticker_raw = a.get("tickers","") or ""
            tickers = [t.split(":")[-1] for t in ticker_raw.split(",") if t.strip()]
            # Strip HTML from content for description
            content = a.get("content","") or ""
            clean = re.sub(r'<[^>]+>', '', content)[:300]
            articles.append({
                "title": a.get("title",""),
                "url": a.get("link",""),
                "source": "FMP Analysis",
                "published": a.get("date",""),
                "tickers": tickers[:6],
                "image": a.get("image",""),
                "description": clean,
                "keywords": [],
                "insights": [],
                "_origin": "fmp_articles",
            })
    except Exception as e:
        print(f"[NewsFeed] FMP Articles error: {e}")
    return articles

def fetch_marketaux():
    """MarketAux — rich sentiment scoring per entity."""
    articles = []
    try:
        url = f"https://api.marketaux.com/v1/news/all?countries=us&filter_entities=true&limit=20&api_token={MARKETAUX_KEY}"
        raw = _http_get(url, timeout=6)
        data = json.loads(raw)
        if not data:
            return articles
        for a in data.get("data",[]):
            tickers = []
            sentiments = []
            for ent in (a.get("entities",[]) or []):
                sym = ent.get("symbol","")
                if sym:
                    tickers.append(sym)
                    sent = ent.get("sentiment_score")
                    if sent is not None:
                        label = "positive" if sent > 0.15 else "negative" if sent < -0.15 else "neutral"
                        sentiments.append({
                            "ticker": sym,
                            "sentiment": label,
                            "reasoning": f"Score: {sent:.2f}",
                        })
            articles.append({
                "title": a.get("title",""),
                "url": a.get("url",""),
                "source": a.get("source","MarketAux"),
                "published": a.get("published_at",""),
                "tickers": tickers[:6],
                "image": (a.get("image_url","") or ""),
                "description": (a.get("description","") or a.get("snippet","") or "")[:300],
                "keywords": [],
                "insights": sentiments,
                "_origin": "marketaux",
            })
    except Exception as e:
        print(f"[NewsFeed] MarketAux error: {e}")
    return articles

def fetch_bloomberg_rss():
    """Bloomberg Markets RSS feed."""
    articles = []
    try:
        xml = _http_get("https://feeds.bloomberg.com/markets/news.rss", timeout=10)
        raw = _parse_rss(xml)
        for r in raw[:20]:
            r["source"] = "Bloomberg"
            r["tickers"] = _extract_tickers(r.get("title","") + " " + r.get("description",""))
            r["keywords"] = []
            r["insights"] = []
            r["_origin"] = "bloomberg_rss"
            articles.append(r)
    except Exception as e:
        print(f"[NewsFeed] Bloomberg RSS error: {e}")
    return articles

def fetch_cnbc_rss():
    """CNBC Top News RSS."""
    articles = []
    try:
        xml = _http_get("https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", timeout=10)
        raw = _parse_rss(xml)
        for r in raw[:15]:
            r["source"] = "CNBC"
            r["tickers"] = _extract_tickers(r.get("title","") + " " + r.get("description",""))
            r["keywords"] = []
            r["insights"] = []
            r["_origin"] = "cnbc_rss"
            articles.append(r)
    except Exception as e:
        print(f"[NewsFeed] CNBC RSS error: {e}")
    return articles


# ── MAIN AGGREGATOR ──────────────────────────────────────

def _fetch_all_sources():
    """Raw fetch from all sources — runs in background thread only."""
    import time as _time
    t0 = _time.time()
    HARD_DEADLINE = 40  # seconds (we're in a bg thread, no request timeout)

    all_articles = []
    source_counts = {}

    sources = [
        ("massive", fetch_massive_news),
        ("benzinga", fetch_benzinga_news),
        ("bloomberg_rss", fetch_bloomberg_rss),
        ("cnbc_rss", fetch_cnbc_rss),
    ]

    for name, fn in sources:
        if _time.time() - t0 > HARD_DEADLINE:
            print(f"[NewsFeed] Deadline hit, skipping remaining sources")
            break
        try:
            result = fn()
            all_articles.extend(result)
            source_counts[name] = len(result)
            print(f"[NewsFeed] {name}: {len(result)} articles")
        except Exception as e:
            source_counts[name] = 0
            print(f"[NewsFeed] {name} failed: {e}")

    # MarketAux (can be slow)
    if _time.time() - t0 < HARD_DEADLINE - 8:
        try:
            result = fetch_marketaux()
            all_articles.extend(result)
            source_counts["marketaux"] = len(result)
            print(f"[NewsFeed] marketaux: {len(result)} articles")
        except Exception as e:
            source_counts["marketaux"] = 0
            print(f"[NewsFeed] marketaux failed: {e}")
    else:
        source_counts["marketaux"] = 0

    # FMP sources last (rate-limit sensitive)
    for name, fn in [("fmp_stock", fetch_fmp_stock_news), ("fmp_general", fetch_fmp_general_news)]:
        if _time.time() - t0 > HARD_DEADLINE - 3:
            source_counts[name] = 0
            print(f"[NewsFeed] Skipping {name} (near deadline)")
            continue
        try:
            result = fn()
            all_articles.extend(result)
            source_counts[name] = len(result)
            print(f"[NewsFeed] {name}: {len(result)} articles")
        except Exception as e:
            source_counts[name] = 0
            print(f"[NewsFeed] {name} failed: {e}")
        _time.sleep(0.3)

    # Deduplicate
    unique = _dedup_articles(all_articles)

    # Categorise and score
    for a in unique:
        tickers = a.get("tickers",[])
        cats = _categorise(a.get("title",""), a.get("description",""), tickers)
        a["categories"] = cats
        a["relevance_score"] = _relevance_score(
            a.get("title",""), a.get("description",""),
            tickers, cats, a.get("published","")
        )
        a["time_ago"] = _time_ago(a.get("published",""))
        desc = a.get("description","") or ""
        if len(desc) > 120:
            sentences = re.split(r'(?<=[.!?])\s+', desc)
            a["summary"] = " ".join(sentences[:2])[:200]
        else:
            a["summary"] = desc[:200]
        if "_origin" in a:
            del a["_origin"]

    unique.sort(key=lambda a: a.get("relevance_score",0), reverse=True)

    elapsed = round(_time.time() - t0, 1)
    print(f"[NewsFeed] Fetched {len(all_articles)} raw, {len(unique)} unique in {elapsed}s")

    return {
        "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "total_fetched": len(all_articles),
        "all_unique": unique,
        "source_counts": source_counts,
    }

def aggregate_news_feed(category_filter=None, ticker_filter=None, limit=60):
    """
    Return cached articles if available, otherwise return warming status.
    Cache is populated/refreshed by background thread.
    """
    import copy
    
    with _cache_lock:
        cached = _cache["data"]
    
    if not cached:
        # Trigger a refresh (non-blocking)
        _trigger_refresh()
        return {
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "total_fetched": 0,
            "total_unique": 0,
            "source_counts": {},
            "category_counts": {},
            "articles": [],
            "status": "warming",
        }
    
    # Trigger background refresh if stale
    import time as _time
    with _cache_lock:
        age = _time.time() - _cache["ts"]
    if age > _CACHE_TTL:
        _trigger_refresh()
    
    unique = copy.deepcopy(cached["all_unique"])

    # Apply filters
    if category_filter and category_filter != "all":
        cat_map = {
            "portfolio": "Portfolio",
            "macro": "Macro",
            "geopolitical": "Geopolitical",
            "crypto": "Crypto",
            "commodities": "Commodities",
            "ai": "AI & Tech",
        }
        target = cat_map.get(category_filter.lower(), category_filter)
        unique = [a for a in unique if target in a.get("categories",[])]

    if ticker_filter:
        tf = set(t.strip().upper() for t in ticker_filter.split(",") if t.strip())
        unique = [a for a in unique if set(a.get("tickers",[])) & tf]

    unique = unique[:limit]

    cat_counts = {}
    for a in unique:
        for c in a.get("categories",[]):
            cat_counts[c] = cat_counts.get(c,0) + 1

    return {
        "updated_at": cached["updated_at"],
        "total_fetched": cached["total_fetched"],
        "total_unique": len(unique),
        "source_counts": cached["source_counts"],
        "category_counts": cat_counts,
        "articles": unique,
    }


_refresh_in_progress = False

def _trigger_refresh():
    """Start background refresh if not already running."""
    global _refresh_in_progress
    if _refresh_in_progress:
        return
    _refresh_in_progress = True
    
    def _do():
        global _refresh_in_progress
        try:
            fresh = _fetch_all_sources()
            with _cache_lock:
                _cache["data"] = fresh
                _cache["ts"] = __import__('time').time()
            print(f"[NewsFeed] Cache refreshed: {fresh.get('total_fetched',0)} articles")
        except Exception as e:
            print(f"[NewsFeed] Refresh failed: {e}")
        finally:
            _refresh_in_progress = False
    
    t = threading.Thread(target=_do, daemon=True)
    t.start()


# Cache warms lazily on first API request (no import-time thread)
# This avoids gunicorn fork issues with --preload
