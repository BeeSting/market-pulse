#!/usr/bin/env python3
"""
Conflict Timeline — LLM-powered geopolitical intelligence feed.
Aggregates news from news_feed cache + Polygon geopolitical search,
then uses GPT-4o-mini to produce a structured conflict timeline with
market impact analysis.

Endpoint: GET /api/conflict-timeline
Cache TTL: 300 seconds (5 min)
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
OPENAI_KEY  = os.environ.get("OPENAI_KEY",  "")
MASSIVE_KEY = os.environ.get("MASSIVE_KEY", "8DLmlXSQ8eaVqjUtNPskYcbLhLasNv1I")
FMP_KEY     = os.environ.get("FMP_KEY",     "EINiL3Pzp1f0YjvQgcnm8t3hBBShCdMd")

# ── Cache ─────────────────────────────────────────────────
_cache_lock          = threading.Lock()
_cache               = {"data": None, "ts": 0}
_CACHE_TTL           = 300   # 5 minutes
_refresh_in_progress = False


# ── HTTP helpers ──────────────────────────────────────────
def _http_get(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "MarketPulse/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _safe_json(url, timeout=12):
    try:
        return json.loads(_http_get(url, timeout))
    except Exception:
        return None


# ── Data gatherers ────────────────────────────────────────

def _fetch_polygon_geo_news():
    """Pull geopolitical news from Polygon filtered for oil/gold/defense tickers."""
    articles = []
    try:
        url = (
            "https://api.polygon.io/v2/reference/news"
            "?ticker.any_of=USO,XLE,OXY,GLD,SLV"
            "&limit=20&order=desc&sort=published_utc"
            f"&apiKey={MASSIVE_KEY}"
        )
        data = _safe_json(url, timeout=12)
        if not data:
            return articles
        for a in data.get("results", []):
            title = a.get("title", "") or ""
            desc  = (a.get("description", "") or "")[:300]
            pub   = a.get("publisher", {}) or {}
            articles.append({
                "title":       title,
                "description": desc,
                "published":   a.get("published_utc", ""),
                "source":      pub.get("name", ""),
                "url":         a.get("article_url", ""),
                "tickers":     a.get("tickers", [])[:6],
            })
    except Exception as e:
        print(f"[ConflictTimeline] Polygon geo news error: {e}")
    return articles


def _fetch_news_feed_geo():
    """Pull 'Geopolitical' articles from the news_feed module cache."""
    articles = []
    try:
        # Import lazily to avoid circular imports at module load
        from news_feed import aggregate_news_feed
        result = aggregate_news_feed(category_filter="geopolitical", limit=30)
        for a in result.get("articles", []):
            articles.append({
                "title":       a.get("title", "") or "",
                "description": (a.get("description", "") or a.get("summary", "") or "")[:300],
                "published":   a.get("published", ""),
                "source":      a.get("source", ""),
                "url":         a.get("url", ""),
                "tickers":     a.get("tickers", [])[:6],
            })
    except Exception as e:
        print(f"[ConflictTimeline] news_feed geo pull error: {e}")
    return articles


def _fetch_market_impact():
    """Fetch spot prices for oil, gold, silver, and BTC to include in the LLM prompt."""
    try:
        url = (
            "https://financialmodelingprep.com/stable/batch-quote"
            "?symbols=GCUSD,SILUSD,CLUSD,BZUSD,BTCUSD"
            f"&apikey={FMP_KEY}"
        )
        data = _safe_json(url, timeout=12)
        if not data or not isinstance(data, list):
            return {}
        market = {}
        sym_map = {
            "GCUSD":  "Gold",
            "SILUSD": "Silver",
            "CLUSD":  "WTI Crude",
            "BZUSD":  "Brent Crude",
            "BTCUSD": "Bitcoin",
        }
        for q in data:
            sym = q.get("symbol", "")
            if sym in sym_map:
                price  = q.get("price", 0) or 0
                chg    = q.get("change", 0) or 0
                chg_pct = q.get("changePercentage", 0) or 0
                prev   = q.get("previousClose", 0) or 0
                market[sym_map[sym]] = {
                    "price":           round(price, 2),
                    "change":          round(chg, 2),
                    "changePercent":   round(chg_pct, 2),
                    "previousClose":   round(prev, 2),
                }
        return market
    except Exception as e:
        print(f"[ConflictTimeline] Market impact fetch error: {e}")
        return {}


# ── LLM call ─────────────────────────────────────────────

def _call_openai(articles, market_data):
    """POST to OpenAI chat completions and return parsed JSON response."""

    # Build a compact article list for the prompt
    articles_text = ""
    seen = set()
    for a in articles[:30]:
        title = (a.get("title") or "").strip()
        if not title or title in seen:
            continue
        seen.add(title)
        desc = (a.get("description") or "").strip()
        pub  = a.get("published", "")[:10]
        line = f"[{pub}] {title}"
        if desc:
            line += f" — {desc[:150]}"
        articles_text += line + "\n"

    if not articles_text.strip():
        articles_text = "No recent geopolitical articles found."

    # Format market data
    market_lines = []
    for asset, vals in market_data.items():
        chg_pct = vals.get("changePercent", 0)
        sign    = "+" if chg_pct >= 0 else ""
        market_lines.append(
            f"{asset}: ${vals.get('price', 0):,.2f} ({sign}{chg_pct:.2f}%)"
        )
    market_text = "\n".join(market_lines) if market_lines else "Market data unavailable."

    system_prompt = (
        "You are a geopolitical intelligence analyst for an investment fund. "
        "Analyze these news articles about the Iran/Middle East conflict and create a concise "
        "conflict timeline. Focus on events that impact oil, gold, crypto, and equity markets. "
        "Include specific market moves where mentioned."
    )

    user_prompt = f"""RECENT GEOPOLITICAL NEWS ARTICLES:
{articles_text}

CURRENT MARKET PRICES (today's moves):
{market_text}

Respond with ONLY valid JSON in this exact structure:
{{
  "alert_level": "critical|high|elevated|moderate",
  "headline": "One-line alert headline summarising the current situation",
  "events": [
    {{"date": "Mar 3", "text": "Event description", "severity": "critical|high|medium|low"}},
    {{"date": "Mar 2", "text": "Event description", "severity": "critical|high|medium|low"}}
  ],
  "market_impact": "2-3 sentence market impact summary with specific price moves from the data above"
}}

Include 4-8 events in the timeline, most recent first. Do not add any text outside the JSON."""

    payload = json.dumps({
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "temperature": 0.3,
        "max_tokens":  800,
        "response_format": {"type": "text"},
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {OPENAI_KEY}",
            "User-Agent":    "MarketPulse/2.0",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = json.loads(resp.read().decode("utf-8"))

    content = raw["choices"][0]["message"]["content"].strip()

    # Strip markdown code fences if present
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()

    return json.loads(content)


# ── Core builder ─────────────────────────────────────────

def _build_timeline():
    """Gather data and call LLM. Returns the timeline dict."""

    # Fetch all data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        f_polygon = pool.submit(_fetch_polygon_geo_news)
        f_feed    = pool.submit(_fetch_news_feed_geo)
        f_market  = pool.submit(_fetch_market_impact)

        polygon_articles = []
        feed_articles    = []
        market_data      = {}

        try:
            polygon_articles = f_polygon.result(timeout=15)
        except Exception as e:
            print(f"[ConflictTimeline] Polygon articles timeout: {e}")

        try:
            feed_articles = f_feed.result(timeout=20)
        except Exception as e:
            print(f"[ConflictTimeline] Feed articles timeout: {e}")

        try:
            market_data = f_market.result(timeout=15)
        except Exception as e:
            print(f"[ConflictTimeline] Market data timeout: {e}")

    # Merge and de-duplicate by title
    all_articles = []
    seen_titles  = set()
    for a in (feed_articles + polygon_articles):
        title = (a.get("title") or "").strip().lower()
        if title and title not in seen_titles:
            seen_titles.add(title)
            all_articles.append(a)

    # Call LLM
    try:
        llm_result = _call_openai(all_articles, market_data)
    except Exception as e:
        print(f"[ConflictTimeline] LLM call failed: {e}")
        # Return a graceful degraded response with raw data
        llm_result = {
            "alert_level":    "moderate",
            "headline":       "Geopolitical monitoring active — LLM analysis temporarily unavailable",
            "events":         [],
            "market_impact":  "Market impact analysis unavailable.",
            "llm_error":      str(e),
        }

    return {
        "updated_at":     datetime.datetime.utcnow().isoformat() + "Z",
        "alert_level":    llm_result.get("alert_level",   "moderate"),
        "headline":       llm_result.get("headline",      ""),
        "events":         llm_result.get("events",        []),
        "market_impact":  llm_result.get("market_impact", ""),
        "market_data":    market_data,
        "article_count":  len(all_articles),
        "status":         "ok",
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
            fresh = _build_timeline()
            with _cache_lock:
                _cache["data"] = fresh
                _cache["ts"]   = time.time()
            print(f"[ConflictTimeline] Cache refreshed — alert_level={fresh.get('alert_level')}")
        except Exception as e:
            print(f"[ConflictTimeline] Refresh failed: {e}")
        finally:
            _refresh_in_progress = False

    t = threading.Thread(target=_do, daemon=True)
    t.start()


# ── Public export ─────────────────────────────────────────

def get_conflict_timeline():
    """
    Return cached conflict timeline data, or trigger a warm-up if the cache
    is empty, or kick off a background refresh if the data is stale.
    """
    with _cache_lock:
        cached = _cache["data"]

    if not cached:
        _trigger_refresh()
        return {
            "updated_at":    datetime.datetime.utcnow().isoformat() + "Z",
            "alert_level":   "moderate",
            "headline":      "Geopolitical intelligence feed is warming up…",
            "events":        [],
            "market_impact": "",
            "market_data":   {},
            "article_count": 0,
            "status":        "warming",
        }

    # Trigger background refresh if stale
    with _cache_lock:
        age = time.time() - _cache["ts"]
    if age > _CACHE_TTL:
        _trigger_refresh()

    return cached
