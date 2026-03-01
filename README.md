# Market Pulse — US Stocks & Commodities Dashboard

Live market intelligence dashboard with real-time price feeds, portfolio tracking, and news integration.

## Features

- **16-Section Market Report** — Equities, commodities, crypto, geopolitical analysis, Fed outlook
- **Portfolio Tracker** — 37-position portfolio with live P&L, sector allocation
- **Sell-Off Buy List** — Pre-positioned buy targets across 5 categories
- **Live News Ticker** — Scrolling headlines from financial news feeds
- **One-Click Refresh** — Live prices via market data APIs with automatic fallback
- **Dark/Light Mode** — Bloomberg-inspired design, mobile responsive

## Tech Stack

- **Frontend**: Vanilla HTML/CSS/JS (single-page, no framework)
- **Backend**: Python Flask + Gunicorn
- **APIs**: Polygon.io (primary), FMP (fallback)
- **Deploy**: Railway

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `POLYGON_KEY` | Polygon.io API key | Yes |
| `FMP_KEY` | Financial Modeling Prep API key | Yes |
| `PORT` | Server port (auto-set by Railway) | No |

## Local Development

```bash
pip install -r requirements.txt
POLYGON_KEY=your_key FMP_KEY=your_key python server.py
```

Open `http://localhost:8080`
