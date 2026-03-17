# =============================================================
# cloud_alert_engine.py  —  24x7 Cloud Alert Engine
# SESSION 37  —  Full rebuild
#
# TWO MODES (set by CLI arg):
#   --morning-brief   → 8:30am IST daily summary to Telegram
#   (default)         → intraday scan (every 30min, 7am-8pm IST)
#
# Runs on GitHub Actions (see alert_engine.yml for cron schedule)
#
# Required GitHub Secrets (repo Settings → Secrets → Actions):
#   TELEGRAM_TOKEN     — from @BotFather
#   TELEGRAM_CHAT_ID   — your personal or group chat ID
#   GROQ_API_KEY       — optional, improves RSS classification
#
# Intraday scan does:
#   1. Load watchlist_for_cloud.csv (PRIME/STRONG/WLC/WLE stocks)
#   2. Fetch BSE corporate announcements (last 2h)
#   3. Fetch NSE corporate announcements (last 2h)
#   4. Fetch macro RSS feeds (top 5 sources) → sector-wide alert
#   5. Classify + cross-check against watchlist
#   6. Deduplicate (seen_alerts.json stored in repo)
#   7. Send Telegram alerts + commit seen_alerts.json
#
# Morning brief does:
#   1. Read composite_scores.csv, market_intelligence.csv,
#      sector_cycle_status.csv from repo
#   2. Build formatted summary message
#   3. Send one Telegram message at 8:30am IST
# =============================================================

import os, sys, json, re, time
from pathlib import Path
from datetime import datetime, timedelta, timezone

try:
    import requests
    import pandas as pd
except ImportError:
    os.system("pip install requests pandas -q")
    import requests
    import pandas as pd

# ── Config ────────────────────────────────────────────────────
# Strip whitespace — prevents silent failure if secret was pasted with spaces
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
SEEN_FILE         = Path("seen_alerts.json")
WATCHLIST         = Path("watchlist_for_cloud.csv")
COMPOSITE_CSV     = Path("composite_scores.csv")
MARKET_INTEL_CSV  = Path("market_intelligence.csv")
SECTOR_CYCLE_CSV  = Path("sector_cycle_status.csv")
BSE_LOOKBACK_HRS  = 2
MODE              = "morning_brief" if "--morning-brief" in sys.argv else "intraday"

# ── Event classification rules ────────────────────────────────
CLASSIFY_RULES = [
    ("ORDER_WIN",    r"order|contract|work order|letter of (intent|award)|awarded|bagged|secured|lo[ia]\b"),
    ("RESULT_BEAT",  r"(profit|revenue|net income).{0,20}(jump|surge|rise|grew|up).{0,10}\d+\s*%|beat.{0,20}estimate"),
    ("CAPEX",        r"capex|capital expenditure|new plant|capacity expansion|greenfield|brownfield"),
    ("FUNDRAISE",    r"qip|rights issue|preferential allotment|fpo|ncd|debenture"),
    ("DIVIDEND",     r"dividend|interim dividend|special dividend"),
    ("BUYBACK",      r"buyback|buy.?back|share repurchase"),
    ("ACQUISITION",  r"acqui|takeover|merger|amalgamation|demerger"),
    ("MGMT_CHANGE",  r"appoint|new (ceo|md|cfo|director)|resign|step.?down"),
    ("PENALTY",      r"penalty|fine|sebi|tax demand|gst demand|show cause|enforcement"),
    ("RATING_UP",    r"upgrade|target (raised|hiked|increased)|buy rating|overweight"),
    ("RATING_DOWN",  r"downgrade|target (cut|reduced|lowered)|sell rating|underweight"),
    ("RESULT",       r"quarterly result|financial result|unaudited|q[1-4] result"),
]

SEVERITY_EMOJI = {
    "ORDER_WIN":   "🟢", "RESULT_BEAT": "🟢", "RATING_UP": "🟢",
    "CAPEX":       "🔵", "FUNDRAISE":   "🔵", "ACQUISITION": "🔵", "BUYBACK": "🔵",
    "DIVIDEND":    "⚪", "RESULT":      "⚪",
    "MGMT_CHANGE": "🟡",
    "PENALTY":     "🔴", "RATING_DOWN": "🔴",
}

TIER_LABEL = {
    "PRIME":               "⭐ PRIME",
    "STRONG":              "💪 STRONG",
    "WATCHLIST_CONFIRMED": "👀 WL-CONFIRMED",
    "WATCHLIST_EXTERNAL":  "📋 WL-EXTERNAL",
}

# ── Macro RSS sources ─────────────────────────────────────────
RSS_FEEDS = [
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/marketreports.xml",
    "https://feeds.feedburner.com/businessstandard/markets",
    "https://www.livemint.com/rss/markets",
    "https://zeenews.india.com/rss/business.xml",
]

# Sector keyword map for RSS macro classification
SECTOR_RSS_MAP = {
    "Oil": [
        "crude", "oil price", "brent", "wti", "opec", "petroleum",
        "refinery", "iran", "oil ministry", "petroleum minister"
    ],
    "Banks": [
        "rbi", "repo rate", "banking sector", "npa", "credit growth",
        "idbi bank", "bank privatisation", "monetary policy", "crr"
    ],
    "IT - Software": [
        "rupee", "dollar appreciation", "it outsourcing", "visa h1b",
        "tech layoff", "software export", "nasscom"
    ],
    "Pharmaceuticals & Biotechnology": [
        "pharma", "usfda", "drug approval", "generic drug", "fda warning",
        "medicine price", "drug controller"
    ],
    "Chemicals & Petrochemicals": [
        "chemical", "petrochemical", "agrochemical", "specialty chemical"
    ],
    "Fertilizers & Agrochemicals": [
        "fertiliser", "urea", "subsidy", "kharif", "rabi", "msp", "agro"
    ],
    "Metals & Minerals Trading": [
        "steel", "metal", "iron ore", "aluminium", "copper", "zinc", "nickel"
    ],
    "Automobiles": [
        "auto sales", "ev policy", "vehicle", "automobile", "two-wheeler",
        "passenger vehicle", "ev subsidy", "scrappage"
    ],
    "Realty": [
        "real estate", "housing", "reit", "property market",
        "affordable housing", "stamp duty"
    ],
    "Capital Markets": [
        "sebi", "market regulator", "fii inflow", "fii outflow",
        "fpi", "stock market", "nifty", "sensex", "d-street"
    ],
    "Finance": [
        "nbfc", "mutual fund", "amc", "asset management", "financial fraud",
        "loan growth", "microfinance"
    ],
    "Construction": [
        "infra spend", "infrastructure budget", "road", "highway",
        "govt contract", "nh", "nhai", "construction sector"
    ],
    "Power": [
        "power demand", "electricity", "renewable energy", "solar tariff",
        "wind energy", "power ministry", "discoms", "coal shortage"
    ],
    "Aerospace & Defense": [
        "defence", "defense", "drdo", "hal", "bhel", "military",
        "arms deal", "indigenisation", "make in india defence"
    ],
}


# ── Helpers ───────────────────────────────────────────────────

def classify(text: str) -> str:
    t = text.lower()
    for etype, pattern in CLASSIFY_RULES:
        if re.search(pattern, t, re.IGNORECASE):
            return etype
    return "NEWS"


def load_watchlist() -> dict:
    if not WATCHLIST.exists():
        print(f"  [WARN] watchlist_for_cloud.csv not found in repo")
        return {}
    df = pd.read_csv(WATCHLIST, low_memory=False)
    result = {}
    for _, r in df.iterrows():
        isin = str(r.get("ISIN","")).strip()
        if isin:
            result[isin] = {
                "symbol":   str(r.get("NSE_SYMBOL","")),
                "name":     str(r.get("NAME","")),
                "tier":     str(r.get("TIER","")),
                "signals":  int(float(str(r.get("SIGNALS_FIRED", 0) or 0))),
                "score":    int(float(str(r.get("COMPOSITE_BALANCED", 0) or 0))),
                "phase":    str(r.get("SECTOR_PHASE","")),
            }
    print(f"  Watchlist: {len(result)} stocks loaded")
    return result


def load_seen() -> dict:
    if SEEN_FILE.exists():
        try:
            data = json.loads(SEEN_FILE.read_text())
            cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            return {k: v for k, v in data.items() if v >= cutoff}
        except Exception:
            return {}
    return {}


def save_seen(seen: dict):
    SEEN_FILE.write_text(json.dumps(seen, indent=2))


def _check_telegram_config():
    """Print diagnostic on startup so logs always show secret status."""
    tok_ok  = bool(TELEGRAM_TOKEN)
    cid_ok  = bool(TELEGRAM_CHAT_ID)
    tok_len = len(TELEGRAM_TOKEN)
    cid_val = TELEGRAM_CHAT_ID if cid_ok else "MISSING"
    print(f"  Telegram config  : TOKEN={'SET (' + str(tok_len) + ' chars)' if tok_ok else 'MISSING'}"
          f"  CHAT_ID={cid_val}")
    if not tok_ok or not cid_ok:
        print(f"  [WARN] Telegram not configured — alerts will NOT be sent.")
        print(f"  [WARN] Check GitHub Secrets: TELEGRAM_TOKEN + TELEGRAM_CHAT_ID")


def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        # Return False — do NOT mark as sent when config is missing
        return False
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            return True
        print(f"  [TG ERROR] {r.status_code}: {r.text[:300]}")
        return False
    except Exception as e:
        print(f"  [TG ERROR] {e}")
        return False


# ── BSE Announcements ─────────────────────────────────────────

def fetch_bse_announcements(hours_back: int = 2) -> list:
    now     = datetime.now(timezone.utc)
    from_dt = (now - timedelta(hours=hours_back)).strftime("%Y%m%d")
    to_dt   = now.strftime("%Y%m%d")
    url = (
        "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
        f"?strCat=-1&strPrevDate={from_dt}&strScrip=&strSearch="
        f"&strToDate={to_dt}&strType=C&subcategory=-1"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":    "https://www.bseindia.com/",
        "Accept":     "application/json",
    }
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        anns = data.get("Table", []) or data.get("data", []) or []
        print(f"  BSE announcements: {len(anns)}")
        return anns
    except Exception as e:
        print(f"  [WARN] BSE fetch failed: {e}")
        return []


# ── NSE Announcements ─────────────────────────────────────────

def fetch_nse_announcements(hours_back: int = 2) -> list:
    """
    Fetch NSE corporate announcements using NSE public API.
    Requires a session cookie obtained from the NSE home page first.
    Returns list of dicts with keys: ISIN, symbol, headline, dt_tm
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.nseindia.com/",
    }
    session = requests.Session()
    session.headers.update(headers)

    try:
        # Step 1: Establish session cookie by hitting NSE homepage
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(1)

        # Step 2: Fetch announcements
        now     = datetime.now()
        from_dt = (now - timedelta(hours=hours_back)).strftime("%d-%m-%Y")
        to_dt   = now.strftime("%d-%m-%Y")
        url = (
            "https://www.nseindia.com/api/corporate-announcements"
            f"?index=equities&from_date={from_dt}&to_date={to_dt}"
        )
        r    = session.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        results = []
        for ann in (data if isinstance(data, list) else []):
            results.append({
                "ISIN":       str(ann.get("isin",     "")),
                "symbol":     str(ann.get("symbol",   "")),
                "headline":   str(ann.get("subject",  "")),
                "dt_tm":      str(ann.get("sort_date",""))[:16],
                "source":     "NSE",
            })
        print(f"  NSE announcements: {len(results)}")
        return results

    except Exception as e:
        print(f"  [WARN] NSE fetch failed: {e}")
        return []


# ── Macro RSS sector alerts ───────────────────────────────────

def _clean_rss_text(raw: str) -> str:
    """Strip CDATA wrappers, HTML tags, and decode entities."""
    # Remove CDATA: <![CDATA[ ... ]]>
    text = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"", raw, flags=re.DOTALL)
    # Remove remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode common HTML entities
    text = (text.replace("&amp;", "&").replace("&lt;", "<")
                .replace("&gt;", ">").replace("&quot;", '"')
                .replace("&#39;", "'").replace("&nbsp;", " "))
    return text.strip()


def fetch_rss_headlines() -> list:
    """
    Fetch headlines from macro RSS feeds.
    Returns list of {title, link, published}
    Handles CDATA-wrapped titles (ET, Mint, BS all use CDATA).
    """
    headlines = []
    for feed_url in RSS_FEEDS:
        try:
            r = requests.get(feed_url, timeout=10,
                             headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                continue
            items = re.findall(r"<item>(.*?)</item>", r.text, re.DOTALL)
            for item in items[:20]:
                # Title: match CDATA or plain
                title_m = re.search(
                    r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                    item, re.DOTALL)
                link_m  = re.search(r"<link>(.*?)</link>", item, re.DOTALL)
                pub_m   = re.search(r"<pubDate>(.*?)</pubDate>", item)
                if not title_m:
                    continue
                title_text = _clean_rss_text(title_m.group(1))
                if not title_text or len(title_text) < 5:
                    continue  # skip empty or garbage
                headlines.append({
                    "title":     title_text[:200],
                    "link":      _clean_rss_text(link_m.group(1)) if link_m else "",
                    "published": pub_m.group(1).strip()[:25] if pub_m else "",
                    "source":    feed_url.split("/")[2],
                })
        except Exception:
            pass
    print(f"  RSS headlines fetched: {len(headlines)}")
    return headlines


def match_rss_to_sectors(headlines: list) -> dict:
    """
    Match RSS headlines to sectors using keyword map.
    Returns {sector_name: [headline, ...]}
    """
    matches = {}
    for h in headlines:
        text = (h.get("title","") + " " + h.get("link","")).lower()
        for sector, keywords in SECTOR_RSS_MAP.items():
            for kw in keywords:
                if kw.lower() in text:
                    matches.setdefault(sector, []).append(h)
                    break  # one match per headline per sector
    return matches


def build_sector_macro_alert(sector: str, headlines: list,
                              watchlist: dict, seen: dict) -> str | None:
    """
    If a sector has macro news AND watchlist stocks in that sector,
    build a Telegram alert message. Returns None if already seen today.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    # Use top headline text as dedup key
    top_hl = headlines[0].get("title","")[:60]
    dedup  = f"MACRO|{sector}|{today}|{top_hl[:30]}"
    if dedup in seen:
        return None

    # Find watchlist stocks in this sector
    sector_stocks = [
        ctx for ctx in watchlist.values()
        if ctx.get("phase","") == sector or
        sector.lower() in ctx.get("name","").lower()
    ]

    # Show top 3 affected stocks
    top_stocks = sorted(sector_stocks, key=lambda x: -x.get("score",0))[:3]

    if not top_stocks:
        return None  # no watchlist stocks in this sector → skip

    # Build message
    lines = [
        f"📰 <b>MACRO ALERT — {sector.upper()}</b>",
        f"{'─'*32}",
    ]
    # Show top 2 headlines
    for h in headlines[:2]:
        title = h.get("title","")[:100]
        src   = h.get("source","")
        lines.append(f"• {title}")
        if src:
            lines.append(f"  <i>— {src}</i>")

    lines.append(f"{'─'*32}")
    lines.append(f"<b>Your watchlist stocks in this sector:</b>")
    for s in top_stocks:
        tier_short = TIER_LABEL.get(s["tier"], s["tier"])
        lines.append(f"  {tier_short} <b>{s['symbol']}</b> | Score {s['score']} | {s['phase']}")

    lines.append(f"{'─'*32}")
    lines.append(f"<i>Multibagger Engine | {datetime.now().strftime('%H:%M IST')}</i>")

    return "\n".join(lines), dedup


# ── Format intraday stock alert ───────────────────────────────

def format_stock_alert(ann: dict, ctx: dict, etype: str, source: str = "BSE") -> str:
    symbol   = ctx["symbol"]
    tier     = TIER_LABEL.get(ctx["tier"], ctx["tier"])
    sigs     = ctx["signals"]
    score    = ctx["score"]
    phase    = ctx["phase"]
    emoji    = SEVERITY_EMOJI.get(etype, "⚪")
    headline = str(ann.get("headline", ann.get("HEADLINE",
               ann.get("SubjectLong", ann.get("subject","")))))[:180]
    ann_time = str(ann.get("dt_tm", ann.get("DT_TM",""))).strip()[:16]

    return (
        f"{emoji} <b>{symbol}</b> | {tier}\n"
        f"<b>{etype}</b>  [{source}]\n"
        f"{'─'*32}\n"
        f"{headline}\n"
        f"{'─'*32}\n"
        f"Score: {score}/100  Signals: {sigs}/15\n"
        f"Sector Phase: {phase}\n"
        f"Time: {ann_time}\n"
        f"<i>Multibagger Engine</i>"
    )


# ── Morning Brief ─────────────────────────────────────────────

def _fetch_global_cues() -> dict:
    """Fetch global market cues using yfinance. Returns dict of {label: value_str}."""
    cues = {}
    tickers = {
        "Dow":       "^DJI",
        "Nasdaq":    "^IXIC",
        "SGX Nifty": "^SGXNIFTY",
        "Crude":     "BZ=F",
        "USD/INR":   "USDINR=X",
        "Gold":      "GC=F",
    }
    try:
        import yfinance as yf
        for label, ticker in tickers.items():
            try:
                t    = yf.Ticker(ticker)
                hist = t.history(period="2d", interval="1d")
                if len(hist) >= 2:
                    prev  = float(hist["Close"].iloc[-2])
                    curr  = float(hist["Close"].iloc[-1])
                    chg   = ((curr - prev) / prev) * 100
                    icon  = "🟢" if chg >= 0 else "🔴"
                    if label == "Crude":
                        cues[label] = f"{icon} ${curr:.1f} ({chg:+.1f}%)"
                    elif label == "USD/INR":
                        cues[label] = f"{icon} ₹{curr:.2f} ({chg:+.2f}%)"
                    elif label == "Gold":
                        cues[label] = f"{icon} ${curr:.0f} ({chg:+.1f}%)"
                    else:
                        cues[label] = f"{icon} {curr:,.0f} ({chg:+.1f}%)"
            except Exception:
                cues[label] = "—"
    except ImportError:
        pass  # yfinance not installed — skip silently
    return cues


def _fetch_fii_dii() -> dict:
    """Fetch FII/DII provisional data from NSE."""
    result = {"FII": None, "DII": None, "date": ""}
    try:
        url = "https://www.nseindia.com/api/fiidiiTradeReact"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer":    "https://www.nseindia.com/",
            "Accept":     "application/json",
        }
        session = requests.Session()
        session.headers.update(headers)
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            # NSE returns list — most recent entry first
            if isinstance(data, list) and len(data) > 0:
                latest = data[0]
                # FII net = buyValue - sellValue
                fii_buy  = float(latest.get("fiiBuy",  0) or 0)
                fii_sell = float(latest.get("fiiSell", 0) or 0)
                dii_buy  = float(latest.get("diiBuy",  0) or 0)
                dii_sell = float(latest.get("diiSell", 0) or 0)
                fii_net  = fii_buy - fii_sell
                dii_net  = dii_buy - dii_sell
                result["FII"]  = fii_net
                result["DII"]  = dii_net
                result["date"] = str(latest.get("date", ""))[:10]
    except Exception:
        pass
    return result


def _fetch_fo_ban() -> list:
    """Fetch F&O ban list from NSE."""
    try:
        from datetime import date
        today = date.today().strftime("%d-%b-%Y").upper()
        url = f"https://nsearchives.nseindia.com/content/fo/fo_secban.csv"
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.nseindia.com/"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            symbols = []
            for line in r.text.strip().splitlines():
                sym = line.strip().strip(",")
                if sym and sym != "Symbol":
                    symbols.append(sym)
            return symbols[:10]  # cap at 10
    except Exception:
        pass
    return []


def _fetch_results_today() -> list:
    """Fetch companies announcing results today from BSE."""
    results = []
    try:
        from datetime import date
        today = date.today().strftime("%Y%m%d")
        url = (
            "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
            f"?strCat=Result&strPrevDate={today}&strScrip=&strSearch="
            f"&strToDate={today}&strType=C&subcategory=-1"
        )
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer":    "https://www.bseindia.com/",
            "Accept":     "application/json",
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            anns = data.get("Table", []) or data.get("data", []) or []
            seen_names = set()
            for ann in anns[:20]:
                name = str(ann.get("LONG_NAME", ann.get("long_name",
                           ann.get("scrip_name", ""))))[:25].strip()
                if name and name not in seen_names:
                    results.append(name)
                    seen_names.add(name)
            return results[:8]
    except Exception:
        pass
    return []


def _fetch_nifty_level() -> str:
    """Fetch Nifty 50 spot from NSE."""
    try:
        url = "https://www.nseindia.com/api/allIndices"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer":    "https://www.nseindia.com/",
            "Accept":     "application/json",
        }
        session = requests.Session()
        session.headers.update(headers)
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(0.5)
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json().get("data", [])
            for idx in data:
                if idx.get("index") == "NIFTY 50":
                    last  = float(idx.get("last", 0))
                    chg   = float(idx.get("percentChange", 0))
                    icon  = "🟢" if chg >= 0 else "🔴"
                    return f"{icon} {last:,.0f} ({chg:+.2f}%)"
    except Exception:
        pass
    return "—"


def build_morning_brief() -> str:
    today_str = datetime.now().strftime("%A, %d %b %Y")
    lines = [
        f"🌅 <b>MULTIBAGGER MORNING BRIEF</b>",
        f"<b>{today_str}  |  8:30 AM IST</b>",
        f"{'═'*34}",
    ]

    # ── 1. Global cues ────────────────────────────────────────
    try:
        cues = _fetch_global_cues()
        if cues:
            lines.append("")
            lines.append("<b>🌍 GLOBAL CUES</b>")
            row1 = []
            row2 = []
            for i, (label, val) in enumerate(cues.items()):
                if i < 3:
                    row1.append(f"{label}: {val}")
                else:
                    row2.append(f"{label}: {val}")
            if row1: lines.append("  " + "  |  ".join(row1))
            if row2: lines.append("  " + "  |  ".join(row2))
    except Exception:
        pass

    # ── 2. Nifty level ────────────────────────────────────────
    try:
        nifty = _fetch_nifty_level()
        if nifty != "—":
            lines.append(f"  Nifty 50: {nifty}")
    except Exception:
        pass

    # ── 3. Market state (from engine CSV) ─────────────────────
    if MARKET_INTEL_CSV.exists():
        try:
            mi   = pd.read_csv(MARKET_INTEL_CSV).iloc[0]
            ms   = str(mi.get("MARKET_STATE","?"))
            fg   = float(mi.get("FEAR_GREED_SCORE", 0) or 0)
            fgl  = str(mi.get("FEAR_GREED_LABEL","?"))
            b200 = float(mi.get("BREADTH_ABOVE_200DMA", 0) or 0)
            b50  = float(mi.get("BREADTH_ABOVE_50DMA",  0) or 0)
            mr1m = float(mi.get("MEDIAN_RETURN_1M", 0) or 0)
            ms_emoji = {"BULL":"📈","BEAR":"📉","NEUTRAL":"➡️","CAUTION":"⚠️"}.get(ms,"📊")
            fg_emoji = ("😱" if fg<25 else "😨" if fg<40 else "😐" if fg<60
                        else "😊" if fg<75 else "🤑")
            lines += [
                f"",
                f"<b>📊 NSE MARKET: {ms_emoji} {ms}</b>",
                f"  F&amp;G: {fg_emoji} {fgl} ({fg:.0f}/100)",
                f"  Above 200DMA: {b200:.0f}%  |  Above 50DMA: {b50:.0f}%",
                f"  Median 1M: {mr1m:+.1f}%",
            ]
        except Exception as e:
            lines.append(f"<i>[Market data: {e}]</i>")

    # ── 4. FII / DII flow ─────────────────────────────────────
    try:
        fii_dii = _fetch_fii_dii()
        fii = fii_dii.get("FII")
        dii = fii_dii.get("DII")
        dt  = fii_dii.get("date","")
        if fii is not None and dii is not None:
            fii_icon = "🟢" if fii >= 0 else "🔴"
            dii_icon = "🟢" if dii >= 0 else "🔴"
            lines += [
                f"",
                f"<b>💰 FII/DII FLOW{' (' + dt + ')' if dt else ''}</b>",
                f"  FII: {fii_icon} ₹{abs(fii):,.0f} Cr ({'NET BUY' if fii>=0 else 'NET SELL'})",
                f"  DII: {dii_icon} ₹{abs(dii):,.0f} Cr ({'NET BUY' if dii>=0 else 'NET SELL'})",
            ]
    except Exception:
        pass

    # ── 5. F&O ban list ───────────────────────────────────────
    try:
        ban = _fetch_fo_ban()
        lines.append("")
        if ban:
            lines.append(f"<b>🚫 F&amp;O BAN ({len(ban)} stocks)</b>")
            lines.append(f"  {', '.join(ban)}")
        else:
            lines.append("<b>🚫 F&amp;O BAN:</b> None today ✅")
    except Exception:
        pass

    # ── 6. Results today ──────────────────────────────────────
    try:
        results = _fetch_results_today()
        lines.append("")
        if results:
            lines.append(f"<b>📅 RESULTS TODAY ({len(results)})</b>")
            lines.append(f"  {', '.join(results)}")
        else:
            lines.append("<b>📅 RESULTS TODAY:</b> None scheduled")
    except Exception:
        pass

    # ── 7. Engine tier counts ─────────────────────────────────
    if COMPOSITE_CSV.exists():
        try:
            cs    = pd.read_csv(COMPOSITE_CSV, low_memory=False)
            tc    = cs["TIER"].value_counts()
            prime  = int(tc.get("PRIME", 0))
            strong = int(tc.get("STRONG", 0))
            wlc    = int(tc.get("WATCHLIST_CONFIRMED", 0))
            wle    = int(tc.get("WATCHLIST_EXTERNAL", 0))
            lines += [
                f"",
                f"<b>🏆 ENGINE UNIVERSE</b>",
                f"  ⭐ PRIME: {prime}  |  💪 STRONG: {strong}",
                f"  👀 WL-Confirmed: {wlc}  |  📋 WL-External: {wle}",
            ]

            # Actionable picks with entry/stop
            action_csv = Path("action_language.csv")
            comp_csv   = COMPOSITE_CSV
            entry_map  = {}
            if comp_csv.exists():
                cdf = pd.read_csv(comp_csv, low_memory=False)
                for _, r in cdf.iterrows():
                    sym = str(r.get("NSE_SYMBOL",""))
                    entry_map[sym] = {
                        "entry": str(r.get("ENTRY_ZONE","—")),
                        "sl":    str(r.get("STOP_LOSS","—")),
                        "rr":    str(r.get("RISK_REWARD","—")),
                        "er":    str(r.get("EXPECTED_RETURN","—")),
                    }

            buys = []
            if action_csv.exists():
                al = pd.read_csv(action_csv, low_memory=False)
                if "AI_ACTION" in al.columns:
                    buy_rows = al[al["AI_ACTION"].str.contains(
                        "ACCUMULATE|BUY", na=False, case=False)].copy()
                    buy_rows = buy_rows.sort_values("COMPOSITE_SCORE", ascending=False)
                    for _, r in buy_rows.head(5).iterrows():
                        sym  = str(r.get("NSE_SYMBOL",""))
                        tier = str(r.get("MULTIBAGGER_TIER",""))
                        sc   = int(r.get("COMPOSITE_SCORE",0) or 0)
                        sp   = str(r.get("SECTOR_PHASE","?"))
                        sp_icon = {"BASING":"⏸️","MID_CYCLE":"🚀","LATE_CYCLE":"⚠️",
                                   "TOPPING":"🔴","CORRECTION":"📉",
                                   "EARLY_RECOVERY":"🌱"}.get(sp,"🔵")
                        act_icon = "🟢" if "BUY" in str(r.get("AI_ACTION","")) else "🔵"
                        em = entry_map.get(sym, {})
                        entry = em.get("entry","—")
                        sl    = em.get("sl","—")
                        rr    = em.get("rr","—")
                        er    = em.get("er","—")
                        buys.append(
                            f"  {act_icon} <b>{sym}</b> | {tier} | {sc}/100 | {sp_icon}{sp}\n"
                            f"     Entry:{entry}  SL:{sl}  R:R:{rr}  Exp:{er}"
                        )

            if buys:
                lines += ["", "<b>💡 ACTIONABLE PICKS</b>"] + buys

        except Exception as e:
            lines.append(f"<i>[Scores: {e}]</i>")

    # ── 8. Sector pulse ───────────────────────────────────────
    if SECTOR_CYCLE_CSV.exists():
        try:
            sc = pd.read_csv(SECTOR_CYCLE_CSV, low_memory=False)
            if not sc.empty:
                lines.append("")
                lines.append("<b>🗺️ SECTOR PULSE</b>")
                mid   = sc[sc["CYCLE_PHASE"]=="MID_CYCLE"]
                early = sc[sc["CYCLE_PHASE"]=="EARLY_RECOVERY"]
                top   = sc[sc["CYCLE_PHASE"]=="TOPPING"]
                corr  = sc[sc["CYCLE_PHASE"]=="CORRECTION"]
                if not mid.empty:
                    best = mid.sort_values("SECTOR_RS", ascending=False).iloc[0]
                    lines.append(f"  🚀 BEST: {best['INDUSTRY_GROUP']} "
                                 f"(RS {best['SECTOR_RS']:.1f}, 6M:{best['RETURN_6M_PCT']:+.1f}%)")
                if not early.empty:
                    e2 = early.sort_values("SECTOR_RS", ascending=False).iloc[0]
                    lines.append(f"  🌱 TURNING: {e2['INDUSTRY_GROUP']}")
                if not top.empty:
                    worst = top.sort_values("MOMENTUM_ACCELERATION").iloc[0]
                    lines.append(f"  🔴 AVOID: {worst['INDUSTRY_GROUP']} "
                                 f"({worst['MOMENTUM_ACCELERATION']:+.1f}%/mo)")
                lines.append(f"  📉 In CORRECTION: {len(corr)} sectors")
        except Exception:
            pass

    # ── 9. Early alerts summary ───────────────────────────────
    wl_path = Path("watchlist_for_cloud.csv")
    # We use early_alerts if bundled, else skip
    try:
        ea_path = Path("early_alerts.csv")
        if ea_path.exists():
            ea = pd.read_csv(ea_path)
            very_high = ea[ea["SEVERITY"]=="VERY_HIGH"]
            if not very_high.empty:
                lines.append("")
                lines.append(f"<b>⚡ INSIDER ALERTS ({len(very_high)} filings)</b>")
                for _, r in very_high.head(3).iterrows():
                    sym  = str(r.get("NSE_SYMBOL",""))
                    det  = str(r.get("ALERT_DETAIL",""))[:60]
                    lines.append(f"  🔵 <b>{sym}</b>: {det}")
    except Exception:
        pass

    # ── Footer ────────────────────────────────────────────────
    lines += [
        f"",
        f"{'─'*32}",
        f"<i>Multibagger Engine v2.2  |  {datetime.now().strftime('%H:%M UTC')}</i>",
    ]
    return "\n".join(lines)
# ── Intraday scan ─────────────────────────────────────────────

def intraday_scan():
    print("=" * 60)
    print(f"  INTRADAY SCAN  {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    _check_telegram_config()

    watchlist = load_watchlist()
    if not watchlist:
        print("  Empty watchlist — nothing to do")
        return

    seen     = load_seen()
    sent     = 0
    skipped  = 0

    # Symbol → ISIN fallback map
    sym_map = {ctx["symbol"].upper(): isin for isin, ctx in watchlist.items()}

    # ── BSE announcements ─────────────────────────────────────
    bse_anns = fetch_bse_announcements(BSE_LOOKBACK_HRS)
    for ann in bse_anns:
        isin = str(ann.get("ISIN", ann.get("isin",""))).strip()
        if not isin or isin in ("nan",""):
            sym  = str(ann.get("SCRIP_CD", ann.get("scripcd",""))).upper()
            isin = sym_map.get(sym, "")
        if isin not in watchlist:
            continue

        headline = str(ann.get("HEADLINE", ann.get("headline",
                       ann.get("SubjectLong", ann.get("subject","")))))
        etype    = classify(headline)
        if etype == "NEWS":
            skipped += 1
            continue

        dedup = f"BSE|{isin}|{etype}|{datetime.now().strftime('%Y-%m-%d')}"
        if dedup in seen:
            skipped += 1
            continue

        ctx = watchlist[isin]
        ann["headline"] = headline
        msg = format_stock_alert(ann, ctx, etype, "BSE")
        print(f"\n  [BSE] ALERT: {ctx['symbol']} | {etype}")

        if send_telegram(msg):
            seen[dedup] = datetime.now().strftime("%Y-%m-%d")
            sent += 1
            time.sleep(1)

    # ── NSE announcements ─────────────────────────────────────
    nse_anns = fetch_nse_announcements(BSE_LOOKBACK_HRS)
    for ann in nse_anns:
        isin = ann.get("ISIN","").strip()
        if not isin:
            sym  = ann.get("symbol","").upper()
            isin = sym_map.get(sym, "")
        if isin not in watchlist:
            continue

        etype = classify(ann.get("headline",""))
        if etype == "NEWS":
            skipped += 1
            continue

        dedup = f"NSE|{isin}|{etype}|{datetime.now().strftime('%Y-%m-%d')}"
        if dedup in seen:
            skipped += 1
            continue

        ctx = watchlist[isin]
        msg = format_stock_alert(ann, ctx, etype, "NSE")
        print(f"\n  [NSE] ALERT: {ctx['symbol']} | {etype}")

        if send_telegram(msg):
            seen[dedup] = datetime.now().strftime("%Y-%m-%d")
            sent += 1
            time.sleep(1)

    # ── Macro RSS sector alerts ───────────────────────────────
    rss_headlines = fetch_rss_headlines()
    sector_hits   = match_rss_to_sectors(rss_headlines)

    for sector, headlines in sector_hits.items():
        result = build_sector_macro_alert(sector, headlines, watchlist, seen)
        if result is None:
            skipped += 1
            continue
        msg, dedup = result
        print(f"\n  [RSS] MACRO ALERT: {sector} ({len(headlines)} headlines)")

        if send_telegram(msg):
            seen[dedup] = datetime.now().strftime("%Y-%m-%d")
            sent += 1
            time.sleep(1)

    save_seen(seen)
    print(f"\n  Sent: {sent}  |  Suppressed/duplicate: {skipped}")
    print("Done")


# ── Morning brief runner ──────────────────────────────────────

def run_morning_brief():
    print("=" * 60)
    print(f"  MORNING BRIEF  {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    _check_telegram_config()

    seen  = load_seen()
    today = datetime.now().strftime("%Y-%m-%d")
    dedup = f"MORNING_BRIEF|{today}"

    if dedup in seen:
        print("  Morning brief already sent today — skipping")
        return

    msg = build_morning_brief()
    print(f"  Brief length: {len(msg)} chars")
    print(f"  Preview:\n{msg[:300]}...")

    if send_telegram(msg):
        seen[dedup] = today
        save_seen(seen)
        print("  Morning brief sent to Telegram ✅")
    else:
        print("  Morning brief FAILED — Telegram not configured or API error ❌")


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    if MODE == "morning_brief":
        run_morning_brief()
    else:
        intraday_scan()
