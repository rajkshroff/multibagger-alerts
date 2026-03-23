#!/usr/bin/env python3
"""
build_corporate_announcements.py — v1.0
CORPORATE ANNOUNCEMENTS LIVE POLLER

Fetches real-time corporate announcements from NSE + BSE APIs,
filters for our universe stocks, sends Telegram alerts immediately.

TRIGGER:
  - GitHub Actions: every 15 minutes, 9am-6pm IST weekdays
  - Add to .github/workflows/alerts.yml as new job: corp_announcements

OUTPUT:
  - Telegram alert: fires immediately on new announcement
  - corp_announcements_seen.json: dedup log

ANNOUNCEMENT TYPES PRIORITISED:
  CRITICAL (alert immediately):
    - Board Meeting, Dividend, Bonus, Split, Buyback
    - Results (quarterly earnings), Annual Report
    - Change in Directors/KMP
    - SEBI/Exchange Order, Suspension, Penalty
    - Acquisition, Merger, Demerger
    - Trading Window / Insider Trading
    - Change in Promoter Holding

  HIGH (alert if our universe):
    - Press Release, Intimation
    - Change in DII/FII holding
    - Analyst/Investor Meet
    - Award of Contract / Order

LOCATION: Multibagger_App/multibagger-alerts/build_corporate_announcements.py
"""

import os, sys, json, hashlib, re, time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

# ── TELEGRAM ─────────────────────────────────────────────────
BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN","")
             or os.environ.get("TELEGRAM_TOKEN",""))
_raw_ids  = os.environ.get("TELEGRAM_CHAT_IDS","") or os.environ.get("TELEGRAM_CHAT_ID","")
CHAT_IDS  = [x.strip() for x in _raw_ids.split(",") if x.strip()]

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)

REPO      = Path(__file__).resolve().parent
SEEN_FILE = REPO / "corp_announcements_seen.json"

# Watchlist CSV — shares same path as cloud_alert_engine
CSV_PATHS = {
    "composite": REPO / "composite_scores.csv",
    "watchlist":  REPO / "watchlist_for_cloud.csv",
}

MAX = 4000

# ── ANNOUNCEMENT PRIORITY ────────────────────────────────────
# NSE uses these category labels in their API
# Routine filings — these are filed by EVERY company every quarter, pure noise
ROUTINE_SUBJECTS = [
    # Single broad match catches ALL trading window variations
    "trading window",       # "closure of trading window", "trading window for dealing", etc.
    "new listing",
    "listing of securities",
    "listing of equity",
    "intimation of closure",
]

# Block entire category unless subject has genuinely material keywords
NOISY_CATEGORIES = {"Insider Trading / SAST", "Insider Trading"}
MATERIAL_OVERRIDE_KEYWORDS = [
    "pledge invoked", "acquisition", "bulk deal", "block deal",
    "merger", "demerger", "open offer", "takeover",
]

CRITICAL_CATEGORIES = {
    # Earnings / financials
    "Financial Results",
    "Board Meeting",
    "Dividend",
    "Bonus",
    "Split",
    "Buyback",
    "Annual Report",
    "AGM",
    "EGM",
    # Corporate structure
    "Acquisition",
    "Merger",
    "Demerger",
    "Amalgamation",
    "Change in Management",
    "Appointment",
    "Resignation",
    "Change in Directors",
    "Key Managerial Personnel",
    # Regulatory
    "SEBI",
    "Exchange",
    "Penalty",
    "Suspension",
    "Trading Window",
    "Insider Trading",
    "Pledging",
    # Orders & Contracts
    "Order Received",
    "New Order",
    "Contract",
    "LOA",
    "Award",
    # Capital structure
    "Rights Issue",
    "FPO",
    "NCD",
    "QIP",
    "Preferential Allotment",
    "Debt",
    "Credit Rating",
    "Default",
}

HIGH_CATEGORIES = {
    "Press Release",
    "Intimation",
    "Analyst Meet",
    "Investor Meet",
    "Conference Call",
    "Outcome of Board Meeting",
    "Change in Shareholding",
    "Promoter Holding",
    "Insider Trading - UPSI",
}

# ── TELEGRAM SENDER ──────────────────────────────────────────
def send_tg(text: str) -> bool:
    if not text.strip() or not BOT_TOKEN or not CHAT_IDS:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+MAX] for i in range(0, len(text), MAX)]
    ok = True
    for chat in CHAT_IDS:
        for chunk in chunks:
            try:
                r = requests.post(url, json={
                    "chat_id": chat, "text": chunk, "parse_mode": "HTML"
                }, timeout=15)
                if not r.ok:
                    print(f"  [tg→{chat}] {r.status_code}: {r.text[:100]}")
                    ok = False
            except Exception as e:
                print(f"  [tg→{chat}] Error: {e}"); ok = False
    return ok

# ── DEDUP ────────────────────────────────────────────────────
def load_seen() -> dict:
    if not SEEN_FILE.exists(): return {}
    try: return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
    except: return {}

def save_seen(seen: dict):
    # Prune items older than 7 days
    cutoff = (now_ist() - timedelta(days=7)).isoformat()
    pruned = {k: v for k, v in seen.items() if v > cutoff}
    SEEN_FILE.write_text(json.dumps(pruned, indent=2, ensure_ascii=False), encoding="utf-8")

def ann_hash(symbol: str, subject: str, date: str) -> str:
    raw = f"{symbol}|{subject[:60]}|{date}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]

# ── UNIVERSE LOADER ──────────────────────────────────────────
def load_universe() -> set:
    """Load all tracked symbols — PRIME to MONITOR + Nifty50."""
    syms = set()
    try:
        import csv as _csv
        for key, p in CSV_PATHS.items():
            if not p.exists(): continue
            with open(p, newline="", encoding="utf-8", errors="replace") as f:
                for row in _csv.DictReader(f):
                    # composite_scores has TIER column
                    tier = row.get("TIER","") or row.get("MULTIBAGGER_TIER","")
                    sym  = (row.get("NSE_SYMBOL","") or row.get("SYMBOL","")).strip().upper()
                    if not sym: continue
                    if tier in ("PRIME","STRONG","WATCHLIST_CONFIRMED","WL_CONFIRMED",
                                "WATCHLIST_EXTERNAL","WL_EXTERNAL","MONITOR"):
                        syms.add(sym)
                    elif key == "watchlist":
                        syms.add(sym)
    except Exception as e:
        print(f"  [universe] {e}")
    # Always include Nifty50
    NIFTY50 = {
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
        "BAJFINANCE","BHARTIARTL","KOTAKBANK","LT","HCLTECH","AXISBANK","ASIANPAINT",
        "MARUTI","TITAN","NESTLEIND","ULTRACEMCO","WIPRO","POWERGRID","NTPC",
        "TATAMOTORS","TATAPOWER","ADANIENT","ADANIPORTS","SUNPHARMA","DRREDDY",
        "CIPLA","DIVISLAB","ONGC","COALINDIA","BPCL","TECHM","INDUSINDBK",
        "BAJAJFINSV","GRASIM","HINDALCO","JSWSTEEL","TATASTEEL",
    }
    syms.update(NIFTY50)
    print(f"  [universe] {len(syms)} symbols tracked")
    return syms

# ── NSE CORPORATE ANNOUNCEMENTS API ──────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    # Seed cookies
    try:
        s.get("https://www.nseindia.com", timeout=8)
    except Exception:
        pass
    return s

def fetch_nse_announcements(session) -> list:
    """Fetch NSE corporate announcements — last 50 entries."""
    try:
        url = "https://www.nseindia.com/api/corp-announcements?index=equities"
        r = session.get(url, timeout=12)
        if not r.ok:
            print(f"  [nse] HTTP {r.status_code}")
            return []
        data = r.json()
        # NSE returns a list of dicts
        if isinstance(data, list):
            return data[:50]
        if isinstance(data, dict) and "data" in data:
            return data["data"][:50]
        return []
    except Exception as e:
        print(f"  [nse] {e}")
        return []

def parse_nse_announcement(item: dict) -> dict:
    """Normalise NSE announcement dict."""
    sym      = str(item.get("symbol","") or item.get("sm_isin","")).strip().upper()
    subject  = str(item.get("subject","") or item.get("desc","") or "").strip()
    category = str(item.get("sm_name","") or item.get("attchmntType","") or "").strip()
    dt_str   = str(item.get("an_dt","") or item.get("exchdisstime","") or "").strip()
    url      = str(item.get("attchmntFile","") or "").strip()
    if url and not url.startswith("http"):
        url = "https://nsearchives.nseindia.com" + url
    return {"symbol": sym, "subject": subject, "category": category,
            "date": dt_str, "url": url, "exchange": "NSE"}

# ── BSE CORPORATE ANNOUNCEMENTS API ──────────────────────────
def fetch_bse_announcements() -> list:
    """Fetch BSE corporate announcements — latest 50."""
    try:
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
               "?strCat=-1&strPrevDate=&strScrip=&strSearch=P&strToDate=&strType=C")
        r = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bseindia.com/",
        }, timeout=12)
        if not r.ok:
            print(f"  [bse] HTTP {r.status_code}")
            return []
        data = r.json()
        items = data if isinstance(data, list) else data.get("Table","")
        return items[:50] if items else []
    except Exception as e:
        print(f"  [bse] {e}")
        return []

# BSE scrip code → company name cache (built during run)
_BSE_NAME_CACHE = {}

def _bse_company_name(scrip_cd: str) -> str:
    """Lookup BSE company name from scrip code. Cached per run."""
    if not scrip_cd: return ""
    if scrip_cd in _BSE_NAME_CACHE: return _BSE_NAME_CACHE[scrip_cd]
    try:
        url = (f"https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w"
               f"?Scrip_cd={scrip_cd}&seriesid=")
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0",
                                        "Referer":"https://www.bseindia.com/"},
                         timeout=5)
        if r.ok:
            d = r.json()
            name = (d.get("CompanyName","") or d.get("companyname","") or
                    d.get("SHORT_NAME","")  or d.get("LONG_NAME","") or "").strip()
            if name:
                _BSE_NAME_CACHE[scrip_cd] = name
                return name
    except Exception:
        pass
    _BSE_NAME_CACHE[scrip_cd] = ""  # cache miss to avoid retrying
    return ""

def parse_bse_announcement(item: dict) -> dict:
    scrip_cd = str(item.get("SCRIP_CD","") or item.get("scrip_cd","")).strip()
    subject  = str(item.get("HEADLINE","") or item.get("headline","") or "").strip()
    category = str(item.get("CATEGORYNAME","") or item.get("SUBCATNAME","") or "").strip()
    dt_str   = str(item.get("NEWS_DT","") or item.get("news_dt","") or "").strip()
    # BSE announcement payload field names (checked from actual API response)
    # BSE uses camelCase and UPPERCASE inconsistently across endpoints
    company = (str(item.get("short_name","")   or  # BSE AnnSubCategoryGetData
                   item.get("SHORT_NAME","")   or
                   item.get("COMPANYNAME","")  or
                   item.get("CompanyName","")  or
                   item.get("company_name","") or
                   item.get("FULL_NAME","")    or
                   item.get("FullName","")     or "").strip()
               .replace(" Ltd", " Ltd").replace("  ", " "))   # normalize spacing
    # Fallback: dedicated BSE company lookup API (adds ~1 HTTP call per new scrip)
    if not company and scrip_cd:
        company = _bse_company_name(scrip_cd)
    return {"symbol": scrip_cd, "subject": subject, "category": category,
            "date": dt_str, "url": "", "exchange": "BSE", "company": company}

# ── PRIORITY CLASSIFIER ───────────────────────────────────────
def classify_priority(ann: dict) -> str:
    """Returns 'CRITICAL', 'HIGH', or 'LOW'."""
    cat  = ann.get("category","").strip()
    subj = ann.get("subject","").lower()
    # Routine filings — always LOW regardless of category
    # Filed by every company every quarter — pure noise
    for r in ROUTINE_SUBJECTS:
        if r in subj:
            return "LOW"
    # Block noisy categories unless subject has material keywords
    if cat in NOISY_CATEGORIES:
        if not any(kw in subj for kw in MATERIAL_OVERRIDE_KEYWORDS):
            return "LOW"
    # Exact category match
    for c in CRITICAL_CATEGORIES:
        if c.lower() in cat.lower() or c.lower() in subj:
            return "CRITICAL"
    for c in HIGH_CATEGORIES:
        if c.lower() in cat.lower():
            return "HIGH"
    # Keyword scan on subject
    critical_kw = [
        "results","dividend","bonus","split","buyback","acquisition","merger",
        "demerger","penalty","sebi order","new order","contract awarded",
        "default","credit rating","downgrade","upgrade","resignation","appointed",
        "pledge invoked","rights issue","qip","ncd","delisting",
        # Removed: "trading window" — routine quarterly filing, always noise
        # Removed: "order received" alone — too broad, matches admin notices
    ]
    for kw in critical_kw:
        if kw in subj:
            return "CRITICAL"
    return "LOW"

# ── MESSAGE BUILDER ──────────────────────────────────────────
PRIORITY_ICONS = {"CRITICAL": "🚨", "HIGH": "📢", "LOW": "📎"}

def build_alert(announcements: list) -> str:
    if not announcements:
        return ""
    now_str = now_ist().strftime("%H:%M IST, %d %b")
    lines = [f"📋 <b>CORPORATE ANNOUNCEMENTS — {now_str}</b>", ""]

    # Group by priority
    critical = [a for a in announcements if a["priority"] == "CRITICAL"]
    high     = [a for a in announcements if a["priority"] == "HIGH"]

    shown = (critical + high)[:15]  # cap at 15 per cycle

    for ann in shown:
        icon     = PRIORITY_ICONS.get(ann["priority"], "📎")
        sym      = ann["symbol"]
        # Show company name if available, else fall back to symbol/code
        company  = ann.get("company","").strip()
        disp_sym = company if company else sym
        subj     = ann["subject"][:100]
        cat      = ann["category"]
        exch     = ann["exchange"]
        import html as _html
        subj_esc    = _html.escape(subj)
        cat_esc     = _html.escape(cat)
        disp_esc    = _html.escape(disp_sym)
        # Show BSE code in brackets if we have the company name
        code_str = f" <i>({sym})</i>" if company and sym != company else ""
        lines.append(
            f"{icon} <b>{disp_esc}</b>{code_str} — {cat_esc}\n"
            f"  {subj_esc}\n"
            f"  <i>— {exch}</i>\n"
        )

    total = len(critical) + len(high)
    lines.append(f"<i>{len(critical)} critical · {len(high)} high-priority · {total} total new</i>")
    return "\n".join(lines)

# ── MAIN ─────────────────────────────────────────────────────
def main():
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] Corporate Announcements v1.0")

    if not BOT_TOKEN or not CHAT_IDS:
        print("⚠ Telegram not configured — alerts will print only")

    universe = load_universe()
    seen     = load_seen()
    new_anns = []

    # Fetch NSE
    print("  [nse] Fetching announcements...")
    nse_sess = _nse_session()
    nse_raw  = fetch_nse_announcements(nse_sess)
    print(f"  [nse] {len(nse_raw)} items")
    for item in nse_raw:
        ann = parse_nse_announcement(item)
        sym = ann["symbol"]
        if not sym: continue
        # NSE symbol must be in universe
        if sym not in universe: continue
        h = ann_hash(sym, ann["subject"], ann["date"])
        if h in seen: continue
        ann["priority"] = classify_priority(ann)
        if ann["priority"] == "LOW": continue
        new_anns.append(ann)
        seen[h] = now_ist().isoformat()

    # Fetch BSE
    print("  [bse] Fetching announcements...")
    bse_raw = fetch_bse_announcements()
    print(f"  [bse] {len(bse_raw)} items")
    for item in bse_raw:
        ann = parse_bse_announcement(item)
        sym = ann["symbol"]
        if not sym: continue
        h = ann_hash(sym, ann["subject"], ann["date"])
        if h in seen: continue
        ann["priority"] = classify_priority(ann)
        if ann["priority"] == "LOW": continue
        new_anns.append(ann)
        seen[h] = now_ist().isoformat()

    print(f"\n  New announcements: {len(new_anns)}")
    if not new_anns:
        print("  Nothing new — exiting")
        save_seen(seen)
        return

    # Sort: CRITICAL first
    new_anns.sort(key=lambda x: 0 if x["priority"] == "CRITICAL" else 1)

    # Print to console always
    for ann in new_anns:
        print(f"  [{ann['priority']}] {ann['symbol']}: {ann['subject'][:80]}")

    # Send Telegram
    msg = build_alert(new_anns)
    if msg:
        ok = send_tg(msg)
        print(f"\n  Telegram: {'✅ sent' if ok else '⚠ failed'}")
    else:
        print("  No HIGH/CRITICAL items — no alert sent")

    save_seen(seen)
    print("\n  Done.")

if __name__ == "__main__":
    main()
