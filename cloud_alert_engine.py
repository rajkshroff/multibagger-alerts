# =============================================================
# cloud_alert_engine.py  —  24x7 Cloud Alert Engine
# SESSION 36
#
# Runs on GitHub Actions every 30 min (9am-5pm IST, Mon-Fri)
# Free tier: 2000 min/month GitHub Actions + 1000 msg/month WhatsApp
#
# Required GitHub Secrets (Settings → Secrets → Actions):
#   WA_TOKEN          — Meta WhatsApp Cloud API token
#   WA_PHONE_ID       — Your WhatsApp Business phone number ID
#   WA_RECIPIENT      — Your personal WhatsApp number (e.g. 919876543210)
#   GITHUB_TOKEN      — Auto-provided by GitHub Actions
#
# Setup (one-time, ~15 min):
#   1. Create private GitHub repo (e.g. multibagger-alerts)
#   2. Upload watchlist_for_cloud.csv from ENGINE_CORE/data_internal/
#      (engine auto-updates this after each run)
#   3. Get WhatsApp Cloud API: developers.facebook.com → My Apps → Create
#      → Business → WhatsApp → Add phone number → get token + phone_id
#   4. Add 4 secrets to GitHub repo
#   5. Push this file + market_alerts.yml to repo → runs automatically
#
# What it does each run:
#   1. Loads watchlist_for_cloud.csv (PRIME/STRONG/WLC stocks)
#   2. Fetches BSE corporate announcements (last 2h)
#   3. Classifies each announcement (ORDER_WIN / RESULT / CAPEX / PENALTY etc)
#   4. Cross-references with watchlist
#   5. Deduplicates against seen_alerts.json (stored in repo)
#   6. Sends WhatsApp for new alerts
#   7. Commits updated seen_alerts.json back to repo
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
WA_TOKEN     = os.environ.get("WA_TOKEN", "")
WA_PHONE_ID  = os.environ.get("WA_PHONE_ID", "")
WA_RECIPIENT = os.environ.get("WA_RECIPIENT", "")   # format: 919876543210
SEEN_FILE    = Path("seen_alerts.json")
WATCHLIST    = Path("watchlist_for_cloud.csv")
BSE_LOOKBACK_HOURS = 2   # fetch announcements from last 2 hours

# ── BSE Corporate Announcement categories to monitor ──────────
BSE_CATEGORIES = {
    "Result":           "QUARTERLY_RESULT",
    "Board Meeting":    "BOARD_MEETING",
    "Dividend":         "DIVIDEND",
    "Buyback":          "BUYBACK",
    "Rights":           "RIGHTS_ISSUE",
    "Acquisition":      "ACQUISITION",
    "Merger":           "MERGER",
}

# ── Event classification rules ────────────────────────────────
CLASSIFY_RULES = [
    ("ORDER_WIN",      r"order|contract|work order|lo[ia]|letter of (intent|award)|awarded|bagged|secured"),
    ("RESULT_BEAT",    r"(profit|revenue|net income).{0,20}(jump|surge|rise|grew|up).{0,10}\d+\s*%|beat.{0,20}estimate"),
    ("CAPEX",          r"capex|capital expenditure|new plant|capacity expansion|greenfield|brownfield"),
    ("FUNDRAISE",      r"qip|rights issue|preferential allotment|fpo|ncd|debenture"),
    ("DIVIDEND",       r"dividend|interim dividend|special dividend"),
    ("BUYBACK",        r"buyback|buy.?back|share repurchase"),
    ("ACQUISITION",    r"acqui|takeover|merger|amalgamation|demerger"),
    ("MGMT_CHANGE",    r"appoint|new (ceo|md|cfo|director)|resign|step.?down|vacates"),
    ("PENALTY",        r"penalty|fine|sebi|tax demand|gst demand|show cause|enforcement"),
    ("RATING_UP",      r"upgrade|target (raised|hiked|increased)|buy rating|overweight"),
    ("RATING_DOWN",    r"downgrade|target (cut|reduced|lowered)|sell rating|underweight|avoid"),
    ("RESULT",         r"quarterly result|financial result|unaudited|q[1-4] result"),
]

SEVERITY_MAP = {
    "ORDER_WIN":     "🟢 HIGH",
    "RESULT_BEAT":   "🟢 HIGH",
    "CAPEX":         "🔵 MEDIUM",
    "FUNDRAISE":     "🔵 MEDIUM",
    "ACQUISITION":   "🔵 MEDIUM",
    "BUYBACK":       "🔵 MEDIUM",
    "DIVIDEND":      "⚪ LOW",
    "MGMT_CHANGE":   "🟡 MEDIUM",
    "PENALTY":       "🔴 HIGH",
    "RATING_UP":     "🟢 MEDIUM",
    "RATING_DOWN":   "🔴 HIGH",
    "RESULT":        "⚪ LOW",
}

TIER_EMOJI = {
    "PRIME":                "⭐ PRIME",
    "STRONG":               "★★ STRONG",
    "WATCHLIST_CONFIRMED":  "👁 WL-CONFIRMED",
    "WATCHLIST_EXTERNAL":   "👁 WL-EXTERNAL",
}


def classify(text: str) -> str:
    t = text.lower()
    for etype, pattern in CLASSIFY_RULES:
        if re.search(pattern, t, re.IGNORECASE):
            return etype
    return "NEWS"


def fetch_bse_announcements(hours_back: int = 2) -> list:
    """Fetch BSE corporate announcements for last N hours."""
    now  = datetime.now(timezone.utc)
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
        print(f"  BSE announcements: {len(anns)} fetched (last {hours_back}h)")
        return anns
    except Exception as e:
        print(f"  [WARN] BSE fetch failed: {e}")
        return []


def load_watchlist() -> dict:
    """Returns {ISIN: {symbol, name, tier, signals}} for actionable stocks."""
    if not WATCHLIST.exists():
        print(f"  [WARN] {WATCHLIST} not found — upload from ENGINE_CORE/data_internal/")
        return {}
    df = pd.read_csv(WATCHLIST, low_memory=False)
    result = {}
    for _, r in df.iterrows():
        isin = str(r.get("ISIN","")).strip()
        if isin:
            result[isin] = {
                "symbol":  str(r.get("NSE_SYMBOL","")),
                "name":    str(r.get("NAME","")),
                "tier":    str(r.get("TIER","")),
                "signals": int(float(str(r.get("SIGNALS_FIRED", 0) or 0))),
                "score":   int(float(str(r.get("COMPOSITE_BALANCED", 0) or 0))),
                "sector_phase": str(r.get("SECTOR_PHASE","")),
            }
    print(f"  Watchlist loaded: {len(result)} actionable stocks")
    return result


def load_seen() -> set:
    if SEEN_FILE.exists():
        try:
            data = json.loads(SEEN_FILE.read_text())
            # Prune alerts older than 7 days
            cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            pruned = {k: v for k, v in data.items() if v >= cutoff}
            return pruned
        except Exception:
            return {}
    return {}


def save_seen(seen: dict):
    SEEN_FILE.write_text(json.dumps(seen, indent=2))


def send_whatsapp(message: str) -> bool:
    """Send a WhatsApp message via Meta Cloud API."""
    if not all([WA_TOKEN, WA_PHONE_ID, WA_RECIPIENT]):
        print(f"  [SKIP] WhatsApp not configured — message:\n{message}")
        return False
    url = f"https://graph.facebook.com/v18.0/{WA_PHONE_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to":   WA_RECIPIENT,
        "type": "text",
        "text": {"body": message, "preview_url": False},
    }
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type":  "application/json",
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        if r.status_code == 200:
            return True
        else:
            print(f"  [WA ERROR] {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        print(f"  [WA ERROR] {e}")
        return False


def format_alert(ann: dict, ctx: dict, etype: str) -> str:
    """Format WhatsApp message for one alert."""
    symbol  = ctx["symbol"]
    tier    = TIER_EMOJI.get(ctx["tier"], ctx["tier"])
    sigs    = ctx["signals"]
    score   = ctx["score"]
    phase   = ctx["sector_phase"]
    sev     = SEVERITY_MAP.get(etype, "⚪ INFO")
    headline= str(ann.get("HEADLINE", ann.get("headline",
                  ann.get("SubjectLong", ann.get("subject","")))))[:150]
    bse_code= str(ann.get("SCRIP_CD", ann.get("scripcd", "")))
    ann_time= str(ann.get("DT_TM", ann.get("dt_tm", "")))[:16]

    msg = (
        f"{'='*35}\n"
        f"📊 *{symbol}*  |  {tier}\n"
        f"🔔 *{etype}*  |  {sev}\n"
        f"{'='*35}\n"
        f"📋 {headline}\n"
        f"\n"
        f"Score: {score}/100  |  Signals: {sigs}/15\n"
        f"Sector: {phase}\n"
        f"BSE Code: {bse_code}  |  {ann_time}\n"
        f"\n"
        f"🔗 https://bseindia.com/corporates/ann.html?scripcd={bse_code}\n"
        f"{'─'*35}\n"
        f"Multibagger Engine Alert"
    )
    return msg


def main():
    print("=" * 60)
    print(f"  CLOUD ALERT ENGINE  —  {datetime.now().strftime('%Y-%m-%d %H:%M IST')}")
    print("=" * 60)

    watchlist = load_watchlist()
    if not watchlist:
        print("  No watchlist — nothing to alert on"); return

    seen = load_seen()
    announcements = fetch_bse_announcements(BSE_LOOKBACK_HOURS)

    # Build ISIN lookup from BSE announcement ISIN field
    # BSE uses ISIN in field "ISIN" or maps via scrip code — try both
    # Also build symbol lookup as fallback
    sym_map = {ctx["symbol"].upper(): isin
               for isin, ctx in watchlist.items()}

    alerts_sent = 0
    alerts_skipped = 0

    for ann in announcements:
        # Try to get ISIN from announcement
        isin = str(ann.get("ISIN", ann.get("isin",""))).strip()
        if not isin or isin == "nan":
            # Fallback: try symbol
            sym = str(ann.get("SCRIP_CD", ann.get("scripcd",""))).strip().upper()
            isin = sym_map.get(sym, "")

        if isin not in watchlist:
            continue

        headline = str(ann.get("HEADLINE", ann.get("headline",
                       ann.get("SubjectLong", ann.get("subject","")))))
        etype = classify(headline)

        # Only alert on meaningful events (skip generic board meets etc)
        if etype == "NEWS":
            continue

        # Dedup key
        dedup_key = f"{isin}|{etype}|{datetime.now().strftime('%Y-%m-%d')}"
        if dedup_key in seen:
            alerts_skipped += 1
            continue

        ctx = watchlist[isin]
        msg = format_alert(ann, ctx, etype)
        print(f"\n  ALERT: {ctx['symbol']} | {etype}")
        print(f"  {headline[:80]}")

        success = send_whatsapp(msg)
        if success:
            seen[dedup_key] = datetime.now().strftime("%Y-%m-%d")
            alerts_sent += 1
            time.sleep(1)  # rate limit: 1 msg/sec
        else:
            print(f"  [FAIL] WhatsApp send failed for {ctx['symbol']}")

    save_seen(seen)
    print(f"\n  Alerts sent    : {alerts_sent}")
    print(f"  Already seen   : {alerts_skipped}")
    print(f"  Run completed  : {datetime.now().strftime('%H:%M:%S')}")
    print("✅ cloud_alert_engine.py DONE")


if __name__ == "__main__":
    main()
