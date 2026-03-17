# =============================================================
# cloud_alert_engine.py  —  24x7 Cloud Alert Engine
# SESSION 36  —  Telegram edition
#
# Runs on GitHub Actions every 30 min (9am-5pm IST, Mon-Fri)
# Free: unlimited Telegram messages, 2000 min/month GitHub Actions
#
# Required GitHub Secrets (repo Settings → Secrets → Actions):
#   TELEGRAM_TOKEN     — from @BotFather
#   TELEGRAM_CHAT_ID   — your personal chat ID
#   GITHUB_TOKEN       — auto-provided by GitHub Actions
#
# What it does each run:
#   1. Loads watchlist_for_cloud.csv (PRIME/STRONG/WLC stocks)
#   2. Fetches BSE corporate announcements (last 2h)
#   3. Classifies: ORDER_WIN / RESULT / CAPEX / PENALTY etc
#   4. Cross-checks against your watchlist
#   5. Deduplicates (seen_alerts.json stored in repo)
#   6. Sends Telegram alert for new events
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
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SEEN_FILE        = Path("seen_alerts.json")
WATCHLIST        = Path("watchlist_for_cloud.csv")
BSE_LOOKBACK_HOURS = 2

# ── Event classification ──────────────────────────────────────
CLASSIFY_RULES = [
    ("ORDER_WIN",   r"order|contract|work order|letter of (intent|award)|awarded|bagged|secured|lo[ia]\b"),
    ("RESULT_BEAT", r"(profit|revenue|net income).{0,20}(jump|surge|rise|grew|up).{0,10}\d+\s*%|beat.{0,20}estimate"),
    ("CAPEX",       r"capex|capital expenditure|new plant|capacity expansion|greenfield|brownfield"),
    ("FUNDRAISE",   r"qip|rights issue|preferential allotment|fpo|ncd|debenture"),
    ("DIVIDEND",    r"dividend|interim dividend|special dividend"),
    ("BUYBACK",     r"buyback|buy.?back|share repurchase"),
    ("ACQUISITION", r"acqui|takeover|merger|amalgamation|demerger"),
    ("MGMT_CHANGE", r"appoint|new (ceo|md|cfo|director)|resign|step.?down"),
    ("PENALTY",     r"penalty|fine|sebi|tax demand|gst demand|show cause|enforcement"),
    ("RATING_UP",   r"upgrade|target (raised|hiked|increased)|buy rating|overweight"),
    ("RATING_DOWN", r"downgrade|target (cut|reduced|lowered)|sell rating|underweight"),
    ("RESULT",      r"quarterly result|financial result|unaudited|q[1-4] result"),
]

SEVERITY_EMOJI = {
    "ORDER_WIN":   "🟢",
    "RESULT_BEAT": "🟢",
    "CAPEX":       "🔵",
    "FUNDRAISE":   "🔵",
    "ACQUISITION": "🔵",
    "BUYBACK":     "🔵",
    "DIVIDEND":    "⚪",
    "MGMT_CHANGE": "🟡",
    "PENALTY":     "🔴",
    "RATING_UP":   "🟢",
    "RATING_DOWN": "🔴",
    "RESULT":      "⚪",
}

TIER_LABEL = {
    "PRIME":               "PRIME",
    "STRONG":              "STRONG",
    "WATCHLIST_CONFIRMED": "WL-CONFIRMED",
    "WATCHLIST_EXTERNAL":  "WL-EXTERNAL",
}


def classify(text: str) -> str:
    t = text.lower()
    for etype, pattern in CLASSIFY_RULES:
        if re.search(pattern, t, re.IGNORECASE):
            return etype
    return "NEWS"


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
        print(f"  BSE: {len(anns)} announcements (last {hours_back}h)")
        return anns
    except Exception as e:
        print(f"  [WARN] BSE fetch failed: {e}")
        return []


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
                "symbol":  str(r.get("NSE_SYMBOL","")),
                "name":    str(r.get("NAME","")),
                "tier":    str(r.get("TIER","")),
                "signals": int(float(str(r.get("SIGNALS_FIRED", 0) or 0))),
                "score":   int(float(str(r.get("COMPOSITE_BALANCED", 0) or 0))),
                "phase":   str(r.get("SECTOR_PHASE","")),
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


def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"  [SKIP] Telegram not configured")
        print(f"  MSG: {message[:100]}")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            return True
        print(f"  [TG ERROR] {r.status_code}: {r.text[:200]}")
        return False
    except Exception as e:
        print(f"  [TG ERROR] {e}")
        return False


def format_alert(ann: dict, ctx: dict, etype: str) -> str:
    symbol   = ctx["symbol"]
    tier     = TIER_LABEL.get(ctx["tier"], ctx["tier"])
    sigs     = ctx["signals"]
    score    = ctx["score"]
    phase    = ctx["phase"]
    emoji    = SEVERITY_EMOJI.get(etype, "⚪")
    headline = str(ann.get("HEADLINE", ann.get("headline",
               ann.get("SubjectLong", ann.get("subject","")))))[:180]
    bse_code = str(ann.get("SCRIP_CD", ann.get("scripcd","")))
    ann_time = str(ann.get("DT_TM",   ann.get("dt_tm",""))).strip()[:16]

    return (
        f"{emoji} <b>{symbol}</b> | {tier}\n"
        f"<b>{etype}</b>\n"
        f"{'─'*32}\n"
        f"{headline}\n"
        f"{'─'*32}\n"
        f"Score: {score}/100  Signals: {sigs}/15\n"
        f"Sector: {phase}\n"
        f"Time: {ann_time}\n"
        f"BSE: {bse_code}\n"
        f"<i>Multibagger Engine Alert</i>"
    )


def main():
    print("=" * 60)
    print(f"  CLOUD ALERT ENGINE  {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    watchlist = load_watchlist()
    if not watchlist:
        print("  Empty watchlist — nothing to do")
        return

    seen = load_seen()
    anns = fetch_bse_announcements(BSE_LOOKBACK_HOURS)

    # Symbol → ISIN fallback map
    sym_map = {ctx["symbol"].upper(): isin for isin, ctx in watchlist.items()}

    sent = skipped = 0

    for ann in anns:
        # Get ISIN from announcement
        isin = str(ann.get("ISIN", ann.get("isin",""))).strip()
        if not isin or isin in ("nan",""):
            sym  = str(ann.get("SCRIP_CD", ann.get("scripcd",""))).strip().upper()
            isin = sym_map.get(sym, "")

        if isin not in watchlist:
            continue

        headline = str(ann.get("HEADLINE", ann.get("headline",
                       ann.get("SubjectLong", ann.get("subject","")))))
        etype = classify(headline)

        if etype == "NEWS":
            continue  # skip uncategorised noise

        dedup = f"{isin}|{etype}|{datetime.now().strftime('%Y-%m-%d')}"
        if dedup in seen:
            skipped += 1
            continue

        ctx = watchlist[isin]
        msg = format_alert(ann, ctx, etype)
        print(f"\n  ALERT: {ctx['symbol']} | {etype}")
        print(f"  {headline[:80]}")

        if send_telegram(msg):
            seen[dedup] = datetime.now().strftime("%Y-%m-%d")
            sent += 1
            time.sleep(1)  # 1 msg/sec rate limit

    save_seen(seen)
    print(f"\n  Sent: {sent}  |  Already seen: {skipped}")
    print("Done")


if __name__ == "__main__":
    main()
