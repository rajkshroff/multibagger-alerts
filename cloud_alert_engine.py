#!/usr/bin/env python3
# ============================================================
# cloud_alert_engine.py — v3.0
# SESSION 42 — 3 Message Types + Dedup Fix + Hourly News
#
# MESSAGE TYPES:
#   TYPE 1 — ACTION PLAN
#     Trigger: push to CSVs (engine just ran)
#     Content: BUY/ACCUMULATE/BEAR_ACCUM stocks, all 5 models
#     When: after every engine run (3x daily from scheduler)
#
#   TYPE 2 — MORNING BRIEF
#     Trigger: 7:30am IST daily (schedule)
#     Content: Market state, global cues, top picks, sector cycle
#
#   TYPE 3 — HOURLY NEWS
#     Trigger: every hour 9am-6pm IST (schedule)
#     Content: RSS news filtered for our watchlist stocks
#              + ace investor moves + MF moves + macro events
#
# DEDUP FIX:
#   Removed git push from inside Python (_save_seen_hashes).
#   Python only WRITES seen_hashes.json.
#   YML handles ALL git commits. No more race conditions.
#
# LOCATION: Multibagger_App/multibagger-alerts/cloud_alert_engine.py
# ============================================================

import os, sys, json, hashlib, argparse
import requests, re
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    os.system("pip install pandas -q"); import pandas as pd
try:
    import yfinance as yf
except ImportError:
    os.system("pip install yfinance -q"); import yfinance as yf

# ── ARGS ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--test", action="store_true")
args, _ = parser.parse_known_args()
TEST_MODE = args.test

# ── TELEGRAM ─────────────────────────────────────────────────
BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN","")
             or os.environ.get("TELEGRAM_TOKEN",""))
# Comma-separated: setx TELEGRAM_CHAT_IDS "ID1,ID2,ID3"
_raw_ids  = os.environ.get("TELEGRAM_CHAT_IDS","") or os.environ.get("TELEGRAM_CHAT_ID","")
CHAT_IDS  = [x.strip() for x in _raw_ids.split(",") if x.strip()]
CHAT_ID   = CHAT_IDS[0] if CHAT_IDS else ""

if not BOT_TOKEN or not CHAT_IDS:
    print("❌ TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set")
    sys.exit(1)

print(f"  [telegram] {len(CHAT_IDS)} recipient(s)")

# ── PATHS ─────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent
CSV  = {
    "composite":    REPO / "composite_scores.csv",
    "market_intel": REPO / "market_intelligence.csv",
    "sector_cycle": REPO / "sector_cycle_status.csv",
    "action":       REPO / "action_language.csv",
    "early_alerts": REPO / "early_alerts.csv",
    "watchlist":    REPO / "watchlist_for_cloud.csv",
}
SEEN_FILE    = REPO / "seen_hashes.json"
MORNING_FILE = REPO / "morning_brief_sent.json"

# ── IST ───────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def h_m(): n = now_ist(); return n.hour, n.minute

# ── TELEGRAM SEND ─────────────────────────────────────────────
MAX = 4000
def send(text: str) -> bool:
    """Send to all recipients in CHAT_IDS."""
    if not text.strip(): return False
    url    = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+MAX] for i in range(0, len(text), MAX)]
    ok = True
    for chat in CHAT_IDS:
        for i, chunk in enumerate(chunks):
            suffix = f"\n<i>Part {i+1}/{len(chunks)}</i>" if len(chunks) > 1 else ""
            try:
                r = requests.post(url, json={
                    "chat_id": chat, "text": chunk+suffix, "parse_mode": "HTML"
                }, timeout=15)
                if r.ok:
                    print(f"  [send→..{chat[-4:]}] ✅ {len(chunk)} chars")
                else:
                    print(f"  [send→..{chat[-4:]}] HTTP {r.status_code}: {r.text[:100]}")
                    ok = False
            except Exception as e:
                print(f"  [send→..{chat[-4:]}] Error: {e}"); ok = False
    return ok

# ── CSV LOADER ────────────────────────────────────────────────
def load(key):
    p = CSV.get(key)
    if not p or not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, low_memory=False)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"  [csv] {key}: {e}")
        return pd.DataFrame()

def _sf(v, d=0.0):
    try: return float(v) if str(v) not in ("nan","None","") else d
    except: return d
def _si(v, d=0):
    try: return int(float(str(v))) if str(v) not in ("nan","None","") else d
    except: return d

# ── DEDUP (file-only, no git inside Python) ───────────────────
HASH_TTL_H = 48

def load_seen() -> dict:
    if not SEEN_FILE.exists(): return {}
    try: return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
    except: return {}

def save_seen(h: dict):
    """Write seen_hashes.json — YML commits it, NOT Python."""
    cutoff = (now_ist() - timedelta(hours=HASH_TTL_H)).isoformat()
    h = {k: v for k, v in h.items() if v >= cutoff}
    SEEN_FILE.write_text(json.dumps(h, indent=2), encoding="utf-8")
    print(f"  [dedup] {len(h)} hashes saved (YML will commit)")

def alert_hash(row) -> str:
    key = f"{row.get('NSE_SYMBOL','')}-{row.get('ALERT_TYPE','')}-{row.get('ALERT_DATE','')}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def news_hash(title: str) -> str:
    return hashlib.md5(title.strip().lower().encode()).hexdigest()[:12]

# ── MORNING BRIEF DEDUP ───────────────────────────────────────
def morning_sent_today() -> bool:
    today = now_ist().strftime("%Y-%m-%d")
    if not MORNING_FILE.exists(): return False
    try: return json.loads(MORNING_FILE.read_text()).get("date") == today
    except: return False

def mark_morning_sent():
    today = now_ist().strftime("%Y-%m-%d")
    MORNING_FILE.write_text(json.dumps({"date": today}, indent=2))
    print(f"  [brief] marked sent {today}")

# ── MARKET SUMMARY ────────────────────────────────────────────
def market_summary():
    mi = load("market_intel")
    mstate = fg = "—"
    fg_score = b200 = b50 = 0.0
    if not mi.empty:
        r      = mi.iloc[0]
        mstate = str(r.get("MARKET_STATE","—"))
        fg     = str(r.get("FEAR_GREED_LABEL","—"))
        fg_score = _sf(r.get("FEAR_GREED_SCORE",0))
        b200   = _sf(r.get("BREADTH_ABOVE_200DMA",0))
        b50    = _sf(r.get("BREADTH_ABOVE_50DMA",0))
    cs = load("composite")
    prime = strong = wlc = total = 0
    if not cs.empty and "TIER" in cs.columns:
        tc     = cs["TIER"].value_counts()
        total  = len(cs)
        prime  = _si(tc.get("PRIME",0))
        strong = _si(tc.get("STRONG",0))
        wlc    = _si(tc.get("WATCHLIST_CONFIRMED",0))
    icon = {"BULL":"🟢","CAUTION":"🟡","BEAR":"🔴"}.get(mstate,"⚪")
    text = (f"{icon} <b>{mstate}</b> | F&amp;G: {fg} ({fg_score:.0f}) | "
            f"Above 200DMA: {b200:.1f}%\n"
            f"PRIME <b>{prime}</b> | STRONG <b>{strong}</b> | WLC <b>{wlc}</b> | Universe {total:,}")
    return mstate, text

# ── GLOBAL CUES ───────────────────────────────────────────────
def global_cues():
    lines = []
    for label, ticker in [("Dow Jones","^DJI"),("Nasdaq","^IXIC"),
                           ("Crude WTI","CL=F"),("USD/INR","USDINR=X")]:
        try:
            info  = yf.Ticker(ticker).fast_info
            price = float(info.get("last_price",0) or 0)
            prev  = float(info.get("previous_close",0) or 0)
            if price <= 0: continue
            pct = (price-prev)/prev*100 if prev else 0
            icon = "🟢" if pct >= 0 else "🔴"
            lines.append(f"{icon} <b>{label:<12}</b> {price:>12,.1f}  ({pct:+.2f}%)")
        except: continue
    return lines

# ════════════════════════════════════════════════════════════════
# TYPE 1 — ACTION PLAN (push-triggered, after every engine run)
# ════════════════════════════════════════════════════════════════
BUY_ACTIONS = {"BUY","STRONG_BUY","FRESH_BUY","ACCUMULATE","ADD",
               "BEAR ACCUMULATE — 3X POTENTIAL","BEAR_ACCUM",
               "RECOVERY WATCH — TRANCHE BUY","RECOVERY_WATCH"}

def build_action_plan() -> str:
    al = load("action")
    if al.empty:
        return ""

    action_col = next((c for c in al.columns if c.upper() in
                       ("AI_ACTION","ACTION","RECOMMENDATION")), None)
    sym_col    = next((c for c in al.columns if c.upper() in
                       ("NSE_SYMBOL","SYMBOL")), None)
    name_col   = next((c for c in al.columns if c.upper() in
                       ("NAME","COMPANY")), None)
    phase_col  = next((c for c in al.columns if c.upper() == "SECTOR_PHASE"), None)
    score_col  = next((c for c in al.columns if c.upper() in
                       ("COMPOSITE_SCORE","COMPOSITE_BALANCED","SIGNAL_TOTAL")), None)
    if not action_col or not sym_col: return ""

    al[action_col] = al[action_col].astype(str).str.strip()

    def bucket(a):
        u = a.upper()
        if "BEAR ACCUM" in u or "3X POTENTIAL" in u: return "BEAR_ACCUM"
        if "RECOVERY"   in u or "TRANCHE"      in u: return "RECOVERY"
        if "STRONG_BUY" in u or "STRONG BUY"   in u: return "STRONG_BUY"
        if "BUY"        in u or "FRESH"        in u: return "BUY"
        if "ACCUMULATE" in u or "ADD"          in u: return "ACCUMULATE"
        return None

    al["_B"] = al[action_col].apply(bucket)
    act = al[al["_B"].notna()].copy()
    if score_col and score_col in act.columns:
        act[score_col] = pd.to_numeric(act[score_col], errors="coerce")
        act = act.sort_values(score_col, ascending=False)
    act = act.drop_duplicates(subset=[sym_col], keep="first")

    _, mkt = market_summary()
    mstate, _ = market_summary()
    now_str = now_ist().strftime("%d %b %Y  %H:%M")
    mkt_icon = {"BULL":"🟢","BEAR":"🔴","CAUTION":"🟡"}.get(mstate,"⚪")

    lines = [
        f"<b>🎯 ACTION PLAN — ENGINE RUN COMPLETE</b>",
        f"<b>{now_str}</b>  {mkt_icon} <b>{mstate}</b>",
        "",
        mkt.split("\n")[0],   # market state line
        "",
    ]

    LABELS = {"STRONG_BUY":"🟢 STRONG BUY","BUY":"🟢 BUY",
              "ACCUMULATE":"🔵 ACCUMULATE","BEAR_ACCUM":"💜 BEAR ACCUM",
              "RECOVERY":"🌱 RECOVERY WATCH"}

    total = 0
    for b in ["STRONG_BUY","BUY","ACCUMULATE","BEAR_ACCUM","RECOVERY"]:
        grp = act[act["_B"]==b]
        if grp.empty: continue
        total += len(grp)
        lines.append(f"<b>{LABELS[b]}</b>  ({len(grp)})")
        for _, row in grp.head(10).iterrows():
            sym  = str(row.get(sym_col,""))
            nm   = str(row.get(name_col,""))[:20] if name_col else ""
            phase = str(row.get(phase_col,"")).strip()[:12] if phase_col else ""
            sc    = _si(row.get(score_col,0)) if score_col else 0
            phase_s = f"[{phase}]" if phase and phase not in ("nan","None","—","") else ""
            lines.append(f"  • <code>{sym:<10}</code> {nm:<22} {phase_s} S:{sc}")
        if len(grp) > 10:
            lines.append(f"  ... +{len(grp)-10} more")
        lines.append("")

    if total == 0:
        lines.append("  No BUY signals this run — market conditions not met.")
        lines.append("  Check WATCHLIST + RECOVERY_WATCH in Excel.")

    # Tier counts
    cs = load("composite")
    if not cs.empty and "TIER" in cs.columns:
        tc = cs["TIER"].value_counts()
        lines.append(f"📊 PRIME <b>{tc.get('PRIME',0)}</b>  "
                     f"STRONG <b>{tc.get('STRONG',0)}</b>  "
                     f"WLC <b>{tc.get('WATCHLIST_CONFIRMED',0)}</b>  "
                     f"LANDMINE <b>{tc.get('LANDMINE',0)}</b>")

    lines.append(f"\n<i>🤖 Engine run complete — {total} actionable stocks</i>")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
# TYPE 2 — MORNING BRIEF (7:30am daily)
# ════════════════════════════════════════════════════════════════
def build_morning_brief() -> str:
    day = now_ist().strftime("%A, %d %b %Y")
    mstate, mkt_text = market_summary()

    # Top picks
    al  = load("action")
    picks = []
    if not al.empty:
        ac = next((c for c in al.columns if "ACTION" in c.upper()), None)
        sc = next((c for c in al.columns if "COMPOSITE" in c.upper()), None)
        if ac and sc:
            al[sc] = pd.to_numeric(al[sc], errors="coerce")
            buys = al[al[ac].str.contains("BUY|ACCUM", case=False, na=False)]
            for _, r in buys.nlargest(5, sc).iterrows():
                sym   = str(r.get("NSE_SYMBOL",""))
                tier  = str(r.get("MULTIBAGGER_TIER",""))
                score = _si(r.get(sc,0))
                entry = str(r.get("ENTRY_ZONE","—") or "—")
                sl    = str(r.get("STOP_LOSS","—") or "—")
                act   = str(r.get(ac,""))[:40]
                picks.append(f"  <code>{sym:<12}</code> {tier:<22} {score}/100\n"
                             f"    ↳ {act}  Entry:{entry}  SL:{sl}")

    # Early alerts
    ea = load("early_alerts")
    alerts = []
    if not ea.empty:
        for _, r in ea.head(4).iterrows():
            sev  = str(r.get("SEVERITY",""))
            sym  = str(r.get("NSE_SYMBOL",""))
            atyp = str(r.get("ALERT_TYPE",""))
            det  = str(r.get("ALERT_DETAIL",""))[:80]
            icon = {"VERY_HIGH":"🔥","HIGH":"⚡","MEDIUM":"📌"}.get(sev,"📌")
            alerts.append(f"  {icon} <b>{sym}</b> [{atyp}] {det}")

    # Sector cycle
    sc_df = load("sector_cycle")
    cycles = []
    if not sc_df.empty and "CYCLE_PHASE" in sc_df.columns:
        for phase, icon in [("MID_CYCLE","🚀"),("EARLY_RECOVERY","🌱"),("BASING","⏸")]:
            subs = sc_df[sc_df["CYCLE_PHASE"]==phase]
            if not subs.empty:
                ig_col = next((c for c in subs.columns if "INDUSTRY" in c.upper()), None)
                if ig_col:
                    names = ", ".join(subs[ig_col].tolist()[:3])
                    cycles.append(f"  {icon} {phase}: {names}")

    cues = global_cues()

    msg = f"🇮🇳 <b>MULTIBAGGER — MORNING BRIEF</b>\n<b>{day}</b>\n\n{mkt_text}\n"
    if cues:
        msg += "\n<b>🌍 Global Cues</b>\n" + "\n".join(cues) + "\n"
    if picks:
        msg += "\n<b>🎯 Top Picks</b>\n" + "\n".join(picks) + "\n"
    if cycles:
        msg += "\n<b>🔄 Sector Cycle</b>\n" + "\n".join(cycles) + "\n"
    if alerts:
        msg += "\n<b>⚡ Early Alerts</b>\n" + "\n".join(alerts) + "\n"
    msg += f"\n<i>Engine data from last run. Open Excel for full detail.</i>"
    return msg


# ════════════════════════════════════════════════════════════════
# TYPE 3 — HOURLY NEWS (9am-6pm, RSS + universe filter)
# ════════════════════════════════════════════════════════════════

RSS_FEEDS = [
    # Corporate filings — highest signal for specific stocks
    {"name":"BSE Filings",  "url":"https://www.bseindia.com/xml-data/corpfiling/CorpFile.xml"},
    {"name":"NSE Filings",  "url":"https://nsearchives.nseindia.com/content/RSS/rss.xml"},
    # Business news
    {"name":"ET Markets",   "url":"https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"},
    {"name":"ET Economy",   "url":"https://economictimes.indiatimes.com/news/economy/rssfeeds/20080670.cms"},
    {"name":"Mint",         "url":"https://www.livemint.com/rss/markets"},
    {"name":"Moneycontrol", "url":"https://www.moneycontrol.com/rss/MCtopnews.xml"},
    {"name":"BS Markets",   "url":"https://www.business-standard.com/rss/markets-106.rss"},
    {"name":"FE Markets",   "url":"https://www.financialexpress.com/market/feed/"},
    # Policy
    {"name":"SEBI",         "url":"https://www.sebi.gov.in/sebi_data/rss/sebirss.xml"},
    {"name":"RBI",          "url":"https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid=rss"},
]

# Ace investors to watch for by name in headlines
ACE_INVESTORS = [
    "ashish kacholia","kacholia",
    "dolly khanna",
    "mukul agarwal","mukul aggarwal",
    "vijay kedia",
    "porinju veliyath","porinju",
    "mohnish pabrai",
    "radhakishan damani",
    "rekha jhunjhunwala","jhunjhunwala",
    "nalanda capital",
    "ace investor",
]

# High-impact keywords that affect stock prices
HIGH_IMPACT = [
    # Insider / institutional activity
    "insider","promoter bought","promoter sold","bulk deal","block deal",
    "pledging","pledge invoked","pledge","mutual fund exit","mf exit",
    "fii bought","fii sold","fii selling","fii buying",
    # Analyst actions
    "analyst buy","analyst sell","target price","upgrade","downgrade",
    "initiates coverage","outperform","overweight","underweight",
    # Earnings / results
    "earnings beat","earnings miss","profit jump","profit falls","loss widens",
    "q3 results","q4 results","q1 results","q2 results",
    "revenue growth","net profit","ebitda","pat rises","pat falls",
    # Orders / business wins — catches Tata Power type news
    "order win","order worth","order received","order bag","bags order",
    "contract awarded","contract won","supply agreement","supply contract",
    "agreement signed","govt approves","government approves","nod received",
    "ppa signed","loa received","letter of award","project award",
    "capacity addition","plant approval","new order","secures order",
    # Corporate events
    "capex","acquisition","merger","demerger","buyback","dividend",
    "delisting","ban","sebi order","rbi policy","rate cut","rate hike",
    # Sector/macro
    "pli scheme","production linked","tariff hike","price hike",
    "import duty","export ban","subsidy",
]

def get_universe_symbols() -> set:
    """Get ALL symbols we track — broad universe for news matching."""
    symbols = set()
    # From watchlist CSV
    wl = load("watchlist")
    if not wl.empty:
        sym_col = next((c for c in wl.columns if "SYMBOL" in c.upper()), None)
        if sym_col:
            symbols.update(wl[sym_col].dropna().astype(str).str.strip().str.upper().tolist())
    # From composite — all non-AVOID tiers (not just PRIME/STRONG)
    cs = load("composite")
    if not cs.empty and "TIER" in cs.columns and "NSE_SYMBOL" in cs.columns:
        # Include everything except pure AVOID (no signals, no quality)
        watched = cs[cs["TIER"].isin(["PRIME","STRONG","WATCHLIST_CONFIRMED",
                                       "WATCHLIST_EXTERNAL","MONITOR"])]
        symbols.update(watched["NSE_SYMBOL"].dropna().astype(str).str.upper().tolist())
    # Always include Nifty50 large caps — news about them is always market-moving
    NIFTY50_SYMS = {
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR",
        "ITC","SBIN","BAJFINANCE","BHARTIARTL","KOTAKBANK","LT",
        "HCLTECH","AXISBANK","ASIANPAINT","MARUTI","TITAN","NESTLEIND",
        "ULTRACEMCO","WIPRO","POWERGRID","NTPC","TATAMOTORS","TATAPOWER",
        "ADANIENT","ADANIPORTS","SUNPHARMA","DRREDDY","CIPLA","DIVISLAB",
        "ONGC","COALINDIA","BPCL","TECHM","INDUSINDBK","BAJAJFINSV",
        "GRASIM","HINDALCO","JSWSTEEL","TATASTEEL","EICHERMOT","APOLLOHOSP",
        "HEROMOTOCO","M&M","BRITANNIA","BAJAJ-AUTO","VEDL","SHRIRAMFIN",
    }
    symbols.update(NIFTY50_SYMS)
    return symbols

def fetch_rss(url: str, timeout=8) -> list:
    """Fetch RSS and return list of {title, link, date}."""
    try:
        r = requests.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; MultibaggerBot/1.0)"
        })
        if not r.ok: return []
        items = []
        # Parse title tags
        titles = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', r.text, re.DOTALL)
        if not titles:
            titles = re.findall(r'<title>(.*?)</title>', r.text, re.DOTALL)
        # Strip CDATA wrappers and XML entities from titles
        clean = []
        for t in titles:
            t = t.strip()
            t = re.sub(r'<!\[CDATA\[|\]\]>', '', t, flags=re.IGNORECASE)
            t = t.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')
            t = re.sub(r'<[^>]+>', '', t)   # strip any remaining HTML tags
            t = t.strip()
            if len(t) > 10:
                clean.append(t)
        titles = clean
        # Skip feed title (first item usually)
        if titles and len(titles) > 1:
            titles = titles[1:]
        return titles[:15]
    except Exception as e:
        print(f"  [rss] {url[:40]}: {e}")
        return []

def is_relevant(headline: str, universe: set) -> tuple:
    """
    Returns (relevant: bool, category: str, matched: str)
    Categories: ACE_INVESTOR, HIGH_IMPACT, UNIVERSE_STOCK
    """
    hl = headline.lower()

    # Check ace investors
    for inv in ACE_INVESTORS:
        if inv in hl:
            return True, "ACE_INVESTOR", inv.title()

    # Check high-impact keywords
    for kw in HIGH_IMPACT:
        if kw in hl:
            # Extra check — is a universe stock mentioned?
            for sym in universe:
                if sym.lower() in hl or sym.lower()[:5] in hl:
                    return True, "STOCK_SIGNAL", sym
            # High-impact even without specific stock
            if any(k in hl for k in ["sebi","rbi","fii","mf exit","bulk deal",
                                      "block deal","pledge","rate cut","rate hike"]):
                return True, "MACRO_SIGNAL", kw

    # Direct symbol mention in universe
    for sym in universe:
        if len(sym) >= 4 and sym.lower() in hl:
            return True, "UNIVERSE_STOCK", sym

    return False, "", ""

def build_hourly_news() -> str:
    universe = get_universe_symbols()
    print(f"  [news] Universe: {len(universe)} symbols")

    seen   = load_seen()
    new_news = []

    for feed in RSS_FEEDS:
        titles = fetch_rss(feed["url"])
        for title in titles:
            h = news_hash(title)
            if h in seen:
                continue
            relevant, category, matched = is_relevant(title, universe)
            if relevant:
                new_news.append({
                    "title": title,
                    "category": category,
                    "matched": matched,
                    "source": feed["name"],
                    "hash": h,
                })

    if not new_news:
        return ""

    # Sort: ACE_INVESTOR and STOCK_SIGNAL first
    CAT_ORDER = {"ACE_INVESTOR":0, "STOCK_SIGNAL":1, "UNIVERSE_STOCK":2, "MACRO_SIGNAL":3}
    new_news.sort(key=lambda x: CAT_ORDER.get(x["category"], 9))

    now_str = now_ist().strftime("%H:%M IST, %d %b")
    lines = [f"📰 <b>MARKET NEWS UPDATE — {now_str}</b>", ""]

    CAT_ICONS = {
        "ACE_INVESTOR":  "👁 ACE INVESTOR",
        "STOCK_SIGNAL":  "⚡ STOCK SIGNAL",
        "UNIVERSE_STOCK":"📌 WATCHLIST STOCK",
        "MACRO_SIGNAL":  "🌐 MACRO",
    }

    for item in new_news[:12]:  # cap at 12 per hour
        icon = CAT_ICONS.get(item["category"], "📎")
        match_str = f"  [{item['matched']}]" if item["matched"] else ""
        import html as _html
        title_clean = _html.escape(item["title"][:120])
        # symbol tag removed — often wrong and noisy
        lines.append(f"{icon}\n  {title_clean}\n  <i>— {item['source']}</i>\n")

    lines.append(f"<i>{len(new_news)} new items found | Full analysis in next engine run</i>")

    # Mark as seen (file only, YML commits)
    now_iso = now_ist().isoformat()
    for item in new_news:
        seen[item["hash"]] = now_iso
    save_seen(seen)

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
def main():
    h, m = h_m()
    now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
    triggered_by_push = os.environ.get("TRIGGERED_BY_PUSH","false").lower() == "true"

    print(f"[{now_str}] cloud_alert_engine v3.0")
    print(f"  IST: {h:02d}:{m:02d} | push={triggered_by_push} | test={TEST_MODE}")

    # ── TEST ─────────────────────────────────────────────────
    if TEST_MODE:
        _, mkt = market_summary()
        msg = (f"🧪 <b>MULTIBAGGER v3.0 — TEST</b>\n"
               f"{now_ist().strftime('%H:%M IST')}\n{mkt}\n"
               f"<i>3 message types active. System verified.</i>")
        ok = send(msg)
        print(f"  Test sent: {ok}")
        return

    sent = 0

    # ── TYPE 1: ACTION PLAN (push-triggered = engine just ran) ──
    if triggered_by_push:
        print("  → TYPE 1: Action Plan (push trigger — engine just ran)")
        msg = build_action_plan()
        if msg:
            ok = send(msg)
            print(f"  Action Plan sent: {ok}")
            if ok: sent += 1
        else:
            print("  No actionable stocks — silent")
        return   # push trigger = action plan only, don't run news scan

    # ── TYPE 2: MORNING BRIEF (7:30am) ───────────────────────
    in_brief_window = (h == 7 and m >= 30) or (h == 8 and m < 30)
    if in_brief_window and not morning_sent_today():
        print("  → TYPE 2: Morning Brief")
        msg = build_morning_brief()
        ok  = send(msg)
        if ok: mark_morning_sent()
        print(f"  Morning Brief sent: {ok}")
        sent += 1
        # After brief, also run news scan for the morning
        # Fall through to TYPE 3

    # ── TYPE 3: HOURLY NEWS (9am-6pm) ────────────────────────
    if 9 <= h <= 18:
        print(f"  → TYPE 3: Hourly News (market hours)")
        msg = build_hourly_news()
        if msg:
            ok = send(msg)
            print(f"  News sent: {ok}")
            if ok: sent += 1
        else:
            print("  No new relevant news this hour")

    # Outside all windows
    if sent == 0 and not (9 <= h <= 18) and not in_brief_window:
        print(f"  → Outside windows ({h:02d}:{m:02d}) — silent")

    print(f"  Total sent: {sent}")


if __name__ == "__main__":
    main()
