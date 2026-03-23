#!/usr/bin/env python3
"""
build_corporate_announcements.py — v2.0
Corporate Announcements Live Poller with Engine Context Enrichment.
Runs every 15 min via GitHub Actions (9am-4:30pm IST weekdays).
Also writes to recent_events.csv for S16 RECENT_CATALYST scoring.
"""
import os, sys, json, hashlib, requests, csv
from datetime import datetime, timezone, timedelta
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN","") or os.environ.get("TELEGRAM_TOKEN","")
_ids = os.environ.get("TELEGRAM_CHAT_IDS","") or os.environ.get("TELEGRAM_CHAT_ID","")
CHAT_IDS = [x.strip() for x in _ids.split(",") if x.strip()]

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)

REPO      = Path(__file__).resolve().parent
SEEN_FILE = REPO / "corp_announcements_seen.json"
MAX       = 4000

# ── UNIVERSE + ENGINE DATA ────────────────────────────────────
def load_universe():
    """Load NSE symbols + engine scores for context enrichment."""
    syms = {}   # {NSE_SYMBOL: {tier, score, entry, sl, target}}
    for p in [REPO / "composite_scores.csv", REPO / "watchlist_for_cloud.csv"]:
        if not p.exists(): continue
        try:
            with open(p, encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    sym  = (row.get("NSE_SYMBOL","") or row.get("SYMBOL","")).strip().upper()
                    tier = (row.get("TIER","") or row.get("MULTIBAGGER_TIER","")).strip()
                    if not sym: continue
                    if sym not in syms:
                        syms[sym] = {
                            "tier":   tier,
                            "score":  row.get("COMPOSITE_BALANCED","") or row.get("COMPOSITE_SCORE",""),
                            "entry":  row.get("ENTRY_ZONE",""),
                            "sl":     row.get("STOP_LOSS",""),
                            "exp":    row.get("EXPECTED_RETURN",""),
                        }
                    elif not syms[sym].get("tier"):
                        syms[sym]["tier"] = tier
        except Exception as e:
            print(f"  [universe] {e}")
    # Nifty50 always included (no score data, just tracking)
    for s in ["RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
              "BAJFINANCE","BHARTIARTL","KOTAKBANK","LT","HCLTECH","AXISBANK","ASIANPAINT",
              "MARUTI","TITAN","NESTLEIND","ULTRACEMCO","WIPRO","TATAMOTORS","TATAPOWER",
              "ADANIENT","ADANIPORTS","SUNPHARMA","DRREDDY","CIPLA","DIVISLAB"]:
        if s not in syms:
            syms[s] = {"tier":"","score":"","entry":"","sl":"","exp":""}
    print(f"  [universe] {len(syms)} symbols tracked")
    return syms

# ── BSE CODE → COMPANY NAME MAP ──────────────────────────────
def load_bse_name_map() -> dict:
    """Resolve BSE numeric codes to company names.
    BSE AnnSubCategoryGetData API does not reliably return a name field.
    We build the map from identity_canonical.csv in the main app repo."""
    m = {}
    candidates = [
        REPO.parent / "ENGINE_CORE" / "data_internal" / "identity_canonical.csv",
        REPO.parent / "ENGINE_CORE" / "data_external" / "identity_canonical.csv",
        REPO / "identity_canonical.csv",
    ]
    for cand in candidates:
        if cand.exists():
            try:
                with open(cand, encoding="utf-8", errors="replace") as f:
                    for row in csv.DictReader(f):
                        bsc  = str(row.get("BSE_CODE","") or "").strip().split(".")[0]
                        name = str(row.get("NAME","") or "").strip()
                        if bsc and bsc.isdigit() and name:
                            m[bsc] = name
                print(f"  [bse_name_map] {len(m):,} codes loaded from {cand.name}")
            except Exception as e:
                print(f"  [bse_name_map] {e}")
            break
    if not m:
        print("  [bse_name_map] WARNING: identity_canonical.csv not found — BSE names will show as codes")
    return m

# ── SEEN HASHES ───────────────────────────────────────────────
def load_seen():
    if SEEN_FILE.exists():
        try: return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        except: pass
    return {}

def save_seen(seen):
    cutoff = (now_ist() - timedelta(days=7)).isoformat()
    pruned = {k: v for k, v in seen.items() if v > cutoff}
    SEEN_FILE.write_text(json.dumps(pruned, indent=2), encoding="utf-8")

def ann_hash(sym, subj):
    return hashlib.md5(f"{sym}|{subj[:60]}".encode()).hexdigest()[:12]

# ── PRIORITY CLASSIFICATION ───────────────────────────────────
# Truly material — ALWAYS fire regardless of category
MATERIAL_ALWAYS_FIRE = [
    # Orders & contracts
    "order win","order worth","new order","secures order","order received",
    "contract awarded","contract won","bags order","purchase order","work order",
    "letter of award","loa received","letter of intent","mou signed","ppa signed",
    "supply agreement","framework agreement","project award","project win",
    "epc contract","repeat order","export order","government contract","govt order",
    # Earnings
    "financial results","quarterly results","q1 result","q2 result",
    "q3 result","q4 result","annual result","half year result",
    # Corporate actions
    "dividend","bonus share","stock split","face value split",
    "buyback","share buyback","open offer","rights issue",
    # M&A
    "acquisition","acquires","merger","amalgamation","demerger","spin-off",
    "takeover","strategic investment","joint venture","jv with",
    "divestment","divestiture","sells stake",
    # Regulatory (negative)
    "sebi order","sebi penalty","sebi ban","sebi notice","sebi show cause",
    "sebi investigation","enforcement directorate","ed raid","ed notice",
    "income tax raid","cbi notice","fraud","scam","alleged fraud",
    "nclt","insolvency","bankruptcy","liquidation","default","npa",
    "loan recall","compulsory delisting","suspension",
    # Management (material change only)
    "ceo resign","md resign","cfo resign","managing director resign",
    "auditor resign","statutory auditor","going concern","adverse opinion",
    # Business events
    "drug approval","dcgi approval","usfda approval","fda approval",
    "anda approval","patent granted","pli scheme","pli benefit",
    "tariff hike","import duty","anti-dumping","safeguard duty",
    "capacity addition","capacity expansion","new plant","plant commissioned",
    "plant fire","factory fire","boiler blast","industrial accident",
    "force majeure","plant shutdown","operations suspended","strike",
    "contract cancelled","contract terminated","order cancellation",
    # Capital markets
    "qip","preferential allotment","preferential issue","fpo","ncd",
    "pledge invoked","pledge created","credit rating","rating downgrade","rating upgrade",
    # Ace investors
    "ashish kacholia","dolly khanna","vijay kedia","mukul agarwal",
    "porinju","rekha jhunjhunwala","nalanda capital",
]

# Always skip — routine filings with no price impact
# NOTE: checked BEFORE MATERIAL_ALWAYS_FIRE so that a trading-window notice
# mentioning "Audited Financial Results" in its text does NOT fire.
ROUTINE_SKIP = [
    "trading window",
    "regulation 30",
    "sebi (lodr)",
    "listing obligations and disclosure",
    "intimation of closure",
    "new listing",
    "listing of securities",
    "listing of equity shares",
    "postal ballot",          # routine shareholder vote
    "scrutinizer report",     # postal ballot outcome
    "scrutinizer's report",
    "closure of trading",     # alternate BSE phrasing
]

# Block these categories unless subject has material keyword
NOISY_CATEGORIES = {
    "Insider Trading / SAST",
    "Insider Trading",
    "Company Update",
    "Intimation",
}
NOISY_CAT_OVERRIDE = [
    "pledge invoked","acquisition","bulk deal","block deal",
    "merger","open offer","takeover","fraud",
]

def classify(subject: str, category: str) -> bool:
    subj = subject.lower()
    cat  = category.strip()
    # ROUTINE SKIP FIRST — a trading-window notice like
    # "...till 48 hours from announcement of Audited Financial Results"
    # contains "financial results" which would wrongly fire MATERIAL_ALWAYS_FIRE
    # if checked first.
    for r in ROUTINE_SKIP:
        if r in subj:
            return False
    # Material always-fire (only reached if not routine)
    for m in MATERIAL_ALWAYS_FIRE:
        if m in subj:
            return True
    # Noisy category block
    if cat in NOISY_CATEGORIES:
        return any(kw in subj for kw in NOISY_CAT_OVERRIDE)
    # CRITICAL_CATEGORIES match
    CRITICAL_CATS = {
        "Financial Results","Board Meeting","Dividend","Bonus","Split","Buyback",
        "Annual Report","AGM","EGM","Acquisition","Merger","Demerger","Amalgamation",
        "SEBI","Penalty","Pledging","Order Received","New Order","Contract","LOA",
        "Award","Rights Issue","FPO","NCD","QIP","Preferential Allotment",
        "Credit Rating","Default","Change in Management","Appointment","Resignation",
    }
    for cc in CRITICAL_CATS:
        if cc.lower() in cat.lower():
            return True
    # Specific keyword fallback
    for kw in ["sebi order","sebi penalty","rating downgrade","rating upgrade",
               "pledge invoked","going concern","auditor resign"]:
        if kw in subj:
            return True
    return False

# ── ENGINE CONTEXT FOR TELEGRAM ───────────────────────────────
def engine_context(sym: str, universe: dict) -> str:
    """Return engine tier/score/price guidance string for a symbol."""
    info = universe.get(sym.upper(), {})
    if not info: return ""
    tier  = info.get("tier","")
    score = info.get("score","")
    entry = info.get("entry","")
    sl    = info.get("sl","")
    exp   = info.get("exp","")
    if not tier and not score: return ""
    parts = []
    if tier and tier not in ("AVOID","MONITOR"):
        parts.append(f"<b>{tier}</b>")
    if score:
        try: parts.append(f"S:{int(float(str(score)))}")
        except: pass
    # Entry zone — extract ₹ range
    if entry and "₹" in entry:
        import re
        nums = re.findall(r"₹([\d,]+)", entry)
        if len(nums) >= 2: parts.append(f"Entry ₹{nums[0]}–{nums[1]}")
        elif nums:         parts.append(f"Entry ₹{nums[0]}")
    # Stop loss
    if sl and "₹" in sl:
        import re
        m = re.search(r"₹([\d,]+)", sl)
        if m: parts.append(f"SL ₹{m.group(1)}")
    # Target from expected return
    return " | ".join(parts) if parts else ""

# ── NSE FETCH ─────────────────────────────────────────────────
NSE_H = {"User-Agent":"Mozilla/5.0","Accept":"application/json",
          "Accept-Language":"en-US,en;q=0.9","Referer":"https://www.nseindia.com/"}

def fetch_nse():
    try:
        s = requests.Session(); s.headers.update(NSE_H)
        s.get("https://www.nseindia.com", timeout=8)
        r = s.get("https://www.nseindia.com/api/corp-announcements?index=equities",
                  timeout=12)
        if r.ok:
            d = r.json()
            return d if isinstance(d, list) else d.get("data", [])
    except Exception as e: print(f"  [nse] {e}")
    return []

def fetch_bse():
    try:
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
               "?strCat=-1&strPrevDate=&strScrip=&strSearch=P&strToDate=&strType=C")
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0",
                                        "Referer":"https://www.bseindia.com/"}, timeout=12)
        if r.ok:
            d = r.json()
            return d if isinstance(d, list) else d.get("Table", []) or []
    except Exception as e: print(f"  [bse] {e}")
    return []

# ── TELEGRAM ──────────────────────────────────────────────────
def send_tg(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_IDS: return False
    ok = True
    for cid in CHAT_IDS:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        for chunk in [text[i:i+MAX] for i in range(0, len(text), MAX)]:
            try:
                r = requests.post(url, json={"chat_id":cid,"text":chunk,"parse_mode":"HTML"},
                                  timeout=15)
                if not r.ok: print(f"  [tg→{cid[-4:]}] {r.status_code}: {r.text[:80]}"); ok=False
                else:         print(f"  [tg→{cid[-4:]}] ✅ sent")
            except Exception as e: print(f"  [tg] {e}"); ok=False
    return ok

def build_msg(anns: list, universe: dict) -> str:
    if not anns: return ""
    import html as _h
    now_s = now_ist().strftime("%H:%M IST, %d %b")
    lines = [f"⚡ <b>CORP ANNOUNCEMENT — {now_s}</b>", ""]
    for a in anns[:12]:
        sym     = a.get("symbol","")
        company = a.get("company","")
        subj    = a["subject"][:120]
        disp    = _h.escape(company if company else sym)
        code    = f" <i>({a['exchange']}:{sym})</i>" if company else f" <i>({a['exchange']}:{sym})</i>"
        ctx     = engine_context(sym, universe)
        lines.append(f"🚨 <b>{disp}</b>{code}")
        lines.append(f"  {_h.escape(subj)}")
        if ctx: lines.append(f"  📊 {ctx}")
        lines.append("")
    if len(anns) > 12:
        lines.append(f"<i>+{len(anns)-12} more</i>")
    lines.append(f"<i>Corp poller v2.0 · {len(anns)} new announcement(s)</i>")
    return "\n".join(lines)

# ── MAIN ──────────────────────────────────────────────────────
def main():
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] Corp Announcements v2.0")
    if not BOT_TOKEN or not CHAT_IDS:
        print("  ⚠ Telegram not configured")

    universe     = load_universe()
    bse_name_map = load_bse_name_map()
    seen         = load_seen()
    new_anns = []

    # NSE
    print("  [nse] Fetching...")
    nse_raw = fetch_nse()
    print(f"  [nse] {len(nse_raw)} items")
    for item in nse_raw:
        import html as _h
        sym  = str(item.get("symbol","") or "").strip().upper()
        subj = _h.unescape(str(item.get("desc","") or item.get("subject",""))).strip()
        cat  = str(item.get("sm_name","") or item.get("series","") or "").strip()
        if not sym or (universe and sym not in universe): continue
        h = ann_hash(sym, subj)
        if h in seen: continue
        if classify(subj, cat):
            new_anns.append({"symbol":sym,"company":"","subject":subj,
                             "category":cat,"exchange":"NSE"})
        seen[h] = now_ist().isoformat()

    # BSE
    print("  [bse] Fetching...")
    bse_raw = fetch_bse()
    print(f"  [bse] {len(bse_raw)} items")
    for item in bse_raw:
        scrip   = str(item.get("SCRIP_CD","") or "").strip()
        subj    = str(item.get("HEADLINE","") or "").strip()
        cat     = str(item.get("CATEGORYNAME","") or "").strip()
        company = str(item.get("short_name","") or item.get("COMPANYNAME","") or "").strip()
        if not company:
            company = bse_name_map.get(scrip, "")  # resolve from identity_canonical
        if not scrip or not subj: continue
        h = ann_hash(scrip, subj)
        if h in seen: continue
        if classify(subj, cat):
            new_anns.append({"symbol":scrip,"company":company,"subject":subj,
                             "category":cat,"exchange":"BSE"})
        seen[h] = now_ist().isoformat()

    print(f"\n  New announcements: {len(new_anns)}")
    if not new_anns:
        print("  Nothing new.")
        save_seen(seen); return

    new_anns.sort(key=lambda x: 0 if any(
        m in x["subject"].lower() for m in ["order","result","dividend","acquisition"]
    ) else 1)
    for a in new_anns:
        print(f"  [{a['exchange']}] {a.get('company',a['symbol'])}: {a['subject'][:70]}")

    msg = build_msg(new_anns, universe)
    if msg:
        ok = send_tg(msg)
        print(f"  Telegram: {'✅ sent' if ok else '⚠ failed'}")

    save_seen(seen)
    print("  Done.")

if __name__ == "__main__":
    main()
