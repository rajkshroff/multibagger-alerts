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
HOURLY_FILE  = REPO / "hourly_news_sent.json"    # per-hour dedup

# ── BSE CODE → NSE SYMBOL MAP ─────────────────────────────────
# identity_canonical.csv is copied to this repo by git_sync.cmd after each engine run
def _load_bse_nse_map() -> dict:
    """Build {bse_code_str: nse_symbol} from identity_canonical.csv."""
    m = {}
    p = REPO / "identity_canonical.csv"
    if not p.exists():
        print("  [identity] identity_canonical.csv not found — name matching only")
        return m
    try:
        import csv as _csv_id
        with open(p, encoding="utf-8", errors="replace") as _f_id:
            for row in _csv_id.DictReader(_f_id):
                bsc = str(row.get("BSE_CODE","") or "").strip().split(".")[0]
                nse = str(row.get("NSE_SYMBOL","") or "").strip().upper()
                if bsc and bsc.isdigit() and nse:
                    m[bsc] = nse
        print(f"  [identity] {len(m):,} BSE->NSE mappings loaded")
    except Exception as e:
        print(f"  [identity] {e}")
    return m

_BSE_NSE_MAP = _load_bse_nse_map()


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





def hourly_sent_this_hour() -> bool:
    n = now_ist()
    if not HOURLY_FILE.exists(): return False
    try:
        d = json.loads(HOURLY_FILE.read_text())
        return d.get("date") == n.strftime("%Y-%m-%d") and d.get("hour") == n.hour
    except: return False

def mark_hourly_sent():
    n = now_ist()
    HOURLY_FILE.write_text(json.dumps(
        {"date": n.strftime("%Y-%m-%d"), "hour": n.hour}, indent=2
    ))
    print(f"  [hourly] marked sent {n.strftime('%Y-%m-%d %H:00')}")

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
    """
    TYPE 1 — Action Plan. Fires on git push (engine just ran).
    Always sends even with 0 actionable stocks — BEAR market summary is informative.
    Tier-aware bucket: BUY/ACCUMULATE gated by tier in BEAR.
    Shows Q/G/S/C + Entry/SL per stock.
    Session 45: full redesign.
    """
    al = load("action")
    if al.empty:
        return ""

    al.columns = [c.strip() for c in al.columns]

    action_col = next((c for c in al.columns if c.upper() in
                       ("AI_ACTION","ACTION","ACTION_PLAN")), None)
    sym_col    = next((c for c in al.columns if c.upper() in ("NSE_SYMBOL","SYMBOL")), None)
    name_col   = next((c for c in al.columns if c.upper() in ("NAME","COMPANY")), None)
    tier_col   = next((c for c in al.columns if c.upper() in
                       ("MULTIBAGGER_TIER","TIER")), None)
    score_col  = next((c for c in al.columns if c.upper() in
                       ("COMPOSITE_SCORE","COMPOSITE_BALANCED","SIGNAL_TOTAL")), None)
    phase_col  = next((c for c in al.columns if c.upper() == "SECTOR_PHASE"), None)

    if not action_col or not sym_col:
        return ""

    al[action_col] = al[action_col].astype(str).str.strip()

    # ── Load composite for Q/G/S/C + BEATEN_DOWN_WATCH + entry/SL ─────────
    cs_ext = load("composite")
    cs_map = {}  # NSE_SYMBOL → dict
    if not cs_ext.empty:
        for _, _cr in cs_ext.iterrows():
            _sym = str(_cr.get("NSE_SYMBOL","")).strip().upper()
            if not _sym: continue
            cs_map[_sym] = {
                "q":  _si(_cr.get("Q_SCORE",0)),
                "g":  _si(_cr.get("G_SCORE",0)),
                "s":  _si(_cr.get("S_SCORE",0)),
                "c":  _si(_cr.get("C_SCORE",0)),
                "entry": str(_cr.get("ENTRY_ZONE","—")).strip(),
                "sl":    str(_cr.get("STOP_LOSS","—")).strip(),
                "beaten": bool(_cr.get("BEATEN_DOWN_WATCH", False)),
                "tier":  str(_cr.get("TIER","")).strip(),
            }

    _, mstate = market_summary()
    mstate2, mkt2 = market_summary()

    # ── Tier-aware bucket ─────────────────────────────────────────────────
    def bucket(row):
        a = str(row.get(action_col,"")).upper()
        t = str(row.get(tier_col, "")).upper() if tier_col else ""
        if "BEAR ACCUM" in a or "3X POTENTIAL" in a or "STAGED ACCUM" in a: return "BEAR_ACCUM"
        if "RECOVERY"   in a or "TRANCHE"      in a: return "RECOVERY"
        if "STRONG_BUY" in a or "STRONG BUY"   in a: return "STRONG_BUY"
        if "BUY" in a or "FRESH" in a:
            # In BEAR, only STRONG+ gets BUY; WLC gets BEAR_ACCUM
            if mstate2 == "BEAR" and t not in ("PRIME","STRONG"): return "BEAR_ACCUM"
            return "BUY"
        if "ACCUMULATE" in a or "ADD" in a:
            if mstate2 == "BEAR" and t not in ("PRIME","STRONG","WATCHLIST_CONFIRMED"): return None
            return "ACCUMULATE"
        return None

    # Add BEATEN_DOWN_WATCH stocks to RECOVERY
    _beaten = {sym for sym, d in cs_map.items() if d.get("beaten")}

    al["_B"] = al.apply(bucket, axis=1)
    if sym_col and _beaten:
        _bm = al[sym_col].str.upper().isin(_beaten) & al["_B"].isna()
        al.loc[_bm, "_B"] = "RECOVERY"

    act = al[al["_B"].notna()].copy()
    if score_col and score_col in act.columns:
        act[score_col] = pd.to_numeric(act[score_col], errors="coerce")
        act = act.sort_values(score_col, ascending=False)
    act = act.drop_duplicates(subset=[sym_col], keep="first")

    # ── Build message ─────────────────────────────────────────────────────
    now_str  = now_ist().strftime("%d %b %Y  %H:%M")  # IST fix Session 45
    mkt_icon = {"BULL":"🟢","BEAR":"🔴","CAUTION":"🟡"}.get(mstate2,"⚪")
    mi = load("market_intel")
    fg_str = ""
    if not mi.empty:
        fg_s = mi.iloc[0].get("FEAR_GREED_SCORE")
        fg_l = str(mi.iloc[0].get("FEAR_GREED_LABEL",""))
        if fg_s is not None:
            fg_str = f" | F&G: {fg_l} ({int(fg_s)})"
    b200 = ""
    if not mi.empty:
        bw = mi.iloc[0].get("BREADTH_ABOVE_200DMA")
        if bw is not None:
            b200 = f" | Above 200DMA: {bw:.1f}%"

    lines = [
        f"<b>🔔 ACTION PLAN — ENGINE RUN COMPLETE</b>",
        f"<b>{now_str}</b>  {mkt_icon} <b>{mstate2}</b>",
        f"{mkt_icon} {mstate2}{fg_str}{b200}",
        "",
    ]

    BEAR_ACCUM_MAX = 5   # Cap BEAR_ACCUM at top 5 (Session 45: 21 is too many)
    LABELS = {"STRONG_BUY":"🟢 STRONG BUY","BUY":"🟢 BUY",
               "ACCUMULATE":"🔵 ACCUMULATE","BEAR_ACCUM":"💜 BEAR ACCUM (top 5)",
               "RECOVERY":"🌱 RECOVERY WATCH"}

    import re as _re
    total = 0
    for b in ["STRONG_BUY","BUY","ACCUMULATE","BEAR_ACCUM","RECOVERY"]:
        grp = act[act["_B"]==b]
        if grp.empty: continue
        total += len(grp)
        lines.append(f"<b>{LABELS[b]}</b>  ({len(grp)})")
        _disp = grp.head(BEAR_ACCUM_MAX) if b == "BEAR_ACCUM" else grp.head(8)
        for _, row in _disp.iterrows():
            sym   = str(row.get(sym_col,"")).strip()
            nm    = str(row.get(name_col,""))[:16] if name_col else ""
            sc    = _si(row.get(score_col,0)) if score_col else 0
            phase = str(row.get(phase_col,"")).strip()[:12] if phase_col else ""
            ph_s  = f"[{phase}]" if phase and phase not in ("nan","None","—","") else ""
            cd    = cs_map.get(sym.upper(), {})
            q,g,s,c = cd.get("q",0),cd.get("g",0),cd.get("s",0),cd.get("c",0)
            er    = cd.get("entry","")
            sl    = cd.get("sl","")
            en    = _re.findall(r"₹([\d,]+)", er)
            e_s   = f"₹{en[0]}–{en[1]}" if len(en)>=2 else (f"₹{en[0]}" if en else "—")
            slm   = _re.search(r"₹([\d,]+(?:\.\d+)?)", sl)
            sl_s  = f"SL₹{slm.group(1)}" if slm else "—"
            lines.append(
                f"  • <code>{sym:<10}</code>{nm:<16} <b>{sc}</b> {ph_s}\n"
                f"       Q:{q} G:{g} S:{s} C:{c} | {e_s} | {sl_s}"
            )
        if len(grp) > 8:
            lines.append(f"  ... +{len(grp)-8} more")
        lines.append("")

    # Always show tier counts
    cs2 = load("composite")
    if not cs2.empty and "TIER" in cs2.columns:
        tc = cs2["TIER"].value_counts()
        lines.append(
            f"📊 PRIME <b>{tc.get('PRIME',0)}</b>  "
            f"STRONG <b>{tc.get('STRONG',0)}</b>  "
            f"WLC <b>{tc.get('WATCHLIST_CONFIRMED',0)}</b>  "
            f"LANDMINE <b>{tc.get('LANDMINE',0)}</b>"
        )

    # No actionable stocks — market summary still sent
    if total == 0:
        wlc_n = int(cs2["TIER"].isin(["WATCHLIST_CONFIRMED","WATCHLIST_EXTERNAL"]).sum()) if not cs2.empty else 0
        lines.append(f"\n  No BUY signals — deep {mstate2} market.")
        lines.append(f"  {wlc_n} watchlist stocks ready for sector recovery.")
        lines.append(f"  Check Recovery Watch in Excel for beaten-down quality stocks.")

    # ── Per-model section ─────────────────────────────────────────────────
    try:
        if not cs2.empty and "Q_SCORE" in cs2.columns:
            PW = {
                "BALANCED":           {"q":20,"g":15,"s":50,"c":15,"i":"⚖️"},
                "MULTIBAGGER_HUNTER": {"q":15,"g":15,"s":55,"c":15,"i":"🎯"},
                "SAFE_COMPOUNDER":    {"q":30,"g":20,"s":35,"c":15,"i":"🛡"},
                "DIVIDEND_INCOME":    {"q":30,"g":20,"s":35,"c":15,"i":"💰"},
                "SECTOR_SPECIALIST":  {"q":18,"g":15,"s":42,"c":25,"i":"🏭"},
            }
            Q_MAX,G_MAX,S_MAX,C_MAX = 20,15,50,15
            _qn = pd.to_numeric(cs2["Q_SCORE"],errors="coerce").fillna(0)/Q_MAX
            _gn = pd.to_numeric(cs2["G_SCORE"],errors="coerce").fillna(0)/G_MAX
            _sn = pd.to_numeric(cs2["S_SCORE"],errors="coerce").fillna(0)/S_MAX
            _cn = pd.to_numeric(cs2["C_SCORE"],errors="coerce").fillna(0)/C_MAX
            _at = ["PRIME","STRONG"]
            if mstate2 in ("BEAR","CAUTION"): _at.append("WATCHLIST_CONFIRMED")
            lines.append("\n<b>── TOP PICKS BY MODEL ──</b>")
            for prof, wts in PW.items():
                _ps = (_qn*wts["q"]+_gn*wts["g"]+_sn*wts["s"]+_cn*wts["c"]).round(1)
                _pc = cs2.copy(); _pc["_PS"] = _ps
                _tc2 = "TIER" if "TIER" in _pc.columns else None
                if _tc2:
                    _top = _pc[_pc[_tc2].isin(_at)].sort_values("_PS",ascending=False).head(3)
                else:
                    _top = _pc.sort_values("_PS",ascending=False).head(3)
                lines.append(f"  {wts['i']} <b>{prof.replace('_',' ').title()}</b>")
                if _top.empty:
                    lines.append(f"    — none in {'/'.join(_at)} tier")
                    continue
                for _, _pr in _top.iterrows():
                    _ps2  = str(_pr.get("NSE_SYMBOL","")).strip()
                    _psc  = int(_pr.get("_PS",0))
                    _pt   = str(_pr.get("TIER",""))[:6]
                    _pq   = _si(_pr.get("Q_SCORE",0)); _pg=_si(_pr.get("G_SCORE",0))
                    _pss  = _si(_pr.get("S_SCORE",0)); _pcc=_si(_pr.get("C_SCORE",0))
                    _pcd  = cs_map.get(_ps2.upper(),{})
                    _pen  = _re.findall(r"₹([\d,]+)", _pcd.get("entry",""))
                    _pes  = f"₹{_pen[0]}–{_pen[1]}" if len(_pen)>=2 else (f"₹{_pen[0]}" if _pen else "—")
                    _pslm = _re.search(r"₹([\d,]+(?:\.\d+)?)", _pcd.get("sl",""))
                    _psls = f"SL₹{_pslm.group(1)}" if _pslm else "—"
                    lines.append(
                        f"    • <code>{_ps2:<10}</code> <b>{_psc}</b> [{_pt}] "
                        f"Q:{_pq} G:{_pg} S:{_pss} C:{_pcc}\n"
                        f"         {_pes} | {_psls}"
                    )
    except Exception as _pme:
        lines.append(f"<i>[model breakdown error: {_pme}]</i>")

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
            buys = al[al[ac].str.contains("BUY|ACCUM", case=False, na=False)].copy()
            TIER_RANK = {"PRIME":0,"STRONG":1,"WL_CONFIRMED":2,"WATCHLIST_CONFIRMED":2,"WL_EXTERNAL":3}
            tc2 = next((c for c in al.columns if "TIER" in c.upper()), None)
            if tc2:
                buys["_tr"] = buys[tc2].map(TIER_RANK).fillna(5)
                buys = buys.sort_values(["_tr", sc], ascending=[True, False])
            else:
                buys = buys.sort_values(sc, ascending=False)
            for _, r in buys.head(7).iterrows():
                sym   = str(r.get("NSE_SYMBOL",""))
                tier  = str(r.get(tc2 if tc2 else "MULTIBAGGER_TIER",""))
                score = _si(r.get(sc,0))
                entry = (str(r.get("ENTRY_ZONE","") or "")).strip()
                sl    = (str(r.get("STOP_LOSS","") or "")).strip()
                act   = str(r.get(ac,""))[:35]
                tbadge = {"PRIME":"PRIME","STRONG":"STRONG"}.get(tier, tier.replace("WATCHLIST_CONFIRMED","WLC").replace("WL_CONFIRMED","WLC"))
                price_line = ("Entry:" + entry + " SL:" + sl if entry and entry not in ("-","nan") else "Run engine for price levels")
                picks.append("  " + sym.ljust(10) + "  " + tbadge.ljust(8) + "  " + str(score) + "/100\n    Action: " + act + "  " + price_line)

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
    # RBI RSS disabled — generic press releases, rarely stock-specific
    # {"name":"RBI", "url":"https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid=rss"},
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

# Noise filter — headlines containing these are ALWAYS skipped
# Checked FIRST before any keyword matching — noise overrides everything
NOISE_KEYWORDS = [
    # Global market noise — not India-stock-specific
    "south korea","hong kong","japan stocks","nikkei","dow jones","nasdaq","s&p 500",
    "wall street","european stocks","ftse","hang seng","kospi","shanghai",
    "global stock market","global market |","global market update",
    "five triggers","triggers that could move","week ahead","month ahead",
    # Geopolitical / macro noise irrelevant to Indian equities
    "iran","mideast","middle east","ukraine","russia","taiwan","israel","hamas",
    "crude oil price","brent crude","wti crude","oil price","oil shock",
    "fed rate cut","fed rate hike","bond market","bond yield","treasury yield",
    # Currency noise (too generic)
    "rupee hits record","rupee falls","rupee weakens","rupee at","rupee down",
    "dollar index","usd/inr","forex reserve",
    # Generic advice / strategy articles (not stock-specific)
    "how to build your portfolio","how to invest","wealth strategy","financial planning",
    "portfolio strategy","retirement planning","investment strategy",
    "invest in staggered","buy in staggered","staggered manner",
    "market wrap","what to expect","market outlook",
    "stocks to watch today","stocks in focus today","stocks to buy or sell",
    "10 shares in focus","stocks in focus","shares in focus",
    # IPO noise (not our holdings universe)
    "ipo day","ipo allotment","ipo subscription","ipo gmp","ipo price band",
    "sme ipo","mainboard ipo","listing today","listing price",
    # Broker generic lists
    "3 shares","5 shares","10 shares","top picks today",
    "penny stocks","cheap stocks","multibagger stock to buy",
    # Press release noise
    "press releases - reserve bank","press releases - sebi",
    # Analysis/opinion pieces — not actionable signals
    "sector to watch","stocks to watch","sectors to watch",
    "how to read","what investors should","what to do",
    "expert says","analyst says","here is why","here's why",
    "market today","opening bell","closing bell",
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

    # Session 34: reject noise FIRST — global/generic headlines add no value
    for noise in NOISE_KEYWORDS:
        if noise in hl:
            return False, "", ""

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
    # Require symbol >= 5 chars to avoid false positives ("ITC" in "critical" etc.)
    for sym in universe:
        if len(sym) >= 5 and sym.lower() in hl:
            return True, "UNIVERSE_STOCK", sym
        elif len(sym) == 4 and f" {sym.lower()} " in f" {hl} ":
            # 4-char symbols: require word boundaries
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




# ── LIVE PRICE + ATR HELPERS (catalyst scorer) ───────────────
def _nse_live_price(symbol: str) -> float:
    """Fetch live price from NSE. Returns 0.0 on failure."""
    try:
        import requests as _r
        s = _r.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0",
                          "Referer":"https://www.nseindia.com/",
                          "Accept":"application/json"})
        s.get("https://www.nseindia.com", timeout=5)
        r = s.get(f"https://www.nseindia.com/api/quote-equity?symbol={symbol}",
                  timeout=8)
        if r.ok:
            d = r.json()
            p = (d.get("priceInfo",{}).get("lastPrice") or
                 d.get("priceInfo",{}).get("close") or 0)
            return float(p) if p else 0.0
    except Exception:
        pass
    return 0.0

def _live_atr_pct(symbol: str, days: int = 14) -> float:
    """ATR as % of price. Same formula as engine. Default 2.5% on failure."""
    try:
        import requests as _r
        from datetime import datetime as _dt, timedelta as _td
        s = _r.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0",
                          "Referer":"https://www.nseindia.com/",
                          "Accept":"application/json"})
        s.get("https://www.nseindia.com", timeout=5)
        to_d   = now_ist().strftime("%d-%m-%Y")
        fr_d   = (_dt.now()-_td(days=days+5)).strftime("%d-%m-%Y")
        url    = (f"https://www.nseindia.com/api/historical/cm/equity"
                  f"?symbol={symbol}&series=[%22EQ%22]"
                  f"&from={fr_d}&to={to_d}&csv=false")
        r = s.get(url, timeout=10)
        if not r.ok: return 2.5
        rows = r.json().get("data",[])
        if len(rows) < 5: return 2.5
        trs = []
        for i in range(1, min(days+1, len(rows))):
            h  = float(rows[i].get("CH_TRADE_HIGH_PRICE",0) or 0)
            l  = float(rows[i].get("CH_TRADE_LOW_PRICE",0)  or 0)
            pc = float(rows[i-1].get("CH_CLOSING_PRICE",0)  or
                       rows[i].get("CH_CLOSING_PRICE",0)     or 0)
            if h>0 and l>0 and pc>0:
                trs.append(max(h-l, abs(h-pc), abs(l-pc)) / pc * 100)
        return sum(trs)/len(trs) if trs else 2.5
    except Exception:
        return 2.5

def _engine_age_hours(composite_df) -> float:
    """Hours since last engine run (from SCORED_AT in composite_scores.csv)."""
    try:
        from datetime import datetime as _dt2
        col = composite_df["SCORED_AT"].dropna() if "SCORED_AT" in composite_df.columns else None
        if col is None or col.empty: return 999.0
        latest = _dt2.fromisoformat(str(col.iloc[-1]).strip()[:19])
        return (_dt2.now() - latest).total_seconds() / 3600
    except Exception:
        return 999.0

def _confidence(age_h: float) -> tuple:
    """Returns (label, icon, use_live_prices)"""
    if   age_h <   6: return "FRESH",   "🟢", False
    elif age_h <  24: return "RECENT",  "🟡", True
    elif age_h < 168: return "STALE",   "🟠", True
    else:             return "EXPIRED", "🔴", True

# ══════════════════════════════════════════════════════════════
# CATALYST SCORER v2 — NOTIFICATION ONLY (Session 34)
#
# DESIGN PRINCIPLE: LOCAL ENGINE = SINGLE SOURCE OF TRUTH
#   The cloud NEVER scores independently.
#   It reads composite_scores.csv written by the local engine.
#   This guarantees cloud Telegram = local Excel. Always.
#
# What it does:
#   1. Polls BSE for new material announcements
#   2. Looks up existing engine score/tier/boundary from composite_scores.csv
#   3. Sends Telegram: event + engine context + boundary status
#   4. Tells user to run Quick Run for updated score
#
# Bull/Bear/Caution handling: automatic.
#   Local engine already applied correct regime thresholds.
#   Cloud just reads the result. Zero logic divergence.
# ══════════════════════════════════════════════════════════════

# Material keywords — order wins, results, material events
_CATALYST_MATERIAL_KW = [
    # Orders & contracts (biggest price movers)
    "order win","order worth","new order","secures order","contract awarded",
    "contract won","loa received","ppa signed","mou signed","project award",
    "letter of award","purchase order","work order","epc contract","repeat order",
    "export order","government contract","bags order",
    # Earnings
    "financial results","quarterly results","q1 result","q2 result",
    "q3 result","q4 result","annual result",
    # Corporate actions
    "dividend","bonus share","stock split","buyback","open offer","rights issue",
    # M&A
    "acquisition","merger","amalgamation","demerger","takeover",
    # Business events (UP)
    "drug approval","usfda approval","dcgi approval","patent granted",
    "capacity expansion","new plant","plant commissioned","pli scheme",
    "tariff hike","price hike","import duty","anti-dumping",
    # Regulatory/negative (DOWN)
    "sebi order","sebi penalty","ed raid","income tax raid",
    "fraud","default","insolvency","pledge invoked",
    "rating downgrade","ceo resign","md resign","cfo resign",
    "auditor resign","going concern","plant fire","factory fire",
    "force majeure","plant shutdown","contract cancelled","strike",
]

# Always skip — routine noise
_CATALYST_ROUTINE_SKIP = [
    "trading window","regulation 30","sebi (lodr)",
    "listing obligations","intimation of closure","new listing",
    "listing of securities",
    # SEBI SAST shareholding disclosures — contain word "acquisition" in regulation
    # name but are NOT actual acquisitions. Session 39 fix.
    "regulation 29","regulation 10(6)","regulation 10 (6)",
    "substantial acquisition of shares and takeovers",
    "sebi (substantial acquisition",
    "reg 29","reg. 29","intimation under regulation",
    # Other routine
    "postal ballot","scrutinizer report","closure of trading window",
    "record date","book closure","change in directorate",
    "resignation of","appointment of","re-appointment",
]

# Noisy categories — skip unless subject has material keyword
_CATALYST_NOISY_CATS = {"Insider Trading / SAST","Insider Trading","Company Update","Intimation"}

def _catalyst_is_material(subject: str, category: str) -> bool:
    """Return True if announcement is price-moving material."""
    subj = subject.lower()
    cat  = category.strip()
    # Routine skip always wins
    for r in _CATALYST_ROUTINE_SKIP:
        if r in subj:
            return False
    # Noisy category: require material keyword
    if cat in _CATALYST_NOISY_CATS:
        return any(kw in subj for kw in [
            "pledge invoked","acquisition","bulk deal","merger","order","fraud"
        ])
    # Material keyword check
    for m in _CATALYST_MATERIAL_KW:
        if m in subj:
            return True
    return False

def _catalyst_event_label(subject: str) -> str:
    """Extract short human-readable event label."""
    subj = subject.lower()
    import re
    if any(k in subj for k in ["order","contract","loa","ppa","epc","purchase order"]):
        m = re.search(r"(?:rs\.?|₹|inr)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)?",
                      subj, re.IGNORECASE)
        val = f" ₹{m.group(1)} Cr" if m else ""
        return f"Order Win{val}"
    if any(k in subj for k in ["result","revenue","profit","pat"]):
        return "Financial Results"
    if "dividend" in subj:  return "Dividend"
    if "bonus"    in subj:  return "Bonus Shares"
    if "split"    in subj:  return "Stock Split"
    if "buyback"  in subj:  return "Buyback"
    if any(k in subj for k in ["acquisition","merger","amalgam"]):
        return "Acquisition/Merger"
    if any(k in subj for k in ["sebi","ed raid","fraud","default","pledge invoked"]):
        return "⚠ Regulatory/Risk"
    if any(k in subj for k in ["fire","shutdown","force majeure","strike"]):
        return "⚠ Business Risk"
    if any(k in subj for k in ["drug approval","usfda","dcgi","patent"]):
        return "Drug/Patent Approval"
    if any(k in subj for k in ["capacity","new plant","commissioned"]):
        return "Capacity Expansion"
    return "Material Event"

def _fetch_bse_for_catalyst() -> list:
    """Fetch latest BSE announcements."""
    try:
        import requests as _req
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
               "?strCat=-1&strPrevDate=&strScrip=&strSearch=P&strToDate=&strType=C")
        r = _req.get(url, headers={"User-Agent":"Mozilla/5.0",
                                    "Referer":"https://www.bseindia.com/"}, timeout=12)
        if r.ok:
            d = r.json()
            return d if isinstance(d, list) else d.get("Table", []) or []
    except Exception as e:
        print(f"  [catalyst] BSE fetch error: {e}")
    return []


# ══════════════════════════════════════════════════════════════════
# MSG TYPE 4 — ⚡ LIVE ANNOUNCEMENT (cloud, every 5 min)
#
# Sends ALL material BSE announcements to user — PC does NOT need to be open.
# Covers every listed stock, not just universe stocks.
# Catalyst (below) handles universe stocks with full score+action plan.
# Dedup: "LIVE_" prefix in seen_hashes — separate from "S16_" catalyst hashes.
# ══════════════════════════════════════════════════════════════════

def send_bse_live_announcements(bse_raw: list, seen: dict) -> int:
    """
    Send ⚡ LIVE ANNOUNCEMENT for ALL material BSE events.
    Universe stocks also get basic tier/score from composite_scores.csv.
    Returns count of new alerts sent.
    """
    import html as _html_la
    try:
        # Load score map for context enrichment (best-effort)
        _score_map_la = {}
        cs_la = load("composite")
        if not cs_la.empty:
            for _, _row_la in cs_la.iterrows():
                _sym_la = str(_row_la.get("NSE_SYMBOL","")).strip().upper()
                if _sym_la:
                    _score_map_la[_sym_la] = {
                        "tier":  str(_row_la.get("TIER","")).strip(),
                        "score": _row_la.get("COMPOSITE_BALANCED",""),
                        "entry": str(_row_la.get("ENTRY_ZONE","")).strip(),
                        "sl":    str(_row_la.get("STOP_LOSS","")).strip(),
                    }
        # Load action language
        _actions_la = {}
        al_la = load("action_language")
        if not al_la.empty and "NSE_SYMBOL" in al_la.columns and "AI_ACTION" in al_la.columns:
            for _, _r_la in al_la.iterrows():
                _s_la = str(_r_la.get("NSE_SYMBOL","")).strip().upper()
                if _s_la: _actions_la[_s_la] = str(_r_la.get("AI_ACTION","") or "").strip()

        alerts_la = []
        for item in bse_raw[:50]:
            scrip   = str(item.get("SCRIP_CD","") or "").strip()
            subj    = str(item.get("HEADLINE","") or "").strip()
            cat     = str(item.get("CATEGORYNAME","") or "").strip()
            company = str(item.get("short_name","") or item.get("COMPANYNAME","") or "").strip()
            if not scrip or not subj: continue
            # Dedup with LIVE_ prefix (separate namespace from S16_ catalyst hashes)
            h = "LIVE_" + hex(abs(hash(f"{scrip}|{subj[:60]}")))[2:14]
            if h in seen: continue
            seen[h] = now_ist().isoformat()
            if not _catalyst_is_material(subj, cat): continue
            # Resolve company name from BSE map
            _nse_la = _BSE_NSE_MAP.get(scrip, "")
            _name_la = company or (
                _score_map_la.get(_nse_la, {}).get("name", "") if _nse_la else ""
            ) or f"BSE:{scrip}"
            alerts_la.append({
                "scrip": scrip, "nse": _nse_la,
                "name": _name_la, "subj": subj,
                "label": _catalyst_event_label(subj),
            })

        if not alerts_la:
            print("  [live-ann] No new material events")
            return 0

        now_s_la = now_ist().strftime("%H:%M IST, %d %b")
        lines_la = [f"\u26a1 <b>LIVE ANNOUNCEMENT \u2014 {now_s_la}</b>", ""]
        for ev in alerts_la[:8]:
            disp_la = _html_la.escape(ev["name"])
            lines_la.append(f"\U0001f6a8 <b>{disp_la}</b> <i>(BSE:{ev['scrip']})</i>")
            lines_la.append(f"  {_html_la.escape(ev['label'])} \u2014 {_html_la.escape(ev['subj'][:100])}")
            # Engine context for universe stocks
            _nse_la = ev["nse"]
            if _nse_la and _nse_la in _score_map_la:
                _inf_la = _score_map_la[_nse_la]
                _t_la   = _inf_la.get("tier","")
                _sc_la  = _inf_la.get("score","")
                _en_la  = _inf_la.get("entry","")
                _sl_la  = _inf_la.get("sl","")
                _ctx_la = []
                if _t_la and _t_la not in ("AVOID","MONITOR"):
                    try: _ctx_la.append(f"<b>{_t_la}</b> S:{int(float(str(_sc_la)))}")
                    except: _ctx_la.append(f"<b>{_t_la}</b>")
                if _en_la and "\u20b9" in _en_la:
                    _ep_la = _en_la.split("\u20b9")[1:]
                    _n1_la = _ep_la[0].split("\u2013")[0].strip().rstrip(",") if _ep_la else ""
                    _n2_la = _ep_la[1].strip().rstrip(",") if len(_ep_la) > 1 else ""
                    if _n1_la and _n2_la: _ctx_la.append(f"Entry \u20b9{_n1_la}\u2013{_n2_la}")
                if _sl_la and "\u20b9" in _sl_la:
                    _sl_n_la = _sl_la.split("\u20b9")[-1].split()[0].rstrip(",")
                    if _sl_n_la: _ctx_la.append(f"SL \u20b9{_sl_n_la}")
                _act_la = _actions_la.get(_nse_la,"")
                if _act_la and _act_la not in ("HOLD","AVOID","SKIP","MONITOR",""):
                    _ctx_la.append(f"\u2192 {_act_la}")
                if _ctx_la:
                    lines_la.append(f"  \U0001f4ca {' | '.join(_ctx_la)}")
            lines_la.append("")
        if len(alerts_la) > 8:
            lines_la.append(f"<i>+{len(alerts_la)-8} more</i>")
        lines_la.append(f"<i>Cloud poller \u00b7 {len(alerts_la)} new announcement(s)</i>")

        msg_la = "\n".join(lines_la)
        ok_la  = send(msg_la)
        print(f"  [live-ann] {len(alerts_la)} alert(s) sent: {ok_la}")
        return len(alerts_la)

    except Exception as _e_la:
        print(f"  [live-ann] Error: {_e_la}")
        return 0

def check_and_score_catalysts(bse_raw=None, seen=None):
    """
    Sends 🔔 CATALYST ALERT for universe stocks only (PRIME/STRONG/WL).
    Full score + entry/SL + AI_ACTION from engine ground truth.
    Accepts pre-fetched bse_raw and seen dict (shared with live announcements).
    NEVER scores independently. LOCAL ENGINE = SINGLE SOURCE OF TRUTH.
    """
    try:
        print("  [catalyst] Checking BSE for material events...")

        # Load composite_scores.csv — written by local engine, this is ground truth
        cs = load("composite")
        if cs.empty:
            print("  [catalyst] No composite_scores.csv — skipping")
            return

        # Build symbol → engine data map
        score_map = {}
        for _, row in cs.iterrows():
            sym = str(row.get("NSE_SYMBOL","")).strip().upper()
            if sym:
                score_map[sym] = {
                    "name":     str(row.get("NAME","")).strip(),
                    "tier":     str(row.get("TIER","")).strip(),
                    "score":    row.get("COMPOSITE_BALANCED",""),
                    "q":        row.get("Q_SCORE",""),
                    "boundary": str(row.get("BOUNDARY_FLAG","")).strip(),
                    "velocity": str(row.get("SCORE_VELOCITY","─")).strip(),
                    "action":   "",   # filled from action_language if available
                    "entry":    str(row.get("ENTRY_ZONE","")).strip(),
                    "sl":       str(row.get("STOP_LOSS","")).strip(),
                    "exp":      str(row.get("EXPECTED_RETURN","")).strip(),
                    "phase":    str(row.get("SECTOR_PHASE","")).strip(),
                    "mkt":      str(row.get("MARKET_STATE","")).strip(),
                    "signals":  row.get("SIGNALS_FIRED",""),
                }

        # Enrich with action language if available
        al = load("action_language")
        if not al.empty and "NSE_SYMBOL" in al.columns and "AI_ACTION" in al.columns:
            for _, row in al.iterrows():
                sym = str(row.get("NSE_SYMBOL","")).strip().upper()
                if sym in score_map:
                    score_map[sym]["action"] = str(row.get("AI_ACTION","")).strip()

        # Use pre-loaded seen dict if provided (shared dedup with live announcements)
        if seen is None:
            seen = load_seen()
        _owns_seen = (seen is None)  # only save if we loaded it ourselves

        # Use pre-fetched BSE data if provided (avoids double API call)
        if bse_raw is None:
            bse_raw = _fetch_bse_for_catalyst()
        if not bse_raw:
            print("  [catalyst] BSE returned 0 items")
            return
        print(f"  [catalyst] {len(bse_raw)} BSE items, checking {len(score_map)} universe stocks")

        events_to_alert = []
        _non_universe_events = []  # non-universe material events

        for item in bse_raw[:50]:
            scrip   = str(item.get("SCRIP_CD","") or "").strip()
            subj    = str(item.get("HEADLINE","") or "").strip()
            cat     = str(item.get("CATEGORYNAME","") or "").strip()
            company = str(item.get("short_name","") or item.get("COMPANYNAME","") or "").strip()
            if not scrip or not subj: continue

            # Dedup — S16 prefix avoids collision with corp_ann hashes
            h = "S16_" + hex(abs(hash(f"{scrip}|{subj[:60]}")))[2:14]
            if h in seen: continue
            seen[h] = now_ist().isoformat()

            if not _catalyst_is_material(subj, cat): continue

            # Match BSE code -> NSE symbol
            matched = None
            # Priority 1: direct BSE code lookup via identity_canonical
            _nse_sym = _BSE_NSE_MAP.get(scrip)
            if _nse_sym and _nse_sym in score_map:
                matched = (_nse_sym, score_map[_nse_sym])
            # Priority 2: company name prefix (8-char) fallback
            if matched is None:
                _cname = company.upper().replace(" LTD","").replace(" LIMITED","").strip()[:8]
                for sym, info in score_map.items():
                    _ename = info["name"].upper().replace(" LTD","").replace(" LIMITED","").strip()[:8]
                    if _cname and len(_cname) >= 4 and _cname == _ename:
                        matched = (sym, info)
                        break

            events_to_alert.append({
                "scrip":   scrip,
                "company": company,
                "subj":    subj,
                "label":   _catalyst_event_label(subj),
                "matched": matched,
            })

        # Save seen only if we own it (not when called from catalyst_only_mode)
        if seen is not None:
            save_seen(seen)

        if not events_to_alert:
            print("  [catalyst] No new material events")
            return

        # Build Telegram message
        import html as _html
        import re as _re
        now_s = now_ist().strftime("%H:%M IST, %d %b")
        lines = [f"🔔 <b>CATALYST ALERT — {now_s}</b>", ""]

        count = 0
        for ev in events_to_alert[:8]:
            scrip   = ev["scrip"]
            company = ev["company"]
            subj    = ev["subj"]
            label   = ev["label"]
            matched = ev["matched"]

            if matched is None:
                # Not in universe — collect for brief section at message end
                _non_universe_events.append(f"• {company}: {label}")
                continue

            sym, info = matched
            tier     = info["tier"]
            score    = info["score"]
            boundary = info["boundary"]
            action   = info["action"]
            entry    = info["entry"]
            sl       = info["sl"]
            phase    = info["phase"]
            mkt      = info["mkt"]
            velocity = info["velocity"]
            name     = info["name"]

            # Skip LANDMINE and AVOID — no action possible
            if tier in ("LANDMINE","AVOID"): continue

            # Score display
            try:    score_str = str(int(float(str(score))))
            except: score_str = "—"

            # Data freshness
            age_h = _engine_age_hours(cs)
            _conf_label, _conf_icon, _use_live = _confidence(age_h)
            broken_flag = False
            price_note  = ""

            entry_clean = "—"
            sl_clean    = "—"

            if _use_live:
                # Engine data >6h old — compute live entry/SL from NSE
                _lp = _nse_live_price(sym)
                if _lp > 0:
                    _atr   = _live_atr_pct(sym)  # FIX Session 39: was matched_sym (NameError)
                    _atr_a = _lp * _atr / 100
                    entry_clean = f"₹{_lp*(1-0.5*_atr/100):,.0f}–₹{_lp*(1+0.5*_atr/100):,.0f}"
                    sl_clean    = f"₹{_lp - 2*_atr_a:,.0f}"
                    # Safety: if price already below last known SL → broken
                    if sl and "₹" in sl:
                        try:
                            _old_sl = float(_re.search(r"₹([\d,]+)", sl).group(1).replace(",",""))
                            if _lp < _old_sl:
                                broken_flag = True
                        except Exception:
                            pass
                    price_note = f"{_conf_icon} Live prices (engine {int(age_h)}h old)"
                else:
                    # NSE unavailable — fallback to engine values
                    if entry and "₹" in entry:
                        _ns = _re.findall(r"₹([\d,]+)", entry)
                        entry_clean = f"₹{_ns[0]}–₹{_ns[1]}" if len(_ns)>=2 else f"₹{_ns[0]}" if _ns else "—"
                    if sl and "₹" in sl:
                        _sm = _re.search(r"₹([\d,]+)", sl)
                        if _sm: sl_clean = f"₹{_sm.group(1)}"
                    price_note = f"🟠 Engine values ({int(age_h)}h old — NSE unavailable)"
            else:
                # FRESH — use engine entry/SL directly
                if entry and "₹" in entry:
                    _ns = _re.findall(r"₹([\d,]+)", entry)
                    entry_clean = f"₹{_ns[0]}–₹{_ns[1]}" if len(_ns)>=2 else f"₹{_ns[0]}" if _ns else "—"
                if sl and "₹" in sl:
                    _sm = _re.search(r"₹([\d,]+)", sl)
                    if _sm: sl_clean = f"₹{_sm.group(1)}"

            # Action from engine (already in action_language.csv)
            # If empty, derive from tier + market
            if not action:
                if tier == "PRIME" and mkt != "BEAR":
                    action = "✅ BUY — engine aligned"
                elif tier == "PRIME":
                    action = "🟡 WAIT — PRIME quality, BEAR market"
                elif tier == "STRONG" and mkt == "BEAR":
                    action = "🟢 STAGED ACCUMULATE — build in thirds"
                elif tier == "STRONG":
                    action = "🟢 ACCUMULATE"
                elif tier == "WATCHLIST_CONFIRMED":
                    action = "👁 WATCH — approaching entry"
                else:
                    action = "👁 MONITOR"

            # Boundary context
            boundary_note = ""
            if boundary:
                if "PRIME" in boundary:
                    boundary_note = " 🎯 <b>Approaching PRIME!</b>"
                elif "STRONG" in boundary:
                    boundary_note = " 📈 Near STRONG boundary"

            # Velocity
            vel_note = ""
            if velocity.startswith("↑"):
                vel_note = f" | Score {velocity}"
            elif velocity.startswith("↓"):
                vel_note = f" | Score {velocity}"

            # Format signals count
            sigs = info.get("signals","")
            sigs_str = f"{int(float(str(sigs)))} signals" if sigs and str(sigs) not in ("","nan") else ""

            lines.append(
                f"\n🚨 <b>{_html.escape(name)}</b>\n"
                f"   {tier} | Score: <b>{score_str}/100</b>{' | '+sigs_str if sigs_str else ''}{boundary_note}\n"
                f"   📋 <b>{_html.escape(label)}</b>{vel_note}\n"
                f"   📌 {_html.escape(subj[:120])}\n"
                f"   💰 Entry: {entry_clean}  |  SL: {sl_clean}\n"
                f"   ⚡ {_html.escape(action)}\n"
                + (f"   ⚠️ <b>PRICE BELOW SL — broken setup</b>\n" if broken_flag else "")
            )
            count += 1

        if count == 0:
            print("  [catalyst] All events filtered or not in universe")
            return

        if len(events_to_alert) > 8:
            lines.append(f"<i>+{len(events_to_alert)-8} more events screened</i>")

        # Non-universe material events (compact — no score data)
        if _non_universe_events:
            lines.append("")
            lines.append(f"<i>📰 Other market events (not in engine universe):</i>")
            for ev_line in _non_universe_events[:5]:
                lines.append(f"<i>{ev_line}</i>")
            if len(_non_universe_events) > 5:
                lines.append(f"<i>+{len(_non_universe_events)-5} more</i>")

        lines.append("")
        lines.append(
            f"<i>Catalyst v2 | Engine truth | No PC needed | {count} universe event(s)</i>"
        )

        msg  = "\n".join(lines)
        parts = _split_message(msg)
        ok    = all(send(p) for p in parts)
        print(f"  [catalyst] Alert sent ({count} events, {len(parts)} msg(s)): {ok}")

    except Exception as e:
        print(f"  [catalyst] Error: {e}")
        import traceback; traceback.print_exc()


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
def main():
    h, m = h_m()
    now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
    triggered_by_push = os.environ.get("TRIGGERED_BY_PUSH","false").lower() == "true"

    # --catalyst-only: skip everything except catalyst scorer
    # Used by the dedicated 5-min GitHub Actions job
    import sys as _sys
    catalyst_only_mode = "--catalyst-only" in _sys.argv

    print(f"[{now_str}] cloud_alert_engine v3.0")
    print(f"  IST: {h:02d}:{m:02d} | push={triggered_by_push} | test={TEST_MODE} | catalyst_only={catalyst_only_mode}")

    if catalyst_only_mode:
        # One BSE fetch, shared seen — live announcements first, then catalyst
        print('  → MSG4: Live BSE Announcements (all material events)')
        _shared_seen = load_seen()
        _bse_data    = _fetch_bse_for_catalyst()
        if _bse_data:
            send_bse_live_announcements(_bse_data, _shared_seen)
            print('  → MSG5: Catalyst Alerts (universe stocks only)')
            check_and_score_catalysts(bse_raw=_bse_data, seen=_shared_seen)
        else:
            print('  [catalyst-only] BSE returned 0 items')
        save_seen(_shared_seen)
        return

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
        if not msg:
            # Build minimal market summary even with 0 actionable stocks
            # BEAR market with all AVOID actions is still informative
            from datetime import datetime
            mstate2, mkt2 = market_summary()
            mkt_icon2 = {"BULL":"🟢","BEAR":"🔴","CAUTION":"🟡"}.get(mstate2,"⚪")
            cs2 = load("composite")
            tc2 = cs2["TIER"].value_counts() if not cs2.empty and "TIER" in cs2.columns else {}
            msg = (f"<b>🔔 ACTION PLAN — ENGINE RUN COMPLETE</b>\n"
                   f"<b>{datetime.now().strftime('%d %b %Y  %H:%M')}</b>  "
                   f"{mkt_icon2} <b>{mstate2}</b>\n\n"
                   f"{mkt2.split(chr(10))[0]}\n\n"
                   f"📊 PRIME <b>{tc2.get('PRIME',0)}</b>  "
                   f"STRONG <b>{tc2.get('STRONG',0)}</b>  "
                   f"WLC <b>{tc2.get('WATCHLIST_CONFIRMED',0)}</b>  "
                   f"LANDMINE <b>{tc2.get('LANDMINE',0)}</b>\n\n"
                   f"  No BUY signals — deep BEAR market.\n"
                   f"  Engine watchlist: {tc2.get('WATCHLIST_CONFIRMED',0) + tc2.get('WATCHLIST_EXTERNAL',0)} stocks ready for sector recovery.\n"
                   f"  Check Recovery Watch in Excel for beaten-down quality stocks.\n"
                   f"<i>🤖 Engine run complete</i>")
        ok = send(msg)
        print(f"  Action Plan sent: {ok}")
        if ok: sent += 1
        return   # push trigger = action plan only, don't run news scan

    # ── TYPE 2: MORNING BRIEF (7:30am) ───────────────────────
    # Session 34: widened from 1hr to 2hr window
    # (7:30am–9:30am IST) so a missed cron doesn't lose the day
    in_brief_window = (h == 7 and m >= 30) or (h == 8) or (h == 9 and m < 30)
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
        if hourly_sent_this_hour():
            print(f"  → TYPE 3: already sent this hour ({h:02d}:xx) -- skip")
        else:
            print(f"  → TYPE 3: Hourly News (market hours)")
            msg = build_hourly_news()
            if msg:
                ok = send(msg)
                print(f"  News sent: {ok}")
                if ok:
                    mark_hourly_sent()
                    sent += 1
            else:
                print("  No new relevant news this hour")

    # Outside all windows
    if sent == 0 and not (9 <= h <= 18) and not in_brief_window:
        print(f"  → Outside windows ({h:02d}:{m:02d}) — silent")

    print(f"  Total sent: {sent}")


if __name__ == "__main__":
    main()
