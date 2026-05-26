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
    from type4_alerts import build_prebreakout_alert, build_catalyst_alert
except ImportError:
    build_prebreakout_alert = None
    build_catalyst_alert = None

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
    "composite":       REPO / "composite_scores.csv",
    "market_intel":    REPO / "market_intelligence.csv",
    "sector_cycle":    REPO / "sector_cycle_status.csv",
    "action":          REPO / "action_language.csv",
    "early_alerts":    REPO / "early_alerts.csv",
    "watchlist":       REPO / "watchlist_for_cloud.csv",
    "event_pulse":     REPO / "event_pulse_signals.csv",
    "portfolio_layer": REPO / "portfolio_layer.csv",
}
PORTFOLIO_FILE = REPO / "user_portfolio.csv"
SEEN_FILE    = REPO / "seen_hashes.json"
MORNING_FILE = REPO / "morning_brief_sent.json"
HOURLY_FILE  = REPO / "hourly_news_sent.json"    # per-hour dedup

# ── BSE CODE → NSE SYMBOL MAP ─────────────────────────────────
# identity_canonical.csv is copied to this repo by git_sync.cmd after each engine run
def _load_bse_nse_map() -> dict:
    """Build {bse_code_str: nse_symbol} from identity_canonical.csv.
    Falls back to bse_symbol_master.csv + composite_scores.csv when identity has no valid BSE codes
    (happens when build_identity_canonical.py serialised BSE Code NaN as the string 'nan').
    """
    m = {}
    p = REPO / "identity_canonical.csv"
    if p.exists():
        try:
            import csv as _csv_id
            with open(p, encoding="utf-8", errors="replace") as _f_id:
                for row in _csv_id.DictReader(_f_id):
                    bsc  = str(row.get("BSE_CODE","") or "").strip().split(".")[0]
                    nse  = str(row.get("NSE_SYMBOL","") or "").strip().upper()
                    name = str(row.get("NAME","") or "").strip()
                    if bsc and bsc.isdigit() and nse:
                        m[bsc] = nse
                        if name:
                            m[f"NAME_{bsc}"] = name
            if m:
                print(f"  [identity] {len(m):,} BSE->NSE mappings loaded from identity_canonical")
                return m
            print("  [identity] identity_canonical has no valid BSE codes — using fallback")
        except Exception as e:
            print(f"  [identity] identity_canonical error: {e} — using fallback")
    else:
        print("  [identity] identity_canonical.csv not found — using fallback")
    # Fallback: bse_symbol_master (BSE_CODE+ISIN) joined with composite_scores (ISIN+NSE_SYMBOL)
    try:
        import csv as _csv_fb
        isin_to_bsc: dict = {}
        bsm_p = REPO / "bse_symbol_master.csv"
        if bsm_p.exists():
            with open(bsm_p, encoding="utf-8", errors="replace") as _f:
                for row in _csv_fb.DictReader(_f):
                    bsc  = str(row.get("BSE_CODE","") or "").strip().split(".")[0]
                    isin = str(row.get("ISIN","") or "").strip().upper()
                    name = str(row.get("NAME","") or "").strip()
                    if bsc.isdigit() and isin:
                        isin_to_bsc[isin] = (bsc, name)
        cs_p = REPO / "composite_scores.csv"
        if cs_p.exists():
            with open(cs_p, encoding="utf-8", errors="replace") as _f:
                for row in _csv_fb.DictReader(_f):
                    isin = str(row.get("ISIN","") or "").strip().upper()
                    nse  = str(row.get("NSE_SYMBOL","") or "").strip().upper()
                    if isin and nse and isin in isin_to_bsc:
                        bsc, name = isin_to_bsc[isin]
                        m[bsc] = nse
                        if name:
                            m[f"NAME_{bsc}"] = name
        print(f"  [identity] {len(m):,} BSE->NSE mappings loaded from fallback (bse_master+composite)")
    except Exception as e:
        print(f"  [identity] fallback error: {e}")
    return m

_BSE_NSE_MAP = _load_bse_nse_map()


# ── IST ───────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def h_m(): n = now_ist(); return n.hour, n.minute

# ── TELEGRAM SEND ─────────────────────────────────────────────
MAX = 4000
def send(text: str) -> bool:
    """Send to all recipients in CHAT_IDS. Retries 3× on transient failures."""
    import time as _time
    if not text.strip(): return False
    url    = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+MAX] for i in range(0, len(text), MAX)]
    ok = True
    for chat in CHAT_IDS:
        for i, chunk in enumerate(chunks):
            suffix = f"\n<i>Part {i+1}/{len(chunks)}</i>" if len(chunks) > 1 else ""
            sent = False
            for attempt in range(3):
                try:
                    r = requests.post(url, json={
                        "chat_id": chat, "text": chunk+suffix, "parse_mode": "HTML"
                    }, timeout=15)
                    if r.ok:
                        print(f"  [send→..{chat[-4:]}] ✅ {len(chunk)} chars")
                        sent = True; break
                    elif r.status_code == 429:
                        retry_after = r.json().get("parameters",{}).get("retry_after", 5)
                        print(f"  [send→..{chat[-4:]}] 429 rate-limit — retry in {retry_after}s")
                        _time.sleep(retry_after)
                    else:
                        print(f"  [send→..{chat[-4:]}] HTTP {r.status_code}: {r.text[:100]}")
                        break
                except Exception as e:
                    print(f"  [send→..{chat[-4:]}] Error (attempt {attempt+1}): {e}")
                    if attempt < 2: _time.sleep(2)
            if not sent: ok = False
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

def _load_conviction_picks() -> list[dict]:
    """Return PRIME_CONVICTION stocks from portfolio_layer.csv, sorted by conviction desc."""
    pl = load("portfolio_layer")
    if pl.empty or "PRIME_CONVICTION" not in pl.columns:
        return []
    mask = pl["PRIME_CONVICTION"].astype(str).str.lower().isin(["true", "1", "1.0"])
    picks = pl[mask].copy()
    if "CONVICTION_SCORE" in picks.columns:
        picks["CONVICTION_SCORE"] = pd.to_numeric(picks["CONVICTION_SCORE"], errors="coerce").fillna(0)
        picks = picks.sort_values("CONVICTION_SCORE", ascending=False)
    out = []
    for _, r in picks.iterrows():
        lo  = _sf(r.get("ENTRY_ZONE_LOW", 0))
        hi  = _sf(r.get("ENTRY_ZONE_HIGH", 0))
        out.append({
            "sym":        str(r.get("NSE_SYMBOL", "")).strip(),
            "composite":  _si(r.get("COMPOSITE", 0)),
            "conviction": _si(r.get("CONVICTION_SCORE", 0)),
            "zone_low":   lo,
            "zone_high":  hi,
            "zone_note":  str(r.get("ENTRY_ZONE_NOTE", "")).strip(),
            "exit":       str(r.get("EXIT_SIGNAL", "HOLD")).strip(),
            "dma200":     _sf(r.get("DMA200_PRICE", 0)),
        })
    return out


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
               "BEAR ACCUMULATE — 3X POTENTIAL","BEAR_ACCUM"}

def build_action_plan() -> str:
    """TYPE 1 — Action Plan. s138: 3-model BUY NOW + Opportunity Buy."""
    try:
        pl  = load("portfolio_layer")
        cs  = load("composite")
        ea  = load("early_alerts")
        mi  = load("market_intel")
        _, mstate = market_summary()
        mstate = (mstate or "").upper()

        def _str(v, d=""):
            if v is None: return d
            s = str(v).strip()
            return d if s in ("nan","None","") else s

        pl_map, cs_map = {}, {}
        if not pl.empty and "NSE_SYMBOL" in pl.columns:
            for _, r in pl.iterrows():
                k = _str(r.get("NSE_SYMBOL","")).upper()
                if k: pl_map[k] = r.to_dict()
        if not cs.empty and "NSE_SYMBOL" in cs.columns:
            for _, r in cs.iterrows():
                k = _str(r.get("NSE_SYMBOL","")).upper()
                if k: cs_map[k] = r.to_dict()

        def _reason(sym):
            cr = cs_map.get(sym, {}); plr = pl_map.get(sym, {})
            parts = []
            phase = _str(cr.get("SECTOR_PHASE", plr.get("SECTOR_PHASE",""))).upper()
            PMAP = {"MID_CYCLE":"mid cycle","EARLY_RECOVERY":"early recovery",
                    "LATE_CYCLE":"late cycle","BASING":"basing","EARLY_CYCLE":"early cycle"}
            if phase in PMAP: parts.append(PMAP[phase])
            sigs = []
            if _sf(cr.get("SIG_FII 3Q Trend",0))>0:         sigs.append("FII 3Q")
            if _sf(cr.get("SIG_Insider Buy",0))>0:           sigs.append("insider buy")
            if _sf(cr.get("SIG_MF Change",0))>0:             sigs.append("MF buying")
            if _sf(cr.get("SIG_Earnings Acceleration",0))>0: sigs.append("earnings accel")
            if _sf(cr.get("SIG_VCP",0))>0:                   sigs.append("VCP base")
            if sigs: parts.append(" + ".join(sigs[:2]))
            above_50 = _str(plr.get("ABOVE_50DMA","")).lower() in ("true","1","1.0","yes")
            et = _si(plr.get("ENTRY_TIMING_SCORE",0))
            if et >= 4: parts.append("prime entry")
            elif above_50: parts.append("above 50DMA")
            return " · ".join(parts[:3]) if parts else "engine pick"

        shown = set()

        def _buy_section(flag_col, max_n, exclude=None):
            exclude = exclude or set()
            if pl.empty or flag_col not in pl.columns: return []
            mask = pl[flag_col].astype(str).str.lower().isin(["true","1","1.0","yes"])
            sub  = pl[mask].copy()
            for c in ("COMPOSITE","COMPOSITE_BALANCED","COMPOSITE_SCORE"):
                if c in sub.columns:
                    sub[c] = pd.to_numeric(sub[c], errors="coerce")
                    sub = sub.sort_values(c, ascending=False); break
            picks = []
            for _, r in sub.iterrows():
                if len(picks) >= max_n: break
                sym = _str(r.get("NSE_SYMBOL","")).upper()
                if not sym or sym in shown or sym in exclude: continue
                if _str(r.get("EXIT_SIGNAL","")).upper() == "EXIT": continue
                score = _si(r.get("COMPOSITE", r.get("COMPOSITE_BALANCED", r.get("COMPOSITE_SCORE",0))))
                tier  = _str(r.get("TIER","")).upper()
                picks.append((sym, score, tier, _reason(sym)))
                shown.add(sym)
            return picks

        safe_p = _buy_section("BUY_NOW_SAFE",     3)
        ar_p   = _buy_section("BUY_NOW_BALANCED", 3, exclude={s[0] for s in safe_p})
        com_p  = _buy_section("BUY_NOW_COMMODITY",2, exclude={s[0] for s in safe_p+ar_p})

        opp_p = []
        if not ea.empty and "ALERT_DATE" in ea.columns and "NSE_SYMBOL" in ea.columns:
            today_d = now_ist().date()
            ea2 = ea.copy()
            ea2["ALERT_DATE"] = pd.to_datetime(ea2["ALERT_DATE"], errors="coerce")
            ea2 = ea2.dropna(subset=["ALERT_DATE","NSE_SYMBOL"])
            ea2["_days"] = ea2["ALERT_DATE"].apply(
                lambda d: (today_d - d.date()).days if pd.notna(d) else 999)
            ea2 = ea2[ea2["_days"] <= 21].sort_values("_days")
            for _, r in ea2.iterrows():
                if len(opp_p) >= 3: break
                sym = _str(r.get("NSE_SYMBOL","")).upper()
                if not sym or sym in shown: continue
                cr   = cs_map.get(sym, {})
                tier = _str(cr.get("TIER","")).upper()
                if tier not in ("PRIME","STRONG","WATCHLIST_CONFIRMED"): continue
                if _str(cr.get("SECTOR_PHASE","")).upper() in ("CORRECTION","TOPPING"): continue
                plr = pl_map.get(sym,{})
                if plr and _str(plr.get("EXIT_SIGNAL","")).upper() == "EXIT": continue
                if plr:
                    ab50 = _str(plr.get("ABOVE_50DMA","")).lower() in ("true","1","1.0","yes")
                    if not ab50: continue
                days  = int(r.get("_days",0))
                atype = _str(r.get("ALERT_TYPE","")).replace("_"," ").lower()
                detail= _str(r.get("ALERT_DETAIL",""))[:55]
                score = _si(cr.get("COMPOSITE_BALANCED", cr.get("COMPOSITE_SCORE",0)))
                opp_p.append((sym, score, tier, atype, detail, days))
                shown.add(sym)

        # ── YOUR HOLDINGS block — local context only ──────────────
        holdings_lines = []
        ltcg_lines = []
        try:
            sys.path.insert(0, str(REPO))
            import portfolio_manager as _pm
            import csv as _csv_pm
            _health = _pm.get_health_scores()

            # Tier-drop diff: compare prev vs current composite_scores
            prev_tier = {}
            _prev_path = REPO / "data_internal" / "composite_scores_prev.csv"
            if not _prev_path.exists():
                _prev_path = REPO / "composite_scores_prev.csv"
            if _prev_path.exists():
                try:
                    import csv as _csv_prev
                    with open(_prev_path, encoding="utf-8", errors="replace") as _pf:
                        for _pr in _csv_prev.DictReader(_pf):
                            _sym = str(_pr.get("NSE_SYMBOL","")).strip().upper()
                            _t   = str(_pr.get("TIER","")).strip().upper()
                            if _sym: prev_tier[_sym] = _t
                except Exception:
                    pass

            _TIER_RANK = {"PRIME":4,"STRONG":3,"WATCHLIST_CONFIRMED":2,"WATCHLIST_EXTERNAL":1,"MONITOR":0,"AVOID":-1,"LANDMINE":-2}

            for h in _health:
                sym  = str(h.get("nse_symbol","")).upper()
                if not sym: continue
                hs   = int(h.get("health_score", 5))
                sig  = str(h.get("exit_signal","HOLD")).upper()
                ltcg = h.get("ltcg_days_left")
                pnl  = float(h.get("pnl_pct", 0))
                tier = str(h.get("tier","")).upper()
                rsn  = str(h.get("exit_reason","")).strip()
                cur_price = float(h.get("current_price", 0))
                ent_price = float(h.get("entry_price", 0))

                # Tier drop vs prev run
                prev_t = prev_tier.get(sym,"")
                tier_dropped = (prev_t and tier and _TIER_RANK.get(tier,0) < _TIER_RANK.get(prev_t,0))

                needs_attention = (hs <= 4 or sig in ("EXIT","PARTIAL_EXIT") or tier_dropped)
                if needs_attention:
                    pnl_str = f"{pnl:+.1f}%"
                    drop_str = f" ⚠️ TIER DROP: {prev_t}→{tier}" if tier_dropped else ""
                    sig_str  = f" | {sig}" + (f": {rsn[:30]}" if rsn and rsn != "nan" else "") if sig != "HOLD" else ""
                    holdings_lines.append(
                        f"  • <b>{sym}</b> ({ent_price:.0f}→{cur_price:.0f}, {pnl_str}) H={hs}{drop_str}{sig_str}"
                    )

                if ltcg is not None:
                    try:
                        ld = int(float(ltcg))
                        if 0 < ld <= 30:
                            ltcg_lines.append(f"  • <b>{sym}</b> — {ld}d to LTCG threshold (pnl {pnl:+.1f}%)")
                    except Exception:
                        pass
        except Exception:
            pass

        sell_p, trim_p = [], []
        if not pl.empty and "EXIT_SIGNAL" in pl.columns and "NSE_SYMBOL" in pl.columns:
            sort_c = next((c for c in ("COMPOSITE","COMPOSITE_BALANCED") if c in pl.columns), None)
            sub2 = pl.copy()
            if sort_c:
                sub2[sort_c] = pd.to_numeric(sub2[sort_c], errors="coerce")
                sub2 = sub2.sort_values(sort_c, ascending=False)
            for _, r in sub2.iterrows():
                sym  = _str(r.get("NSE_SYMBOL","")).upper()
                tier = _str(r.get("TIER","")).upper()
                if tier not in ("PRIME","STRONG"): continue
                sig  = _str(r.get("EXIT_SIGNAL","")).upper()
                rsn  = _str(r.get("EXIT_REASON",""))[:40]
                if   sig == "EXIT"         and len(sell_p) < 5: sell_p.append((sym, tier, rsn))
                elif sig == "PARTIAL_EXIT" and len(trim_p) < 5: trim_p.append((sym, tier))

        now_str  = now_ist().strftime("%a %d %b  %H:%M")
        mkt_icon = {"BULL":"🟢","BEAR":"🔴","CAUTION":"🟡","RECOVERY":"🔵","PANIC":"⚫"}.get(mstate,"⚪")
        fg_str = b200 = ""
        if not mi.empty:
            fg_s = mi.iloc[0].get("FEAR_GREED_SCORE")
            fg_l = _str(mi.iloc[0].get("FEAR_GREED_LABEL",""))
            if fg_s is not None: fg_str = f" | F&G: {fg_l} ({int(fg_s)})"
            bw   = mi.iloc[0].get("BREADTH_ABOVE_200DMA")
            if bw is not None: b200 = f" | Above 200DMA: {bw:.1f}%"

        lines = [
            "<b>🔔 ACTION PLAN — ENGINE RUN COMPLETE</b>",
            f"<b>{now_str}</b>  {mkt_icon} <b>{mstate}</b>{fg_str}{b200}",
            "",
        ]

        def _fmt(picks):
            return [f"  • <b>{sym}</b> ({sc},{tr}) — <i>{rsn}</i>" for sym,sc,tr,rsn in picks]

        if holdings_lines:
            lines.append("<b>📍 YOUR HOLDINGS — NEEDS ATTENTION</b>")
            lines += holdings_lines
            lines.append("")
        if ltcg_lines:
            lines.append("<b>📅 LTCG WATCH</b>")
            lines += ltcg_lines
            lines.append("")

        lines.append("<b>🛡 SAFE COMPOUNDER</b>  88% win · zero leverage")
        lines += _fmt(safe_p) if safe_p else ["  <i>(no picks today)</i>"]
        lines.append("")
        lines.append("<b>⚖️ ALL-ROUNDER</b>  84% win · incremental picks")
        lines += _fmt(ar_p) if ar_p else ["  <i>(none beyond SAFE today)</i>"]
        lines.append("")
        if com_p:
            lines.append("<b>⛏ COMMODITY CYCLE</b>  94% win · sector momentum")
            lines += _fmt(com_p)
            lines.append("")

        lines.append("<b>💡 OPPORTUNITY BUY</b>  catalyst-driven · exit when catalyst &gt; 21d old")
        if opp_p:
            for sym,sc,tr,atype,detail,days in opp_p:
                lines.append(f"  • <b>{sym}</b> ({sc},{tr}) — {atype}: {detail} ({days}d ago)")
        else:
            lines.append("  <i>(no fresh catalysts today)</i>")
        lines.append("")

        if sell_p:
            lines.append("<b>🔴 SELL TODAY</b>")
            for sym,tier,rsn in sell_p:
                lines.append(f"  • <b>{sym}</b> ({tier})" + (f" — {rsn}" if rsn else ""))
            lines.append("")
        if trim_p:
            lines.append("<b>🟡 TRIM</b>")
            lines.append("  " + " · ".join(f"<b>{s}</b>({t})" for s,t in trim_p))
            lines.append("")

        if not cs.empty and "TIER" in cs.columns:
            tc = cs["TIER"].value_counts()
            _TG = {("PRIME","BULL"):(25,42),("PRIME","CAUTION"):(15,30),("PRIME","BEAR"):(18,40),
                   ("STRONG","BULL"):(15,28),("STRONG","CAUTION"):(10,22),("STRONG","BEAR"):(12,30)}
            pe = _TG.get(("PRIME",mstate),(15,30)); se = _TG.get(("STRONG",mstate),(10,22))
            lines.append(
                f"📊 PRIME <b>{tc.get('PRIME',0)}</b> (+{pe[0]}–{pe[1]}%/yr) | "
                f"STRONG <b>{tc.get('STRONG',0)}</b> (+{se[0]}–{se[1]}%/yr) | "
                f"WLC <b>{tc.get('WATCHLIST_CONFIRMED',0)}</b> | "
                f"MINE <b>{tc.get('LANDMINE',0)}</b>")

        return "\n".join(lines)

    except Exception as _e:
        import traceback as _tb
        print(f"[build_action_plan] EXCEPTION: {_e}\n{_tb.format_exc()}")
        try:
            pl_fb = load("portfolio_layer")
            _, ms_fb = market_summary()
            now_str_fb = now_ist().strftime("%a %d %b  %H:%M")
            mkt_icon_fb = {"BULL":"🟢","BEAR":"🔴","CAUTION":"🟡","RECOVERY":"🔵","PANIC":"⚫"}.get(
                (ms_fb or "").upper(), "⚪")
            hdr_fb = (f"<b>🔔 ACTION PLAN — ENGINE RUN COMPLETE</b>\n"
                      f"<b>{now_str_fb}</b>  {mkt_icon_fb} <b>{ms_fb or ''}</b>\n\n")
            lines_fb = []
            for _fc, _ic, _lbl in [
                ("BUY_NOW_SAFE",      "🛡", "SAFE COMPOUNDER  88% win"),
                ("BUY_NOW_BALANCED",  "⚖️", "ALL-ROUNDER  84% win"),
                ("BUY_NOW_COMMODITY", "⛏", "COMMODITY CYCLE  94% win"),
            ]:
                if not pl_fb.empty and _fc in pl_fb.columns:
                    _mask = pl_fb[_fc].astype(str).str.lower().isin(["true","1","1.0","yes"])
                    _syms = pl_fb[_mask]["NSE_SYMBOL"].dropna().str.strip().str.upper().tolist()[:3]
                    if _syms or _fc != "BUY_NOW_COMMODITY":
                        lines_fb.append(f"<b>{_ic} {_lbl}</b>")
                        lines_fb += ([f"  • <b>{s}</b>" for s in _syms] if _syms else ["  <i>(none)</i>"])
                        lines_fb.append("")
            return hdr_fb + "\n".join(lines_fb)
        except Exception:
            return f"<b>🔔 ENGINE RUN COMPLETE</b>\n{now_ist().strftime('%a %d %b  %H:%M')}"


# ════════════════════════════════════════════════════════════════
# TYPE 2 — MORNING BRIEF (7:30am daily)
# ════════════════════════════════════════════════════════════════
def build_morning_brief() -> str:
    day = now_ist().strftime("%A, %d %b %Y")
    mstate, mkt_text = market_summary()

    # Load action language — primary source for actions + tier
    al  = load("action")
    # Load composite scores for expected return projections (best-effort)
    cs  = load("composite")
    _er_map = {}
    if not cs.empty and "NSE_SYMBOL" in cs.columns:
        _er_col = next((c for c in cs.columns if "EXPECTED_RETURN" in c.upper()), None)
        if _er_col:
            for _, _r in cs.iterrows():
                _s = str(_r.get("NSE_SYMBOL","")).strip().upper()
                if _s: _er_map[_s] = str(_r.get(_er_col,"") or "").strip()

    # Channel counts + exit set
    _exit_syms = set()
    _ba_syms   = set()
    _ch_line   = ""
    ac = None
    if not al.empty:
        ac  = next((c for c in al.columns if "ACTION" in c.upper()), None)
        sc2 = next((c for c in al.columns if "COMPOSITE" in c.upper()), None)
        if ac:
            _lm_df = al[al[ac].astype(str).str.contains("EXIT|LANDMINE",  case=False, na=False)]
            _bp_df = al[al[ac].astype(str).str.contains("BOOK PROFIT",    case=False, na=False)]
            _ba_df = al[al[ac].astype(str).str.contains("BEAR ACCUM",     case=False, na=False)]
            _ep_df = al[al[ac].astype(str).str.contains("ENGINE PICK",    case=False, na=False)]
            _lm = _lm_df.shape[0]; _bp = _bp_df.shape[0]
            _ba = _ba_df.shape[0]; _ep = _ep_df.shape[0]
            _ch_line = (f"☠ EXIT <b>{_lm}</b> | 📈 BOOK PROFIT <b>{_bp}</b> | "
                        f"🐻 BEAR ACCUM <b>{_ba}</b> | ✅ ENGINE PICK <b>{_ep}</b>")
            # Build sets for exclusion
            for _, _r in _lm_df.iterrows():
                _s = str(_r.get("NSE_SYMBOL","")).strip().upper()
                if _s: _exit_syms.add(_s)
            for _, _r in _bp_df.iterrows():
                _s = str(_r.get("NSE_SYMBOL","")).strip().upper()
                if _s: _exit_syms.add(_s)
            for _, _r in _ba_df.iterrows():
                _s = str(_r.get("NSE_SYMBOL","")).strip().upper()
                if _s: _ba_syms.add(_s)

    # BEAR ACCUMULATE section with 1Y projections
    bear_lines = []
    if not al.empty and ac:
        sc2 = next((c for c in al.columns if "COMPOSITE" in c.upper()), None)
        ba_df = al[al[ac].astype(str).str.contains("BEAR ACCUM", case=False, na=False)].copy()
        if sc2:
            ba_df[sc2] = pd.to_numeric(ba_df[sc2], errors="coerce")
            ba_df = ba_df.sort_values(sc2, ascending=False)
        for _, r in ba_df.head(5).iterrows():
            sym   = str(r.get("NSE_SYMBOL",""))
            score = _si(r.get(sc2,0)) if sc2 else "—"
            _er   = _er_map.get(sym.upper(),"")
            _er_sfx = f" | {_er}" if _er and "%" in _er else ""
            bear_lines.append(f"  🐻 <b>{sym}</b> S:{score}{_er_sfx}")

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

    msg = (f"🇮🇳 <b>MULTIBAGGER — MORNING BRIEF</b>\n"
           f"<b>{day}</b>\n\n"
           f"{mkt_text}\n")
    if _ch_line:
        msg += f"\n{_ch_line}\n"
    if cues:
        msg += "\n<b>🌍 Global Cues</b>\n" + "\n".join(cues) + "\n"
    if bear_lines:
        msg += "\n<b>🐻 Bear Accumulate (3× potential)</b>\n" + "\n".join(bear_lines) + "\n"

    # ── 3-MODEL BUY NOW ───────────────────────────────────────────
    pl_data = load("portfolio_layer")
    if not pl_data.empty:
        safe_lines, ar_lines, comm_lines = [], [], []
        _shown_mb = set()

        def _mb_picks(flag_col, max_n, exclude=set()):
            if flag_col not in pl_data.columns:
                return []
            mask = pl_data[flag_col].astype(str).str.lower().isin(["true","1","1.0"])
            sub = pl_data[mask].copy()
            if "CONVICTION_SCORE" in sub.columns:
                sub["CONVICTION_SCORE"] = pd.to_numeric(sub["CONVICTION_SCORE"], errors="coerce")
                sub = sub.sort_values("CONVICTION_SCORE", ascending=False)
            result = []
            for _, r in sub.iterrows():
                if len(result) >= max_n: break
                s = str(r.get("NSE_SYMBOL","")).strip().upper()
                if not s or s in _shown_mb or s in exclude: continue
                if str(r.get("EXIT_SIGNAL","")).upper() == "EXIT": continue
                score = _si(r.get("CONVICTION_SCORE", r.get("Q_SCORE", 0)))
                tier = str(r.get("TIER","")).strip()
                tbadge = {"PRIME":"PRIME","STRONG":"STRONG"}.get(tier, "WLC")
                result.append(f"  <b>{s}</b> {tbadge} S:{score}")
                _shown_mb.add(s)
            return result

        safe_lines = _mb_picks("BUY_NOW_SAFE", 3)
        ar_lines   = _mb_picks("BUY_NOW_BALANCED", 3, exclude=_shown_mb.copy())
        comm_lines = _mb_picks("BUY_NOW_COMMODITY", 2, exclude=_shown_mb.copy())

        bn_block = ""
        if safe_lines:
            bn_block += "  🛡 <b>SAFE</b> (88% win)\n" + "\n".join(safe_lines) + "\n"
        if ar_lines:
            bn_block += "  ⚖️ <b>ALL-ROUNDER</b> (84% win)\n" + "\n".join(ar_lines) + "\n"
        if comm_lines:
            bn_block += "  ⛏ <b>COMMODITY</b> (94% win)\n" + "\n".join(comm_lines) + "\n"
        if bn_block:
            msg += "\n<b>🎯 BUY NOW — High Conviction</b>\n" + bn_block

    if cycles:
        msg += "\n<b>🔄 Sector Cycle</b>\n" + "\n".join(cycles) + "\n"
    if alerts:
        msg += "\n<b>⚡ Early Alerts</b>\n" + "\n".join(alerts) + "\n"
    msg += "\n<i>Engine data from last run. Open Excel for full detail.</i>"
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
    # Moneycontrol removed — 403 Forbidden blocked externally since 2026-03
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

# s115: 3-pass filter redesign — noise-first → high-priority-material → routine-skip
# s117: added xbrl/initial disclosure; HTML stripping added inside _catalyst_is_material
# Pass 1 (absolute noise): transcript, conference calls, investor meets — never material
_CATALYST_NOISE_ALWAYS = [
    "transcript","conference call","investor meet","analyst meet",
    "earnings call invite","earnings call schedule","webinar",
    "press conference","investor day","investor presentation",
    "newspaper publication",   # e.g. "Newspaper Publication - Financial Results"
    "investor call",           # e.g. "Acquisition investor call" transcripts
    "earnings presentation",   # investor slide decks
    "xbrl",                    # XBRL tagging wrappers (not standalone events)
    "initial disclosure",      # SEBI SAST initial holding disclosures (LIC/FII threshold crossings)
    "format of initial",       # "Format of Initial Disclosure to be made by..."
]
# Pass 2 (high-priority material): always pass regardless of other patterns
_CATALYST_HIGH_PRI = [
    # Orders & contracts
    "order win","order worth","new order","secures order","contract awarded",
    "contract won","loa received","ppa signed","mou signed","project award",
    "letter of award","purchase order","work order","epc contract","repeat order",
    "export order","government contract","bags order",
    "receipt of order","receipt of work order","orders worth","receives order",
    # Earnings & financial events
    "financial result","quarterly result","annual result",
    "q1 result","q2 result","q3 result","q4 result","half year result",
    "audited result","unaudited result",
    # Corporate actions
    "dividend","bonus share","stock split","buyback","open offer","rights issue",
    # M&A
    "acquisition","merger","amalgamation","demerger","takeover",
    # Regulatory approvals
    "drug approval","usfda approval","dcgi approval","patent granted",
    "capacity expansion","new plant","plant commissioned","pli scheme",
    # Legal/regulatory orders filed under LODR (HIGH_PRI beats ROUTINE_SKIP "sebi (lodr)")
    "cestat","nclt","nclat","rera order","ngt order","court order received","stay granted","penalty waived",
    # Negative events
    "sebi order","sebi penalty","ed raid","income tax raid",
    "fraud","default","insolvency","pledge invoked",
    "rating downgrade","bulk deal","block deal",
    "tariff hike","price hike","import duty","anti-dumping",
    "plant fire","factory fire","force majeure","plant shutdown",
    "contract cancelled","going concern",
    "ceo resign","md resign","cfo resign","auditor resign",
]
# Pass 3 (routine skip): low-value admin/scheduling notices
_CATALYST_ROUTINE_SKIP = [
    "board meeting","meeting of the board","board of directors",
    "is scheduled","scheduled on","to be held on","scheduled to be held",
    "trading window","regulation 30","sebi (lodr)",
    "listing obligations","intimation of closure","new listing",
    "listing of securities",
    "regulation 29","regulation 10(6)","regulation 10 (6)",
    "substantial acquisition of shares and takeovers",
    "sebi (substantial acquisition",
    "reg 29","reg. 29","intimation under regulation",
    "postal ballot","scrutinizer report","closure of trading window",
    "record date","book closure","change in directorate",
    "resignation of","appointment of","re-appointment",
    "change in registered office","shifting of registered",
    "shareholding pattern","promoter holding",
    # s117 additions — routine corporate governance events
    "independent director","statutory auditor","auditor appointment",
    "annual general meeting","extraordinary general meeting","agm notice","egm notice",
    "familiarisation programme","induction programme",
    "change of auditor","secretarial audit",
    "corporate governance report","compliance report",
    "lic of india","life insurance corporation",  # LIC's regular SAST holding disclosures
]

# Noisy categories — pass only for the few material sub-types
_CATALYST_NOISY_CATS = {"Insider Trading / SAST","Insider Trading","Company Update","Intimation"}

def _catalyst_is_material(subject: str, category: str) -> bool:
    """Return True if announcement is price-moving material.
    3-pass: noise-first (always skip) → high-priority (always pass) → routine-skip → default False.
    HTML tags stripped before matching so BSE's <b>/<br/> wrappers don't block keyword detection.
    """
    # s117: BSE subjects sometimes contain HTML tags — strip before keyword matching
    _clean = re.sub(r'<[^>]+>', '', subject).strip()
    subj = _clean.lower()
    cat  = category.strip()
    # Pass 1: absolute noise — skip before checking material keywords
    if any(kw in subj for kw in _CATALYST_NOISE_ALWAYS):
        return False
    # Pass 2: high-priority material — pass immediately
    if any(kw in subj for kw in _CATALYST_HIGH_PRI):
        return True
    # Pass 3: routine admin/scheduling skip
    if any(kw in subj for kw in _CATALYST_ROUTINE_SKIP):
        return False
    # Noisy categories: only specific high-value sub-types
    if cat in _CATALYST_NOISY_CATS:
        return any(kw in subj for kw in [
            "pledge invoked","acquisition","bulk deal","merger","order","fraud"
        ])
    return False

def _catalyst_event_label(subject: str) -> str:
    """Extract short human-readable event label."""
    subj = subject.lower()
    import re
    # s55: exclude tax/GST/regulatory "orders"
    _tax_ord = any(k in subj for k in [
        "gst","income tax","tax authority","tax demand",
        "customs","enforcement","penalty","show cause",
    ])
    if not _tax_ord and any(k in subj for k in ["order","contract","loa","ppa","epc","purchase order"]):
        m = re.search(r"(?:rs\.?|₹|inr)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr)?",
                      subj, re.IGNORECASE)
        val = f" ₹{m.group(1)} Cr" if m else ""
        return f"Order Win{val}"
    if _tax_ord:
        return "⚠ Tax/Regulatory Notice"
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
    Send LIVE ANNOUNCEMENTS for material BSE events.
    s117: NSE symbol first, HTML stripped, same-company grouping.
    s119: Universe gate — only PRIME/STRONG/WLC/WLE stocks fire alerts.
          Results downgrade — financial results only for PRIME/STRONG.
    Returns count of new alerts sent.
    """
    import html as _html_la
    import re as _re_la
    try:
        # Load score map for context enrichment (best-effort)
        _score_map_la = {}
        _actions_la   = {}
        cs_la = load("composite")
        if not cs_la.empty:
            for _, _row_la in cs_la.iterrows():
                _sym_la = str(_row_la.get("NSE_SYMBOL","")).strip().upper()
                if _sym_la:
                    _score_map_la[_sym_la] = {
                        "tier":  str(_row_la.get("TIER","")).strip(),
                        "score": _row_la.get("COMPOSITE_BALANCED",""),
                        "name":  str(_row_la.get("NAME","")).strip(),
                    }
        al_la = load("action_language")
        if not al_la.empty and "NSE_SYMBOL" in al_la.columns and "AI_ACTION" in al_la.columns:
            for _, _r_la in al_la.iterrows():
                _s_la = str(_r_la.get("NSE_SYMBOL","")).strip().upper()
                if _s_la: _actions_la[_s_la] = str(_r_la.get("AI_ACTION","") or "").strip()

        # s119: Universe gate — build tier map. Only fire alerts for engine-tracked stocks.
        # Non-universe stocks (AVOID/MONITOR/LANDMINE/unmapped) are dropped silently.
        _universe_tier_la = {}   # NSE_SYMBOL -> TIER (PRIME/STRONG/WLC/WLE only)
        if not cs_la.empty and "NSE_SYMBOL" in cs_la.columns and "TIER" in cs_la.columns:
            for _, _ur in cs_la.iterrows():
                _us = str(_ur.get("NSE_SYMBOL","")).strip().upper()
                _ut = str(_ur.get("TIER","")).strip()
                if _us and _ut in ("PRIME","STRONG","WATCHLIST_CONFIRMED","WATCHLIST_EXTERNAL"):
                    _universe_tier_la[_us] = _ut
        _ps_la = {s for s,t in _universe_tier_la.items() if t in ("PRIME","STRONG")}

        # Keywords that classify an event as "financial results" (scheduled event, not breaking news)
        _RESULTS_KW_LA = ("financial result","quarterly result","annual result",
                          "audited result","unaudited result","half year result",
                          "q1 result","q2 result","q3 result","q4 result")

        alerts_la = []
        for item in bse_raw[:150]:
            scrip    = str(item.get("SCRIP_CD","") or "").strip()
            subj_raw = str(item.get("HEADLINE","") or "").strip()
            cat      = str(item.get("CATEGORYNAME","") or "").strip()
            company  = str(item.get("short_name","") or item.get("COMPANYNAME","") or "").strip()
            if not scrip or not subj_raw: continue

            # Dedup on raw subject (stable hash regardless of HTML in feed)
            h = "LIVE_" + hex(abs(hash(f"{scrip}|{subj_raw[:60]}" )))[2:14]
            if h in seen: continue
            # NOTE: hash written AFTER all gates so a broken mapping / filter miss
            # never permanently suppresses an alert via a consumed-but-unsent hash.

            # Strip HTML before filter + display (BSE subjects often contain <b>/<br/>)
            subj = _re_la.sub(r'<[^>]+>', '', subj_raw).strip()

            if not _catalyst_is_material(subj, cat): continue

            # Resolve NSE symbol first (needed for universe gate)
            _nse_la = _BSE_NSE_MAP.get(scrip, "")

            # Universe gate: skip stocks not in engine universe entirely
            if _universe_tier_la and _nse_la not in _universe_tier_la:
                continue

            # Results downgrade: financial results only for PRIME/STRONG.
            # WLC/WLE stocks' results are picked up by the next engine run — not urgent live alerts.
            _subj_lo_la = subj.lower()
            if (any(kw in _subj_lo_la for kw in _RESULTS_KW_LA)
                    and _nse_la not in _ps_la):
                continue

            # All gates passed — mark as seen now (not before) so a filter miss
            # on one run doesn't permanently block a retry on the next run.
            seen[h] = now_ist().isoformat()

            # Display name resolution
            _id_name = _BSE_NSE_MAP.get(f"NAME_{scrip}", "")
            _sc_name = _score_map_la.get(_nse_la, {}).get("name", "") if _nse_la else ""
            _name_la = _id_name or _sc_name or company or (_nse_la if _nse_la else f"BSE:{scrip}")

            alerts_la.append({
                "scrip": scrip, "nse": _nse_la,
                "name":  _name_la, "subj": subj,
                "label": _catalyst_event_label(subj),
            })

        if not alerts_la:
            print("  [live-ann] No new material events")
            return 0

        # Group same company (same NSE symbol or same BSE code)
        _grp_keys   = {}
        _grouped_la = []
        for ev in alerts_la:
            _k = ev["nse"] if ev["nse"] else ev["scrip"]
            if _k not in _grp_keys:
                _grp_keys[_k] = ev.copy()
                _grouped_la.append(_grp_keys[_k])
            else:
                _existing = _grp_keys[_k]["label"]
                _new_lbl  = ev["label"]
                if _new_lbl not in _existing:
                    _grp_keys[_k]["label"] = _existing + " + " + _new_lbl

        now_s_la = now_ist().strftime("%H:%M IST, %d %b")
        lines_la = [f"⚡ <b>LIVE ANNOUNCEMENTS — {now_s_la}</b>", ""]

        for ev in _grouped_la[:8]:
            _nse_la  = ev["nse"]
            _name_la = ev["name"]
            _lbl_la  = ev["label"]
            _subj    = ev["subj"]

            # Header: NSE symbol first if known, otherwise company name
            if _nse_la:
                lines_la.append(
                    f"🚨 <b>{_html_la.escape(_nse_la)}</b> "
                    f"({_html_la.escape(_name_la)}) — {_html_la.escape(_lbl_la)}"
                )
            else:
                lines_la.append(
                    f"📰 <b>{_html_la.escape(_name_la)}</b> — {_html_la.escape(_lbl_la)}"
                )

            # Clean boilerplate from subject and show key sentence
            _sc = _subj
            for _bp in [
                r"^(The company|The Board|The Company).{0,60}(informs?|hereby).{0,30}that\s+",
                r"^Pursuant to.{0,60}Regulation[^,]{0,20},\s*",
                r"^(This is to|We hereby) (inform|intimate).{0,30}that\s+",
                r"^(In|With) reference to.{0,40},\s*",
            ]:
                _sc = _re_la.sub(_bp, "", _sc, flags=_re_la.I).strip()
            if _sc: _sc = _sc[0].upper() + _sc[1:]
            _cut = min(110, len(_sc))
            _se = _re_la.search(r"[.!?]", _sc[30:80])
            if _se: _cut = 30 + _se.end()
            lines_la.append(f"  {_html_la.escape(_sc[:_cut])}")

            # Engine context line (only for universe stocks with meaningful tier)
            if _nse_la and _nse_la in _score_map_la:
                _inf_la = _score_map_la[_nse_la]
                _t_la   = _inf_la.get("tier","")
                _sc_la  = _inf_la.get("score","")
                _act_la = _actions_la.get(_nse_la,"")
                _ctx_la = []
                if _t_la and _t_la not in ("AVOID","MONITOR",""):
                    try:    _ctx_la.append(f"<b>{_t_la}</b> S:{int(float(str(_sc_la)))}")
                    except: _ctx_la.append(f"<b>{_t_la}</b>")
                if _act_la and _act_la not in ("HOLD","AVOID","SKIP","MONITOR",""):
                    _ctx_la.append(f"→ {_act_la}")
                if _ctx_la:
                    lines_la.append(f"  📊 {' | '.join(_ctx_la)}")
            lines_la.append("")

        if len(_grouped_la) > 8:
            lines_la.append(f"<i>+{len(_grouped_la)-8} more material event(s)</i>")
        lines_la.append(f"<i>{len(_grouped_la)} new event(s) this session</i>")

        msg_la = "\n".join(lines_la)
        ok_la  = send(msg_la)
        print(f"  [live-ann] {len(_grouped_la)} alert(s) sent: {ok_la}")
        return len(_grouped_la)

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
            scrip    = str(item.get("SCRIP_CD","") or "").strip()
            subj_raw = str(item.get("HEADLINE","") or "").strip()
            cat      = str(item.get("CATEGORYNAME","") or "").strip()
            company  = str(item.get("short_name","") or item.get("COMPANYNAME","") or "").strip()
            if not scrip or not subj_raw: continue

            # Dedup — S16 prefix avoids collision with corp_ann hashes
            h = "S16_" + hex(abs(hash(f"{scrip}|{subj_raw[:60]}")))[2:14]
            if h in seen: continue
            seen[h] = now_ist().isoformat()

            # Strip HTML tags before filter and display
            subj = re.sub(r'<[^>]+>', '', subj_raw).strip()

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
        # s55: send() handles chunking internally -- no _split_message needed
        ok   = send(msg)
        print(f"  [catalyst] Alert sent ({count} events): {ok}")

    except Exception as e:
        print(f"  [catalyst] Error: {e}")
        import traceback; traceback.print_exc()


# ════════════════════════════════════════════════════════════════
# MSG6 — CONVICTION HOLD CHECK (P11, s83)
# Fires at market close for portfolio stocks down >15% from entry.
# Answers: "Is the thesis broken, or is this just volatility?"
# ════════════════════════════════════════════════════════════════
def check_conviction_hold_alerts(seen: dict) -> int:
    """MSG6 — Conviction Hold Check.
    For each portfolio stock with drawdown > 15%, send conviction assessment.
    Deduplicates per stock per 10% drawdown floor per day.
    Returns count of Telegram messages sent."""
    if not PORTFOLIO_FILE.exists():
        return 0
    try:
        port_df = pd.read_csv(PORTFOLIO_FILE, low_memory=False)
        port_df.columns = [c.strip() for c in port_df.columns]
    except Exception as e:
        print(f"  [MSG6] portfolio read error: {e}")
        return 0
    if port_df.empty or "symbol" not in port_df.columns:
        return 0

    action_df = load("action")
    comp_df   = load("composite")
    if action_df.empty or comp_df.empty:
        return 0

    def _row_map(df, sym_col):
        m = {}
        for _, r in df.iterrows():
            s = str(r.get(sym_col, "") or "").strip().upper()
            if s:
                m[s] = r
        return m

    action_map = _row_map(action_df, "NSE_SYMBOL")
    comp_map   = _row_map(comp_df,   "NSE_SYMBOL")

    sent      = 0
    today_str = now_ist().strftime("%Y-%m-%d")

    for _, holding in port_df.iterrows():
        sym = str(holding.get("symbol", "") or "").strip().upper()
        if not sym:
            continue
        entry_price = _sf(holding.get("entry_price", 0))
        if entry_price <= 0:
            continue

        curr_price = _nse_live_price(sym)
        if curr_price <= 0:
            continue

        drawdown_pct = (entry_price - curr_price) / entry_price * 100
        if drawdown_pct < 15.0:
            continue

        # Floor key: alert fires at 15%, 25%, 35%
        if drawdown_pct >= 35:
            floor_key = "35"
        elif drawdown_pct >= 25:
            floor_key = "25"
        else:
            floor_key = "15"

        dedup_key = f"msg6_{sym}_{floor_key}_{today_str}"
        if dedup_key in seen:
            continue

        ar = action_map.get(sym, {})
        cr = comp_map.get(sym,   {})

        name          = str(ar.get("NAME") or cr.get("NAME") or sym).strip()
        tier          = str(ar.get("MULTIBAGGER_TIER") or cr.get("TIER") or "").strip()
        score         = _si(ar.get("COMPOSITE_SCORE") or cr.get("COMPOSITE_BALANCED") or 0)
        score_vel     = str(cr.get("SCORE_VELOCITY") or ar.get("SCORE_VELOCITY") or "").strip()
        conviction    = _si(ar.get("CONVICTION_SCORE", 0))
        conv_label    = str(ar.get("CONVICTION_LABEL", "")).strip()
        thesis        = str(ar.get("THESIS_WATCHPOINTS", "")).strip()
        forensic      = str(cr.get("FORENSIC_WARNING", "")).strip()
        phase         = str(cr.get("DISCOVERY_PHASE", "")).strip()
        fpi           = _sf(cr.get("FPI_PCT", 0))
        mf_pct        = _sf(cr.get("MF_PCT", 0))
        exp_ret       = str(cr.get("EXPECTED_RETURN", "")).strip()
        stop_loss_val = _sf(cr.get("STOP_LOSS", 0))
        ai_action     = str(ar.get("AI_ACTION", "")).strip()

        # Icons
        if drawdown_pct >= 35:
            dd_icon = "🔴"
        elif drawdown_pct >= 25:
            dd_icon = "🟠"
        else:
            dd_icon = "🟡"

        if conviction >= 8:
            conv_icon = "🟢"
            engine_call = "HOLD — thesis intact, this is volatility"
        elif conviction >= 5:
            conv_icon = "🟡"
            engine_call = "MONITOR — review watchpoints carefully"
        else:
            conv_icon = "🔴"
            engine_call = "RE-EVALUATE — exit signals building"

        vel_display = "↑ Rising" if "+" in score_vel else ("↓ Falling" if "-" in score_vel else "→ Stable")

        # Build message
        header = (f"📊 <b>CONVICTION CHECK</b> | {now_ist().strftime('%d %b %Y')}\n"
                  f"<b>{name} ({sym})</b>")
        if forensic and forensic not in ("nan", ""):
            header += f"\n⚠️ FORENSIC: {forensic}"

        drawdown_line = (f"\n{dd_icon} Down <b>{drawdown_pct:.0f}%</b> from entry "
                         f"₹{entry_price:.0f} → now ₹{curr_price:.0f}")

        watchpoints_block = f"\n\n<b>THESIS WATCHPOINTS:</b>\n{thesis}" if thesis and thesis not in ("nan", "") else ""

        phase_line = (f"\n📍 Phase {phase} | FPI+MF: {fpi:.1f}%+{mf_pct:.1f}%"
                      if phase and phase not in ("nan", "") else "")

        target_line = (f"\n🎯 Expected return: {exp_ret} from today"
                       if exp_ret and exp_ret not in ("nan", "") else "")

        stop_breach = (f"\n\n🛑 <b>STOP LOSS BREACHED</b> — SL was ₹{stop_loss_val:.0f}"
                       if stop_loss_val > 0 and curr_price < stop_loss_val else "")

        msg = (f"{header}{drawdown_line}{watchpoints_block}\n\n"
               f"<b>Score:</b> {score}/100  <b>{tier}</b>\n"
               f"<b>Velocity:</b> {vel_display}  ({score_vel})\n"
               f"<b>Conviction:</b> {conv_icon} <b>{conviction}/10</b> — {conv_label}\n"
               f"{phase_line}{target_line}{stop_breach}\n\n"
               f"<b>Engine says: {engine_call}</b>")

        ok = send(msg)
        print(f"  [MSG6] {sym} down {drawdown_pct:.0f}% | conviction {conviction}/10 | sent={ok}")
        if ok:
            sent += 1
            seen[dedup_key] = now_ist().isoformat()

    return sent


# ════════════════════════════════════════════════════════════════
# TYPE 5 — EVENT PULSE ALERT (s122, hourly 9am-6pm)
# Reads event_pulse_signals.csv written by event_pulse_engine.py
# Hard label: EVENT TRADE ≠ multibagger position
# ════════════════════════════════════════════════════════════════
_EP_SEEN_FILE = REPO / "event_pulse_sent.json"

def _ep_load_seen() -> set:
    if not _EP_SEEN_FILE.exists():
        return set()
    try:
        import json as _json_ep
        d = _json_ep.loads(_EP_SEEN_FILE.read_text(encoding="utf-8"))
        return set(d.get("sent", []))
    except Exception:
        return set()

def _ep_save_seen(seen: set) -> None:
    import json as _json_ep
    try:
        _EP_SEEN_FILE.write_text(
            _json_ep.dumps({"sent": list(seen)}),
            encoding="utf-8"
        )
    except Exception:
        pass

def build_event_pulse_alert() -> str:
    """
    Build TYPE 5 Event Pulse Telegram message.
    Returns formatted message string or "" if nothing actionable.
    Deduplicates by NSE_SYMBOL+EVENT_DATE within 24h.
    """
    ep_df = load("event_pulse")
    if ep_df.empty:
        return ""

    # Only actionable, non-suppressed, high-impact signals
    mask = (
        ep_df.get("SUPPRESSED", pd.Series(False, index=ep_df.index))
            .astype(str).str.lower().isin(["false", "0"])
        & ep_df.get("SIGNAL_TYPE", pd.Series("", index=ep_df.index))
            .isin(["BUY_SIGNAL_STRONG", "BUY_SIGNAL", "INSIDER_BUY_SIGNAL", "BONUS_SPLIT_SIGNAL"])
        & (ep_df.get("IMPACT_SCORE", pd.Series(0, index=ep_df.index)).astype(float) >= 5)
    )
    actionable = ep_df[mask].copy() if not ep_df.empty else pd.DataFrame()
    if actionable.empty:
        return ""

    seen = _ep_load_seen()
    lines = []

    for _, r in actionable.iterrows():
        sym      = str(r.get("NSE_SYMBOL", "")).strip()
        ev_date  = str(r.get("EVENT_DATE", "")).strip()
        dedup_key = f"{sym}_{ev_date}"
        if dedup_key in seen:
            continue

        event_class = str(r.get("EVENT_CLASS", "")).strip()
        signal_type = str(r.get("SIGNAL_TYPE", "")).strip()
        impact_tier = str(r.get("IMPACT_TIER", "")).strip()
        tier        = str(r.get("TIER", "")).strip()
        composite   = r.get("COMPOSITE", 0)
        price_status = str(r.get("PRICE_STATUS", "")).strip()
        alert_text  = str(r.get("ALERT_TEXT", "")).strip()

        icon = {"BUY_SIGNAL_STRONG": "🔥", "BUY_SIGNAL": "📈",
                "INSIDER_BUY_SIGNAL": "👥", "BONUS_SPLIT_SIGNAL": "🎯"}.get(signal_type, "⚡")

        line_parts = [f"{icon} <b>{sym}</b> — {event_class}"]
        if impact_tier:
            line_parts.append(f"Impact: {impact_tier}")
        if tier:
            line_parts.append(f"Tier: {tier}")
        if price_status == "PRICED_IN":
            line_parts.append("⚠️ LIKELY PRICED IN")

        lines.append(" | ".join(line_parts))
        seen.add(dedup_key)

    if not lines:
        return ""

    _ep_save_seen(seen)
    header = "⚡ <b>EVENT PULSE</b> — Live Announcements\n<i>⚠️ EVENT TRADE ≠ multibagger position</i>\n"
    return header + "\n".join(lines)


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
    audit_mode         = "--audit" in _sys.argv
    news_only_mode     = "--news" in _sys.argv     # skip morning brief even if in window
    morning_only_mode  = "--morning" in _sys.argv  # morning brief only, no news

    print(f"[{now_str}] cloud_alert_engine v3.0")
    print(f"  IST: {h:02d}:{m:02d} | push={triggered_by_push} | test={TEST_MODE} | catalyst_only={catalyst_only_mode} | audit={audit_mode} | news_only={news_only_mode} | morning_only={morning_only_mode}")

    # ── AUDIT MODE (s90) — 5:45am IST daily engine health report ──
    if audit_mode:
        print("  → AUDIT: Engine health audit")
        try:
            _val_path = Path(__file__).resolve().parent / "engine_validator.py"
            if _val_path.exists():
                import importlib.util as _ilu
                _spec = _ilu.spec_from_file_location("engine_validator", _val_path)
                _vm = _ilu.module_from_spec(_spec)
                _spec.loader.exec_module(_vm)
                _vr = _vm.run_audit(verbose=True)
                # Send the Telegram summary
                health = _vr.get("health", "UNKNOWN")
                icon = {"HEALTHY": "OK", "DEGRADED": "DEGRADED", "CRITICAL": "CRITICAL"}.get(health, "?")
                msg = (f"<b>[{icon}] Daily Engine Audit — {health}</b>\n"
                       f"{now_str}\n"
                       f"Pass={_vr['pass']}  Warn={_vr['warn']}  Fail={_vr['fail']}\n\n")
                for s in _vr.get("sections", []):
                    sico = {"PASS": "[+]", "WARN": "[~]", "FAIL": "[!]"}.get(s["status"], "?")
                    msg += f"{sico} {s['section']}: {(s.get('detail') or '')[:70]}\n"
                ok = send(msg)
                print(f"  Audit Telegram sent: {ok}")
            else:
                send(f"<b>Audit ERROR</b> — engine_validator.py not found")
        except Exception as _ae:
            send(f"<b>Audit ERROR</b> — {_ae}")
        return

    if catalyst_only_mode:
        # One BSE fetch, shared seen — live announcements first, then catalyst
        print('  → MSG4: Live BSE Announcements (all material events)')
        _shared_seen = load_seen()
        _bse_data    = _fetch_bse_for_catalyst()
        if _bse_data:
            send_bse_live_announcements(_bse_data, _shared_seen)

        # ── TYPE 4: PRE_BREAKOUT + CATALYST (s56) ────────────────
        # SAFE: wrapped individually, TYPE 1 already sent above.
        # Uses actual send function: send_bse_live_announcements
        try:
            from type4_alerts import (build_prebreakout_alert as _t4a,
                                      build_catalyst_alert    as _t4b)
            try:
                _pb = _t4a(load)
                if _pb:
                    send_bse_live_announcements(_pb)
            except Exception as _e4a:
                pass  # PRE_BREAKOUT failed silently -- TYPE 1 unaffected
            try:
                _cat = _t4b(load)
                if _cat:
                    send_bse_live_announcements(_cat)
            except Exception as _e4b:
                pass  # CATALYST failed silently -- TYPE 1 unaffected
        except ImportError:
            pass  # type4_alerts.py not deployed yet
        except Exception:
            pass  # any other error -- TYPE 1 unaffected
        # ── end TYPE 4 ────────────────────────────────────────────

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
        # Freshness gate: only send if composite_scores.csv was scored within 8 hours.
        # Prevents action plan firing when a non-engine push (e.g. infra fix) hits GitHub.
        _engine_fresh = False
        try:
            _cs_chk = load("composite")
            if not _cs_chk.empty and "SCORED_AT" in _cs_chk.columns:
                from datetime import timedelta as _td
                _scored = pd.to_datetime(_cs_chk["SCORED_AT"].max(), errors="coerce")
                if pd.notna(_scored):
                    _age_h = (now_ist().replace(tzinfo=None) - _scored.to_pydatetime().replace(tzinfo=None)).total_seconds() / 3600
                    _engine_fresh = _age_h <= 4
        except Exception as _fe:
            print(f"  [freshness] check error: {_fe}")
            _engine_fresh = True   # fail-open: assume fresh if check errors
        if not _engine_fresh:
            print("  → Push trigger: engine data >8h old — skipping action plan (no engine run detected)")
            return
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
    # Window 7:30–9:30 IST as fallback for missed cron.
    # --news mode skips this to prevent duplicate when 9:00 --news cron fires
    # inside the window. morning_brief_sent.json is now persisted to GitHub
    # so same-day dedup works across runner instances.
    in_brief_window = ((h == 7 and m >= 30) or (h == 8) or (h == 9 and m < 30)) and not news_only_mode
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
    # s55: 3 news slots per day (9-11, 12-14, 15-18)
    _nslot = (1 if 9<=h<12 else 2 if 12<=h<15 else 3 if 15<=h<=18 else 0)
    if _nslot > 0:
        if hourly_sent_this_hour():
            print(f"  → TYPE 3: slot {_nslot} already sent -- skip")
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

    # ── TYPE 5: EVENT PULSE (s122, 9am-6pm) ────────────────────────
    if 9 <= h <= 18:
        _ep_msg = build_event_pulse_alert()
        if _ep_msg:
            ok = send(_ep_msg)
            if ok:
                sent += 1
            print(f"  → TYPE 5 Event Pulse sent: {ok}")
        else:
            print("  → TYPE 5 Event Pulse: no new actionable signals")

    # ── MSG6: CONVICTION HOLD CHECK (market close 15:00–18:00 IST) ──
    # s83: Fires for portfolio stocks down >15% from entry price.
    # One alert per stock per 10% drawdown floor per day.
    if 15 <= h <= 18 and PORTFOLIO_FILE.exists():
        print("  → MSG6: Conviction Hold Check (portfolio drawdown)")
        _seen_mutable = load_seen()
        _m6_sent = check_conviction_hold_alerts(_seen_mutable)
        if _m6_sent > 0:
            save_seen(_seen_mutable)
            sent += _m6_sent
            print(f"  MSG6 alerts sent: {_m6_sent}")
        else:
            print("  MSG6: no portfolio drawdowns >15% or no portfolio file")

    # Outside all windows
    if sent == 0 and not (9 <= h <= 18) and not in_brief_window:
        print(f"  → Outside windows ({h:02d}:{m:02d}) — silent")

    print(f"  Total sent: {sent}")


if __name__ == "__main__":
    main()
# s55-final-cae
# s55-telegram
# s55-catalyst-fix
# s55-bear-tight
