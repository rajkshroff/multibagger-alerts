#!/usr/bin/env python3
# ============================================================
# send_action_plan_alert.py — v2.2
# SESSION 34: correct buckets + price guidance + tier filter
#
# Reads: action_language.csv + composite_scores.csv
#        + fundamentals_canonical.csv (for Current Price)
# Sends: 1 consolidated Telegram message, all 5 models
# ============================================================

import os, sys, requests, pandas as pd, re
from pathlib import Path
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

THIS    = Path(__file__).resolve().parent
EXT     = THIS / "external_lane" / "data_external"
INT     = THIS / "data_internal"
ROOT    = THIS.parent
RESULTS = ROOT / "USER_WORKSPACE" / "YOUR_RESULTS"

BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN","") or os.environ.get("TELEGRAM_TOKEN",""))
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID","")
MAX_CHUNK = 4000

BUY_ACTIONS = {
    "BUY","STRONG_BUY","FRESH_BUY","ACCUMULATE","ADD",
    "BEAR ACCUMULATE — 3X POTENTIAL","BEAR_ACCUM",
    "RECOVERY WATCH — TRANCHE BUY","RECOVERY_WATCH",
    "STAGED ACCUMULATE",
}

PROFILE_LABELS = {
    "BALANCED":          "⚖️  Balanced",
    "MULTIBAGGER_HUNTER":"🎯  Multibagger Hunter",
    "SAFE_COMPOUNDER":   "🛡️  Safe Compounder",
    "DIVIDEND_INCOME":   "💰  Dividend Income",
    "SECTOR_SPECIALIST": "🏭  Sector Specialist",
}

def send(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        print("  ⚠ Token/ChatID missing")
        return False
    chat_ids = [x.strip() for x in CHAT_ID.split(",") if x.strip()]
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    ok  = True
    for cid in chat_ids:
        for chunk in [text[i:i+MAX_CHUNK] for i in range(0, len(text), MAX_CHUNK)]:
            try:
                r = requests.post(url, json={"chat_id": cid, "text": chunk,
                                             "parse_mode": "HTML"}, timeout=15)
                if r.ok:
                    print(f"  [send→..{cid[-4:]}] ✅ {len(chunk)} chars", flush=True)
                else:
                    print(f"  [send→..{cid[-4:]}] HTTP {r.status_code}: {r.text[:100]}",
                          flush=True)
                    ok = False
            except Exception as e:
                print(f"  ⚠ send error: {e}")
                ok = False
    return ok

def _s(v, default=""):
    if v is None: return default
    if isinstance(v, float) and v != v: return default
    return str(v).strip()

def _he(v, default=""):
    import html as _html
    return _html.escape(_s(v, default))

def _si(v, default=0):
    try: return int(float(str(v)))
    except: return default

# ── LOAD DATA ─────────────────────────────────────────────────
print("\n── Telegram Action Plan Alert v2.2 ──", flush=True)

action_csv = EXT / "action_language.csv"
if not action_csv.exists():
    print("  ⚠ action_language.csv not found — skipping")
    sys.exit(0)

try:
    adf = pd.read_csv(action_csv, low_memory=False)
    adf.columns = [c.strip() for c in adf.columns]
except Exception as e:
    print(f"  ⚠ Cannot read action_language.csv: {e}")
    sys.exit(0)

# Composite scores — tier counts + ENTRY_ZONE + STOP_LOSS etc.
comp = pd.DataFrame()
comp_csv = INT / "composite_scores.csv"
if comp_csv.exists():
    try:
        comp = pd.read_csv(comp_csv, low_memory=False)
        comp.columns = [c.strip() for c in comp.columns]
    except Exception:
        pass

# Current Price — from fundamentals_canonical
price_map = {}
for fund_path in [INT / "fundamentals_canonical.csv",
                  INT / "fundamentals_with_guards.csv"]:
    if fund_path.exists():
        try:
            fd = pd.read_csv(fund_path, low_memory=False)
            fd.columns = [c.strip() for c in fd.columns]
            sym_c = next((c for c in fd.columns
                          if c.upper() in ("NSE CODE","NSE_SYMBOL","NSE_CODE")), None)
            if sym_c and "Current Price" in fd.columns:
                fd[sym_c] = fd[sym_c].astype(str).str.strip().str.upper()
                fd["Current Price"] = pd.to_numeric(fd["Current Price"], errors="coerce")
                price_map = dict(zip(fd[sym_c], fd["Current Price"].fillna(0)))
                print(f"  Price map: {len(price_map)} stocks from {fund_path.name}")
                break
        except Exception:
            pass

# Price guidance map from composite_scores
pg_map = {}  # {NSE_SYMBOL: {entry, sl, exp_ret, rr}}
if not comp.empty:
    cs_sym = next((c for c in comp.columns
                   if c.upper() in ("NSE_SYMBOL","SYMBOL")), None)
    if cs_sym:
        for _, cr in comp.iterrows():
            sym = str(cr.get(cs_sym,"")).strip().upper()
            if not sym: continue
            pg_map[sym] = {
                "entry":   _s(cr.get("ENTRY_ZONE","")),
                "sl":      _s(cr.get("STOP_LOSS","")),
                "exp_ret": _s(cr.get("EXPECTED_RETURN","")),
                "rr":      _s(cr.get("RISK_REWARD","")),
            }
    print(f"  Price guidance: {len(pg_map)} stocks from composite_scores")

# Market state
mkt_state = "—"
mi_csv = INT / "market_intelligence.csv"
if mi_csv.exists():
    try:
        mi = pd.read_csv(mi_csv, low_memory=False)
        mkt_state = str(mi.iloc[0].get("MARKET_STATE","—"))
    except Exception:
        pass

# Fear & Greed
fg_score = None
fg_label = ""
if mi_csv.exists():
    try:
        mi = pd.read_csv(mi_csv, low_memory=False)
        mi.columns = [c.strip() for c in mi.columns]
        fg_score = mi.iloc[0].get("FEAR_GREED_SCORE")
        fg_label = str(mi.iloc[0].get("FEAR_GREED_LABEL",""))
    except Exception:
        pass

action_col = next((c for c in adf.columns
                   if c.upper() in ("AI_ACTION","ACTION","ACTION_PLAN")), None)
sym_col    = next((c for c in adf.columns if c.upper() in ("NSE_SYMBOL","SYMBOL")), None)
name_col   = next((c for c in adf.columns if c.upper() in ("NAME","COMPANY")), None)
tier_col   = next((c for c in adf.columns
                   if "TIER" in c.upper() and "MULTI" in c.upper()), None)
score_col  = next((c for c in adf.columns
                   if c.upper() in ("COMPOSITE_SCORE","COMPOSITE_BALANCED")), None)

if not action_col or not sym_col:
    print(f"  ⚠ Required columns missing")
    sys.exit(0)

adf[action_col] = adf[action_col].astype(str).str.strip()

# ── BUCKET MAPPING v4.1 ───────────────────────────────────────
def _to_bucket(act: str, tier: str = "") -> str:
    a = act.upper()
    t = str(tier).strip().upper()
    # Hard skips — these are NOT actionable
    if any(x in a for x in ["WAIT","HOLD","AVOID","EXIT","MONITOR","WATCH CLOSELY",
                             "CYCLE CONFLICT","PRESERVE CAPITAL"]):
        return None
    # Actionable buckets
    if "BOOK PARTIAL" in a:                                         return "PROFIT_BOOK"
    if "BEAR ACCUM" in a or "3X POTENTIAL" in a or "STAGED ACCUM" in a: return "BEAR_ACCUM"
    if "RECOVERY" in a or "TRANCHE" in a:                          return "RECOVERY"
    if "STRONG_BUY" in a or "STRONG BUY" in a:                    return "STRONG_BUY"
    # BUY — must be PRIME or STRONG tier, not just any stock
    if "BUY" in a and t in ("PRIME","STRONG"):                     return "BUY"
    if "ACCUMULATE" in a and t in ("PRIME","STRONG","WATCHLIST_CONFIRMED"): return "ACCUMULATE"
    return None

actionable = adf.copy()
if tier_col:
    actionable["_BUCKET"] = actionable.apply(
        lambda r: _to_bucket(r[action_col], r.get(tier_col,"")), axis=1)
else:
    actionable["_BUCKET"] = actionable[action_col].apply(_to_bucket)
actionable = actionable[actionable["_BUCKET"].notna()].copy()

if score_col:
    actionable[score_col] = pd.to_numeric(actionable[score_col], errors="coerce")
    actionable = actionable.sort_values(score_col, ascending=False)

# ── UNIVERSE TIER COUNTS ──────────────────────────────────────
tier_counts = {}
if not comp.empty:
    tc = next((c for c in comp.columns if c.upper() == "TIER"), None)
    if tc:
        _d = comp.copy()
        if "EXCLUDED_GATE" in _d.columns:
            _pg = {"LANDMINE","HIGH_PLEDGING","PLEDGING"}
            _mask = (_d[tc]=="LANDMINE") & (~_d["EXCLUDED_GATE"].astype(str).str.upper().isin(_pg))
            _d.loc[_mask, tc] = "AVOID"
        tier_counts = _d[tc].value_counts().to_dict()

# ── BUILD MESSAGE ─────────────────────────────────────────────
now_str  = datetime.now().strftime("%d %b %Y  %H:%M")
mkt_icon = {"BULL":"🟢","BEAR":"🔴","CAUTION":"🟡"}.get(mkt_state,"⚪")
fg_str   = f" | F&G: {fg_label} ({int(fg_score)})" if fg_score is not None else ""

lines = [
    f"<b>🔔 MULTIBAGGER ENGINE — ACTION PLAN</b>",
    f"<b>📅 {now_str}</b>  |  {mkt_icon} Market: <b>{_he(mkt_state)}</b>{fg_str}",
    "",
]

if tier_counts:
    prime  = tier_counts.get("PRIME",0)
    strong = tier_counts.get("STRONG",0)
    wlc    = tier_counts.get("WL_CONFIRMED",tier_counts.get("WATCHLIST_CONFIRMED",0))
    land   = tier_counts.get("LANDMINE",0)
    lines.append(
        f"📊 PRIME <b>{prime}</b>  STRONG <b>{strong}</b>  "
        f"WLC <b>{wlc}</b>  MINE <b>{land}</b>"
    )
    lines.append("")

BUCKET_ORDER = ["PROFIT_BOOK","STRONG_BUY","BUY","ACCUMULATE","BEAR_ACCUM","RECOVERY"]
BUCKET_LABEL = {
    "PROFIT_BOOK":  "📈 BOOK PARTIAL PROFITS",
    "STRONG_BUY":   "🟢 STRONG BUY",
    "BUY":          "🟢 BUY",
    "ACCUMULATE":   "🔵 ACCUMULATE",
    "BEAR_ACCUM":   "💜 BEAR ACCUM / STAGED (3x Potential)",
    "RECOVERY":     "🌱 RECOVERY WATCH",
}

total_actionable = 0
for bucket in BUCKET_ORDER:
    grp = actionable[actionable["_BUCKET"] == bucket].copy()
    if grp.empty: continue
    if score_col and score_col in grp.columns:
        grp = grp.drop_duplicates(subset=[sym_col], keep="first")

    total_actionable += len(grp)
    lines.append(f"<b>{BUCKET_LABEL[bucket]}</b>  ({len(grp)})")

    for _, row in grp.head(10).iterrows():
        sym      = _he(row.get(sym_col,""))
        nm       = _he(row.get(name_col,""))[:18] if name_col else ""
        tier     = _he(row.get(tier_col,""))[:8] if tier_col else ""
        sc       = _si(row.get(score_col,0)) if score_col else 0
        sym_upper = str(row.get(sym_col,"")).strip().upper()

        # Price guidance
        cp  = price_map.get(sym_upper, 0.0)
        pg  = pg_map.get(sym_upper, {})

        cp_str = f"₹{cp:,.0f}" if cp and cp > 0 else "—"

        # Entry zone — extract ₹ range
        entry_raw = pg.get("entry","")
        if "₹" in entry_raw:
            nums = re.findall(r"₹([\d,]+)", entry_raw)
            entry_str = f"₹{nums[0]}–{nums[1]}" if len(nums)>=2 else f"₹{nums[0]}" if nums else "—"
        else:
            entry_str = "—"

        # Stop loss — extract ₹
        sl_raw = pg.get("sl","")
        sl_m = re.search(r"₹([\d,]+(?:\.\d+)?)", sl_raw)
        sl_str = f"SL₹{sl_m.group(1)}" if sl_m else "—"

        # Target — midpoint of expected return
        exp = pg.get("exp_ret","")
        exp_nums = re.findall(r"\d+", exp)
        if len(exp_nums) >= 2 and cp > 0:
            mid = (float(exp_nums[0]) + float(exp_nums[1])) / 2 / 100
            tgt_str = f"T₹{cp*(1+mid):,.0f}"
        else:
            tgt_str = "—"

        # R:R
        rr_raw = pg.get("rr","")
        rr_m   = re.search(r"(\d+\.?\d*:\d+)", rr_raw)
        rr_str = rr_m.group(1) if rr_m else ""

        price_line = f"{cp_str} | {entry_str} | {sl_str}"
        if tgt_str != "—": price_line += f" | {tgt_str}"
        if rr_str:         price_line += f" | R{rr_str}"

        lines.append(
            f"  • <code>{sym:<10}</code> {nm:<18} S:<b>{sc}</b> [{tier}]\n"
            f"       {price_line}"
        )

    if len(grp) > 10:
        lines.append(f"  ... +{len(grp)-10} more")
    lines.append("")

if total_actionable == 0:
    lines.append("  No actionable stocks — market in BEAR/EXTREME FEAR.")
    lines.append("  Next action: wait for sector recovery signals.")

# ── PER-MODEL PRIME/STRONG SECTION ──────────────────────────
# Reads composite_scores.csv and re-ranks by each profile's weights.
# Shows PRIME+STRONG stocks per model with Q/G/S/C scores.
try:
    PROFILE_WEIGHTS = {
        "BALANCED":          {"q":20,"g":15,"s":50,"c":15},
        "MULTIBAGGER_HUNTER":{"q":15,"g":15,"s":55,"c":15},
        "SAFE_COMPOUNDER":   {"q":30,"g":20,"s":35,"c":15},
        "DIVIDEND_INCOME":   {"q":30,"g":20,"s":35,"c":15},
        "SECTOR_SPECIALIST": {"q":18,"g":15,"s":42,"c":25},
    }
    PROFILE_ICONS = {
        "BALANCED":          "⚖️",
        "MULTIBAGGER_HUNTER":"🎯",
        "SAFE_COMPOUNDER":   "🛡️",
        "DIVIDEND_INCOME":   "💰",
        "SECTOR_SPECIALIST": "🏭",
    }
    PROFILE_FILTERS = {
        "SAFE_COMPOUNDER":   {"mcap_min": 500, "de_max": 0.3},
        "DIVIDEND_INCOME":   {"div_payout_min": 20.0},
        "MULTIBAGGER_HUNTER":{"wle_min_sig": 3},
        "SECTOR_SPECIALIST": {},
        "BALANCED":          {},
    }
    Q_MAX, G_MAX, S_MAX, C_MAX = 20, 15, 50, 15  # axis maxima in BALANCED

    if not comp.empty and "Q_SCORE" in comp.columns:
        # Normalise raw axis scores to 0-1
        _qn = pd.to_numeric(comp["Q_SCORE"], errors="coerce").fillna(0) / Q_MAX
        _gn = pd.to_numeric(comp["G_SCORE"], errors="coerce").fillna(0) / G_MAX
        _sn = pd.to_numeric(comp["S_SCORE"], errors="coerce").fillna(0) / S_MAX
        _cn = pd.to_numeric(comp["C_SCORE"], errors="coerce").fillna(0) / C_MAX
        _mcap = pd.to_numeric(comp.get("Market Capitalization", comp.get("MARKET_CAP", 0)), errors="coerce").fillna(0)
        _de   = pd.to_numeric(comp.get("Debt to equity", comp.get("DEBT_EQUITY", 999)), errors="coerce").fillna(999)
        _div  = pd.to_numeric(comp.get("Dividend Payout", comp.get("DIVIDEND_PAYOUT", 0)), errors="coerce").fillna(0)
        _sigs = pd.to_numeric(comp.get("SIGNALS_FIRED", 0), errors="coerce").fillna(0)

        # Load fundamentals for mcap/de/div filters
        _fund_extra = pd.DataFrame()
        _fund_p2 = INT / "fundamentals_with_guards.csv"
        if _fund_p2.exists():
            try:
                _fund_extra = pd.read_csv(_fund_p2, low_memory=False)
                _fe_sym = "NSE_SYMBOL" if "NSE_SYMBOL" in _fund_extra.columns else "NSE Code"
                _fund_extra = _fund_extra.rename(columns={_fe_sym: "NSE_SYMBOL"})
            except Exception: pass

        lines.append("")
        lines.append("<b>── PRIME &amp; STRONG BY MODEL ──</b>")

        for prof, wts in PROFILE_WEIGHTS.items():
            # Re-score using this profile's weights
            _prof_score = (
                _qn * wts["q"] +
                _gn * wts["g"] +
                _sn * wts["s"] +
                _cn * wts["c"]
            ).round(1)
            _pcomp = comp.copy()
            _pcomp["_PROF_SCORE"] = _prof_score

            # Apply profile filters
            flt = PROFILE_FILTERS.get(prof, {})
            if flt.get("mcap_min") and not _fund_extra.empty:
                try:
                    _mc_ser = pd.to_numeric(_fund_extra.set_index("NSE_SYMBOL").reindex(
                        _pcomp["NSE_SYMBOL"])["Market Capitalization"].values, errors="coerce")
                    _pcomp = _pcomp[pd.Series(_mc_ser, index=_pcomp.index).fillna(9999) >= flt["mcap_min"]]
                except Exception: pass
            if flt.get("de_max") and not _fund_extra.empty:
                try:
                    _de_ser = pd.to_numeric(_fund_extra.set_index("NSE_SYMBOL").reindex(
                        _pcomp["NSE_SYMBOL"])["Debt to equity"].values, errors="coerce")
                    _pcomp = _pcomp[pd.Series(_de_ser, index=_pcomp.index).fillna(0) <= flt["de_max"]]
                except Exception: pass
            if flt.get("wle_min_sig"):
                _sig_mask = pd.to_numeric(_pcomp.get("SIGNALS_FIRED", 0), errors="coerce").fillna(0) >= flt["wle_min_sig"]
                _pcomp = _pcomp[_sig_mask | _pcomp.get("TIER","").isin(["PRIME","STRONG"])]

            # Get PRIME+STRONG by profile score
            _tier_col2 = "TIER" if "TIER" in _pcomp.columns else None
            if _tier_col2:
                _ps = _pcomp[_pcomp[_tier_col2].isin(["PRIME","STRONG"])].sort_values(
                    "_PROF_SCORE", ascending=False)
            else:
                _ps = _pcomp.sort_values("_PROF_SCORE", ascending=False).head(5)

            _icon = PROFILE_ICONS.get(prof, "")
            _prof_label = PROFILE_LABELS.get(prof, prof)
            if _ps.empty:
                lines.append(f"\n{_icon} <b>{_prof_label}</b>  — none pass filters in {mkt_state}")
                continue

            lines.append(f"\n{_icon} <b>{_prof_label}</b>  ({len(_ps)} stocks)")
            for _, _pr in _ps.head(5).iterrows():
                _psym  = _he(str(_pr.get("NSE_SYMBOL","")))
                _pname = _he(str(_pr.get("NAME","")))[:16]
                _ptier = str(_pr.get("TIER",""))[:6]
                _psc   = int(_pr.get("_PROF_SCORE", 0))
                _pq    = int(float(str(_pr.get("Q_SCORE",0) or 0)))
                _pg2   = int(float(str(_pr.get("G_SCORE",0) or 0)))
                _ps2   = int(float(str(_pr.get("S_SCORE",0) or 0)))
                _pc    = int(float(str(_pr.get("C_SCORE",0) or 0)))
                lines.append(
                    f"  • <code>{_psym:<10}</code>{_pname:<16} "
                    f"<b>{_psc}</b> [{_ptier}] "
                    f"Q:{_pq} G:{_pg2} S:{_ps2} C:{_pc}"
                )
        lines.append("")
except Exception as _pme:
    lines.append(f"<i>[model breakdown unavailable: {_pme}]</i>")

lines.append(f"<i>🤖 Multibagger Engine v2.2 — {total_actionable} actionable | All 5 Models</i>")

msg = "\n".join(lines)
ok  = send(msg)

if ok:
    print(f"  ✅ Telegram sent v2.2 — {total_actionable} actionable stocks", flush=True)
else:
    print(f"  ⚠ Alert not sent", flush=True)
