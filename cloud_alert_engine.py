#!/usr/bin/env python3
# ============================================================
# cloud_alert_engine.py — v2.0
# SESSION 39: Morning brief + FII/DII + Global cues (yfinance)
#
# LOCATION: Multibagger_App/multibagger-alerts/cloud_alert_engine.py
# TRIGGERED BY: GitHub Actions (alert_engine.yml)
#   - Morning Brief: 8:30am IST daily (3:00 UTC)
#   - Intraday Alerts: every 30min, 7am–8pm IST, 7 days
#
# SESSION 39 ADDITIONS:
#   - FII/DII flow (NSEI breadth proxy via yfinance)
#   - Global cues: Dow/Nasdaq/SGX Nifty/Crude/USDINR
#   - SECTOR_TURN alerts forwarded from early_alerts.csv
#   - Market breadth context in morning brief
#
# DATA SOURCES (from GitHub repo root — git_sync.cmd pushes these):
#   composite_scores.csv     — tier + signal per stock
#   market_intelligence.csv  — breadth + fear/greed
#   sector_cycle_status.csv  — sector phases
#   watchlist_for_cloud.csv  — PRIME/STRONG/BeatDown stocks
#   action_language.csv      — BUY/ACCUMULATE actions
#   early_alerts.csv         — pre-price signals + SECTOR_TURN
# ============================================================

import os
import sys
import requests
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    os.system("pip install pandas -q")
    import pandas as pd

try:
    import yfinance as yf
except ImportError:
    os.system("pip install yfinance -q")
    import yfinance as yf

# ── TELEGRAM CONFIG ──────────────────────────────────────────
BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "")

if not BOT_TOKEN or not CHAT_ID:
    print("❌ TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
    sys.exit(1)

# ── PATHS ─────────────────────────────────────────────────────
REPO      = Path(__file__).resolve().parent
CSV_FILES = {
    "composite":    REPO / "composite_scores.csv",
    "market_intel": REPO / "market_intelligence.csv",
    "sector_cycle": REPO / "sector_cycle_status.csv",
    "watchlist":    REPO / "watchlist_for_cloud.csv",
    "action":       REPO / "action_language.csv",
    "early_alerts": REPO / "early_alerts.csv",
}

# ── IST HELPERS ───────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def ist_hour_min():
    n = now_ist()
    return n.hour, n.minute

def is_morning_brief():
    """True if we're in the 8:25–8:35am IST window."""
    h, m = ist_hour_min()
    return h == 8 and 25 <= m <= 35

def is_market_hours():
    """True if 7am–8pm IST (inclusive)."""
    h, _ = ist_hour_min()
    return 7 <= h <= 20


# ── CSV LOADER ────────────────────────────────────────────────
def _load(key):
    p = CSV_FILES.get(key)
    if not p or not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, low_memory=False)
    except Exception:
        return pd.DataFrame()

def _sf(v, default=0.0):
    try:
        return float(v) if str(v) not in ("nan","None","","NaT") else default
    except Exception:
        return default

def _si(v, default=0):
    try:
        return int(float(str(v))) if str(v) not in ("nan","None","") else default
    except Exception:
        return default


# ── TELEGRAM SENDER ──────────────────────────────────────────
def send(msg: str, parse_mode: str = "HTML") -> bool:
    """Send a message to Telegram. Returns True on success."""
    if not msg.strip():
        return False
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": parse_mode}
    try:
        r = requests.post(url, json=data, timeout=15)
        if r.status_code == 200:
            return True
        print(f"[Telegram] HTTP {r.status_code}: {r.text[:200]}")
        return False
    except Exception as e:
        print(f"[Telegram] Error: {e}")
        return False


# ── GLOBAL CUES (yfinance) ────────────────────────────────────
GLOBAL_TICKERS = {
    "Dow Jones":   "^DJI",
    "Nasdaq":      "^IXIC",
    "GIFT Nifty":  "GC=F",    # fallback; use SGX proxy
    "Crude WTI":   "CL=F",
    "USD/INR":     "USDINR=X",
}

def fetch_global_cues() -> list[str]:
    """
    Returns list of formatted lines like:
    🟢 Dow Jones      42,150.30  (+0.42%)
    """
    lines = []
    try:
        for label, ticker in GLOBAL_TICKERS.items():
            try:
                info  = yf.Ticker(ticker).fast_info
                price = float(info.get("last_price", 0) or 0)
                prev  = float(info.get("previous_close", 0) or 0)
                if price <= 0:
                    continue
                chg   = price - prev if prev else 0
                pct   = chg / prev * 100 if prev else 0
                icon  = "🟢" if pct >= 0 else "🔴"
                lines.append(f"{icon} <b>{label:<12}</b> {price:>12,.2f}  ({pct:+.2f}%)")
            except Exception:
                continue
    except Exception:
        pass
    return lines


# ── FII/DII PROXY (breadth approach via yfinance) ─────────────
def fetch_fii_proxy() -> str:
    """
    FII/DII actual data is not available freely in real-time.
    Uses NIFTY 50 vs NIFTY Midcap 100 divergence as a proxy
    for FII (large-cap) vs DII (mid-cap) flow direction.
    """
    try:
        nifty  = yf.Ticker("^NSEI").fast_info
        midcap = yf.Ticker("NIFTY_MIDCAP_100.NS").fast_info
        n_pct  = round((float(nifty.get("last_price",0)) / float(nifty.get("previous_close",1)) - 1) * 100, 2)
        m_pct  = round((float(midcap.get("last_price",0)) / float(midcap.get("previous_close",1)) - 1) * 100, 2)
        fii_sig = "FII-driven 📈" if n_pct > m_pct + 0.3 else ("DII-driven 📈" if m_pct > n_pct + 0.3 else "Balanced")
        return f"NIFTY50 {n_pct:+.2f}% vs MidCap {m_pct:+.2f}% → {fii_sig}"
    except Exception:
        return "FII/DII data unavailable"


# ── MORNING BRIEF ─────────────────────────────────────────────
def build_morning_brief() -> str:
    n   = now_ist()
    day = n.strftime("%A, %d %b %Y")

    # ── Market state ─────────────────────────────────────────
    mi  = _load("market_intel")
    mstate = fglabel = "—"
    fgscore = 0
    b200 = b50 = med1m = 0.0
    sectors_up = 0
    if not mi.empty:
        r = mi.iloc[0]
        mstate   = str(r.get("MARKET_STATE", "—"))
        fglabel  = str(r.get("FEAR_GREED_LABEL", "—"))
        fgscore  = _sf(r.get("FEAR_GREED_SCORE", 0))
        b200     = _sf(r.get("BREADTH_ABOVE_200DMA", 0))
        b50      = _sf(r.get("BREADTH_ABOVE_50DMA", 0))
        med1m    = _sf(r.get("MEDIAN_RETURN_1M", 0))
        sectors_up = _si(r.get("SECTORS_OUTPERFORMING", 0))

    mstate_icon = {"BULL": "🟢", "CAUTION": "🟡", "BEAR": "🔴"}.get(mstate, "⚪")

    # ── Tier counts ──────────────────────────────────────────
    cs  = _load("composite")
    total = prime = strong = wlc = landmines = 0
    if not cs.empty and "TIER" in cs.columns:
        tc      = cs["TIER"].value_counts()
        total   = len(cs)
        prime   = _si(tc.get("PRIME", 0))
        strong  = _si(tc.get("STRONG", 0))
        wlc     = _si(tc.get("WATCHLIST_CONFIRMED", 0))
        landmines = _si(tc.get("LANDMINE", 0))

    # ── Actionable picks ─────────────────────────────────────
    al  = _load("action")
    picks_lines = []
    if not al.empty and "AI_ACTION" in al.columns:
        buys = al[al["AI_ACTION"].str.contains("BUY|ACCUMULATE", case=False, na=False)]
        buys = buys.sort_values("COMPOSITE_SCORE", ascending=False).head(5)
        for _, r in buys.iterrows():
            sym   = str(r.get("NSE_SYMBOL",""))
            tier  = str(r.get("MULTIBAGGER_TIER",""))
            score = _si(r.get("COMPOSITE_SCORE", 0))
            act   = str(r.get("AI_ACTION","")).replace("🟢","").replace("✅","").strip()
            phase = str(r.get("SECTOR_PHASE",""))
            # Entry/SL from composite_scores
            entry = sl = rr = "—"
            if not cs.empty and "NSE_SYMBOL" in cs.columns:
                match = cs[cs["NSE_SYMBOL"] == sym]
                if not match.empty:
                    entry = str(match.iloc[0].get("ENTRY_ZONE","—"))
                    sl    = str(match.iloc[0].get("STOP_LOSS","—"))
                    rr    = str(match.iloc[0].get("RISK_REWARD","—"))
            picks_lines.append(
                f"  <code>{sym:<14}</code> {tier:<20} {score}/100  {phase}\n"
                f"    ↳ {act}  │  Entry:{entry}  SL:{sl}  R:R:{rr}"
            )

    # ── RECOVERY_WATCH ────────────────────────────────────────
    bw_lines = []
    if not cs.empty and "BEATEN_DOWN_WATCH" in cs.columns:
        bw = cs[cs["BEATEN_DOWN_WATCH"] == True].sort_values(
            "COMPOSITE_BALANCED", ascending=False).head(4)
        for _, r in bw.iterrows():
            sym   = str(r.get("NSE_SYMBOL",""))
            phase = str(r.get("SECTOR_PHASE",""))
            entry = str(r.get("ENTRY_ZONE","—"))
            bw_lines.append(f"  🌱 <code>{sym}</code>  {phase}  Entry:{entry}")

    # ── Early alerts ──────────────────────────────────────────
    ea  = _load("early_alerts")
    alert_lines = []
    if not ea.empty:
        top = ea.sort_values("SEVERITY", ascending=True).head(5)
        for _, r in top.iterrows():
            sev  = str(r.get("SEVERITY",""))
            sym  = str(r.get("NSE_SYMBOL",""))
            atyp = str(r.get("ALERT_TYPE",""))
            det  = str(r.get("ALERT_DETAIL",""))[:80]
            tier = str(r.get("TIER",""))
            icon = {"VERY_HIGH":"🔥","HIGH":"⚡","MEDIUM":"📌","LOW":"📎"}.get(sev,"📌")
            if atyp == "SECTOR_TURN":
                alert_lines.append(f"  {icon} SECTOR_TURN [{tier}] {det}")
            else:
                alert_lines.append(f"  {icon} [{atyp}] {sym} ({tier})  {det}")

    # ── Sector cycle highlights ───────────────────────────────
    sc = _load("sector_cycle")
    cycle_lines = []
    if not sc.empty:
        for phase, icon in [("MID_CYCLE","🚀"),("EARLY_RECOVERY","🌱"),("BASING","⏸️"),("CORRECTION","📉")]:
            subs = sc[sc["CYCLE_PHASE"] == phase]
            if not subs.empty:
                names = ", ".join(subs["INDUSTRY_GROUP"].tolist()[:3])
                cycle_lines.append(f"  {icon} {phase:<18} ({len(subs)}): {names}")

    # ── Global cues ──────────────────────────────────────────
    global_lines = fetch_global_cues()
    fii_line     = fetch_fii_proxy()

    # ── Compose message ───────────────────────────────────────
    msg = f"""🇮🇳 <b>MULTIBAGGER ENGINE — MORNING BRIEF</b>
{day}
{mstate_icon} Market: <b>{mstate}</b> | F&amp;G: {fglabel} ({fgscore:.0f}) | Above 200DMA: {b200:.1f}%
Breadth: {b50:.1f}% above 50DMA | Median 1M: {med1m:+.1f}% | Outperforming sectors: {sectors_up}

<b>📊 Universe Counts ({total:,} stocks)</b>
PRIME {prime} | STRONG {strong} | WL_CONFIRMED {wlc} | LANDMINES {landmines}
"""
    if global_lines:
        msg += "\n<b>🌍 Global Cues</b>\n" + "\n".join(global_lines) + f"\n{fii_line}\n"

    if picks_lines:
        msg += "\n<b>🎯 Actionable Picks (BUY / ACCUMULATE)</b>\n" + "\n".join(picks_lines) + "\n"

    if bw_lines:
        msg += "\n<b>🌱 RECOVERY WATCH (quality in sector correction)</b>\n" + "\n".join(bw_lines) + "\n"

    if alert_lines:
        msg += "\n<b>⚡ Early Alerts (pre-price signals)</b>\n" + "\n".join(alert_lines) + "\n"

    if cycle_lines:
        msg += "\n<b>🔄 Sector Cycle</b>\n" + "\n".join(cycle_lines) + "\n"

    msg += f"\n<i>Engine data as of last run. Run engine for fresh data.</i>"
    return msg


# ── INTRADAY ALERTS ───────────────────────────────────────────
def build_intraday_alerts() -> str | None:
    """
    Fire intraday alerts ONLY for:
      ORDER_WIN / PENALTY / RESULT_BEAT / CAPEX / ACQUISITION (from early_alerts)
      SECTOR_TURN (phase upgrades)
    For PRIME + STRONG stocks only (tier gate applied inside early_alerts.py)
    Returns None if nothing worth alerting.
    """
    ea = _load("early_alerts")
    if ea.empty:
        return None

    FIRE_TYPES = {"ORDER_WIN","PENALTY","RESULT_BEAT","CAPEX","ACQUISITION",
                  "BULK_BUY","BLOCK_BUY","INSIDER_BUY","ACE_INVESTOR","SECTOR_TURN",
                  "MF_JUMP","FII_JUMP"}
    HIGH_TIERS = {"PRIME","STRONG","WATCHLIST_CONFIRMED"}

    rows = ea[
        (ea["ALERT_TYPE"].isin(FIRE_TYPES)) &
        (ea["TIER"].isin(HIGH_TIERS) | (ea["ALERT_TYPE"] == "SECTOR_TURN"))
    ]

    # Only show alerts from last 24h (avoid re-sending old alerts)
    cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d")
    if "ALERT_DATE" in rows.columns:
        rows = rows[rows["ALERT_DATE"].astype(str) >= cutoff]

    if rows.empty:
        return None

    now_str = now_ist().strftime("%H:%M IST")
    lines   = [f"⚡ <b>INTRADAY ALERTS — {now_str}</b>"]
    for _, r in rows.sort_values("SEVERITY").head(8).iterrows():
        sev  = str(r.get("SEVERITY",""))
        sym  = str(r.get("NSE_SYMBOL",""))
        atyp = str(r.get("ALERT_TYPE",""))
        tier = str(r.get("TIER",""))
        det  = str(r.get("ALERT_DETAIL",""))[:100]
        date = str(r.get("ALERT_DATE",""))
        icon = {"VERY_HIGH":"🔥","HIGH":"⚡","MEDIUM":"📌","LOW":"📎"}.get(sev,"📌")
        if atyp == "SECTOR_TURN":
            lines.append(f"\n{icon} <b>SECTOR_TURN</b> [{sym}]\n  {det}")
        else:
            lines.append(f"\n{icon} <b>{sym}</b> [{tier}]  {atyp}  {date}\n  {det}")

    return "\n".join(lines)


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M IST')}] cloud_alert_engine v2.0 starting")

    sent = 0

    if is_morning_brief():
        print("  → Morning Brief mode")
        msg = build_morning_brief()
        if msg:
            ok = send(msg)
            print(f"  → Morning Brief sent: {ok}")
            sent += 1

    elif is_market_hours():
        print("  → Intraday Alert mode")
        msg = build_intraday_alerts()
        if msg:
            ok = send(msg)
            print(f"  → Intraday alert sent: {ok}")
            sent += 1
        else:
            print("  → No new alerts to send")
    else:
        print(f"  → Outside alert window ({now_ist().strftime('%H:%M IST')}) — sleeping")

    print(f"  Messages sent: {sent}")


if __name__ == "__main__":
    main()
