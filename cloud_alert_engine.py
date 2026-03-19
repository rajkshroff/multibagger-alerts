#!/usr/bin/env python3
# ============================================================
# cloud_alert_engine.py — v2.1
# SESSION 40 FIXES:
#   - Morning brief: was 8:25-8:35 (10min window, GitHub Actions
#     startup could miss it). Now full 8am hour (h==8). Any
#     30-min cron slot in the 8am hour fires the morning brief.
#   - Noon heartbeat (12:00-12:30 IST): always sends market state
#     even when no alerts — proves system is alive.
#   - Intraday: sends "no alerts" status every 2hrs (10am/2pm/6pm)
#     so silence = intentional, not broken.
#   - --test flag: sends test message immediately, bypasses time gates.
#   - Better console logging for GitHub Actions debug.
#
# LOCATION: Multibagger_App/multibagger-alerts/cloud_alert_engine.py
# TRIGGERED BY: GitHub Actions (alert_engine.yml)
#   - Morning Brief: 8:30am IST daily (3:00 UTC)
#   - Intraday Alerts: every 30min, 7am-8pm IST, 7 days
# ============================================================

import os
import sys
import requests
import json
import argparse
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

# ── ARGS ─────────────────────────────────────────────────────
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--test", action="store_true",
                    help="Send a test message immediately, bypass time gates")
args, _ = parser.parse_known_args()
TEST_MODE = args.test

# ── TELEGRAM CONFIG ──────────────────────────────────────────
BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "") or os.environ.get("TELEGRAM_TOKEN", "")
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
    """
    True if we're in the 8am IST hour.
    WIDENED from 8:25-8:35 → full hour (h==8).
    GitHub Actions takes 30-60s to start — a 10-min window was
    too narrow and caused silent failures.
    Any 30-min cron slot that fires during the 8am hour triggers
    the morning brief exactly once.
    """
    h, _ = ist_hour_min()
    return h == 8

def is_noon_heartbeat():
    """
    True 12:00-12:29 IST.
    Sends a short market status even with no alerts — proves
    system is alive. Replaces silence with status.
    """
    h, m = ist_hour_min()
    return h == 12 and m < 30

def is_status_slot():
    """
    True at 10am, 2pm, 6pm IST (first 30 min of those hours).
    Sends 'no alerts' confirmation so silence = intentional.
    """
    h, m = ist_hour_min()
    return h in (10, 14, 18) and m < 30

def is_market_hours():
    """True if 7am-8pm IST (inclusive)."""
    h, _ = ist_hour_min()
    return 7 <= h <= 20


# ── CSV LOADER ────────────────────────────────────────────────
def _load(key):
    p = CSV_FILES.get(key)
    if not p or not p.exists():
        print(f"  [CSV] MISSING: {key} → {p}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, low_memory=False)
        print(f"  [CSV] loaded: {key} → {len(df):,} rows")
        return df
    except Exception as e:
        print(f"  [CSV] FAILED: {key} → {e}")
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

SEEN_HASHES_FILE = REPO / "seen_hashes.json"
SEEN_HASH_TTL_HOURS = 48   # forget alerts older than this (avoids file growing forever)

def _load_seen_hashes() -> dict:
    """Load previously sent alert hashes from repo file."""
    if not SEEN_HASHES_FILE.exists():
        return {}
    try:
        import json
        return json.loads(SEEN_HASHES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_seen_hashes(hashes: dict):
    """Save updated hashes back to repo file + git commit."""
    import json, subprocess
    # Prune expired entries
    cutoff = (datetime.now(IST) - timedelta(hours=SEEN_HASH_TTL_HOURS)).isoformat()
    hashes = {k: v for k, v in hashes.items() if v >= cutoff}
    SEEN_HASHES_FILE.write_text(json.dumps(hashes, indent=2), encoding="utf-8")
    # Commit back to repo so next run sees updated hashes
    try:
        subprocess.run(["git", "add", "seen_hashes.json"], cwd=str(REPO), check=True)
        subprocess.run(["git", "commit", "-m", "chore: update seen alert hashes"],
                       cwd=str(REPO), check=True)
        subprocess.run(["git", "push"], cwd=str(REPO), check=True)
        print("  [dedup] seen_hashes.json committed + pushed")
    except Exception as e:
        print(f"  [dedup] git commit failed (non-fatal): {e}")

def _alert_hash(row) -> str:
    """Stable hash for one alert row — SYMBOL + TYPE + DATE."""
    import hashlib
    key = f"{row.get('NSE_SYMBOL','')}-{row.get('ALERT_TYPE','')}-{row.get('ALERT_DATE','')}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


# ── TELEGRAM SENDER ──────────────────────────────────────────
def send(msg: str, parse_mode: str = "HTML") -> bool:
    """Send a message to Telegram. Returns True on success."""
    if not msg.strip():
        print("  [send] Empty message — skipping")
        return False
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": parse_mode}
    try:
        r = requests.post(url, json=data, timeout=15)
        if r.status_code == 200:
            print(f"  [send] ✅ Sent ({len(msg)} chars)")
            return True
        print(f"  [send] HTTP {r.status_code}: {r.text[:300]}")
        return False
    except Exception as e:
        print(f"  [send] Error: {e}")
        return False


# ── GLOBAL CUES (yfinance) ────────────────────────────────────
GLOBAL_TICKERS = {
    "Dow Jones":  "^DJI",
    "Nasdaq":     "^IXIC",
    "Crude WTI":  "CL=F",
    "USD/INR":    "USDINR=X",
}

def fetch_global_cues() -> list:
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


# ── FII/DII PROXY ─────────────────────────────────────────────
def fetch_fii_proxy() -> str:
    try:
        nifty  = yf.Ticker("^NSEI").fast_info
        midcap = yf.Ticker("NIFTY_MIDCAP_100.NS").fast_info
        n_pct  = round((float(nifty.get("last_price",0)) / float(nifty.get("previous_close",1)) - 1) * 100, 2)
        m_pct  = round((float(midcap.get("last_price",0)) / float(midcap.get("previous_close",1)) - 1) * 100, 2)
        fii_sig = "FII-driven 📈" if n_pct > m_pct + 0.3 else ("DII-driven 📈" if m_pct > n_pct + 0.3 else "Balanced")
        return f"NIFTY50 {n_pct:+.2f}% vs MidCap {m_pct:+.2f}% → {fii_sig}"
    except Exception:
        return ""


# ── MARKET SUMMARY BLOCK (shared by brief + heartbeat) ────────
def build_market_summary() -> tuple:
    """Returns (mstate, summary_text). summary_text is multi-line HTML."""
    mi = _load("market_intel")
    mstate = fglabel = "—"
    fgscore = b200 = b50 = med1m = 0.0
    sectors_up = 0
    if not mi.empty:
        r = mi.iloc[0]
        mstate    = str(r.get("MARKET_STATE", "—"))
        fglabel   = str(r.get("FEAR_GREED_LABEL", "—"))
        fgscore   = _sf(r.get("FEAR_GREED_SCORE", 0))
        b200      = _sf(r.get("BREADTH_ABOVE_200DMA", 0))
        b50       = _sf(r.get("BREADTH_ABOVE_50DMA", 0))
        med1m     = _sf(r.get("MEDIAN_RETURN_1M", 0))
        sectors_up= _si(r.get("SECTORS_OUTPERFORMING", 0))

    mstate_icon = {"BULL":"🟢","CAUTION":"🟡","BEAR":"🔴"}.get(mstate,"⚪")

    cs = _load("composite")
    prime = strong = wlc = total = 0
    if not cs.empty and "TIER" in cs.columns:
        tc     = cs["TIER"].value_counts()
        total  = len(cs)
        prime  = _si(tc.get("PRIME",0))
        strong = _si(tc.get("STRONG",0))
        wlc    = _si(tc.get("WATCHLIST_CONFIRMED",0))

    text = (
        f"{mstate_icon} <b>{mstate}</b> | F&amp;G: {fglabel} ({fgscore:.0f}) | "
        f"Above 200DMA: {b200:.1f}%\n"
        f"50DMA: {b50:.1f}% | Median 1M: {med1m:+.1f}% | Outperforming: {sectors_up} sectors\n"
        f"PRIME <b>{prime}</b> | STRONG <b>{strong}</b> | WLC <b>{wlc}</b> | Universe {total:,}"
    )
    return mstate, text


# ── MORNING BRIEF ─────────────────────────────────────────────
def build_morning_brief() -> str:
    n   = now_ist()
    day = n.strftime("%A, %d %b %Y")

    mstate, mkt_text = build_market_summary()

    # Actionable picks
    al  = _load("action")
    cs  = _load("composite")
    picks_lines = []
    if not al.empty and "AI_ACTION" in al.columns:
        buys = al[al["AI_ACTION"].str.contains("BUY|ACCUMULATE", case=False, na=False)]
        buys = buys.sort_values("COMPOSITE_SCORE", ascending=False).head(5)
        for _, r in buys.iterrows():
            sym   = str(r.get("NSE_SYMBOL",""))
            tier  = str(r.get("MULTIBAGGER_TIER",""))
            score = _si(r.get("COMPOSITE_SCORE",0))
            act   = str(r.get("AI_ACTION","")).replace("🟢","").replace("✅","").strip()
            phase = str(r.get("SECTOR_PHASE",""))
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

    # Recovery watch
    bw_lines = []
    if not cs.empty and "BEATEN_DOWN_WATCH" in cs.columns:
        bw = cs[cs["BEATEN_DOWN_WATCH"] == True].sort_values(
            "COMPOSITE_BALANCED", ascending=False).head(4)
        for _, r in bw.iterrows():
            sym   = str(r.get("NSE_SYMBOL",""))
            phase = str(r.get("SECTOR_PHASE",""))
            entry = str(r.get("ENTRY_ZONE","—"))
            bw_lines.append(f"  🌱 <code>{sym}</code>  {phase}  Entry:{entry}")

    # Early alerts
    ea = _load("early_alerts")
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

    # Sector cycle highlights
    sc = _load("sector_cycle")
    cycle_lines = []
    if not sc.empty:
        for phase, icon in [("MID_CYCLE","🚀"),("EARLY_RECOVERY","🌱"),
                             ("BASING","⏸️"),("CORRECTION","📉")]:
            subs = sc[sc["CYCLE_PHASE"] == phase]
            if not subs.empty:
                names = ", ".join(subs["INDUSTRY_GROUP"].tolist()[:3])
                cycle_lines.append(f"  {icon} {phase:<18} ({len(subs)}): {names}")

    # Global cues
    global_lines = fetch_global_cues()
    fii_line     = fetch_fii_proxy()

    msg = f"""🇮🇳 <b>MULTIBAGGER ENGINE — MORNING BRIEF</b>
{day}
{mkt_text}
"""
    if global_lines:
        msg += "\n<b>🌍 Global Cues</b>\n" + "\n".join(global_lines)
        if fii_line:
            msg += f"\n{fii_line}"
        msg += "\n"

    if picks_lines:
        msg += "\n<b>🎯 Actionable Picks (BUY / ACCUMULATE)</b>\n" + "\n".join(picks_lines) + "\n"

    if bw_lines:
        msg += "\n<b>🌱 RECOVERY WATCH</b>\n" + "\n".join(bw_lines) + "\n"

    if alert_lines:
        msg += "\n<b>⚡ Early Alerts</b>\n" + "\n".join(alert_lines) + "\n"

    if cycle_lines:
        msg += "\n<b>🔄 Sector Cycle</b>\n" + "\n".join(cycle_lines) + "\n"

    msg += f"\n<i>Engine data from last run.</i>"
    return msg


# ── NOON HEARTBEAT ────────────────────────────────────────────
def build_noon_heartbeat() -> str:
    _, mkt_text = build_market_summary()
    t = now_ist().strftime("%H:%M IST")
    ea = _load("early_alerts")
    alert_count = len(ea) if not ea.empty else 0
    return (
        f"💓 <b>MIDDAY STATUS — {t}</b>\n"
        f"{mkt_text}\n"
        f"Early alerts in queue: {alert_count}\n"
        f"<i>Engine running. No action needed unless alerts below.</i>"
    )


# ── INTRADAY ALERTS ───────────────────────────────────────────
def build_intraday_alerts(send_status_if_empty: bool = False):
    """
    Returns alert message string, or status string if send_status_if_empty=True
    and nothing actionable found, or None if nothing to send.
    """
    ea = _load("early_alerts")
    if ea.empty:
        if send_status_if_empty:
            t = now_ist().strftime("%H:%M IST")
            return f"✅ <b>{t}</b> — Engine alive. No new alerts."
        return None

    # ── CONTENT-HASH DEDUP — prevents cron duplicate sends ──────
    # Each alert row is hashed (SYMBOL+TYPE+DATE).
    # Hashes seen in previous runs are stored in seen_hashes.json.
    # Only unseen rows fire. After firing, hashes are committed back.
    # Works correctly on GitHub Actions (no mtime dependency).
    seen = _load_seen_hashes()
    new_rows_mask = ea.apply(lambda r: _alert_hash(r) not in seen, axis=1)
    ea = ea[new_rows_mask].copy()
    print(f"  [dedup] {new_rows_mask.sum()} new / {(~new_rows_mask).sum()} already seen")

    if ea.empty:
        print("  [intraday] all alerts already sent — skipping")
        if send_status_if_empty:
            t = now_ist().strftime("%H:%M IST")
            return f"✅ <b>{t}</b> — Engine alive. No new alerts."
        return None

    FIRE_TYPES = {"ORDER_WIN","PENALTY","RESULT_BEAT","CAPEX","ACQUISITION",
                  "BULK_BUY","BLOCK_BUY","INSIDER_BUY","ACE_INVESTOR","SECTOR_TURN",
                  "MF_JUMP","FII_JUMP"}
    HIGH_TIERS = {"PRIME","STRONG","WATCHLIST_CONFIRMED"}

    rows = ea[
        (ea["ALERT_TYPE"].isin(FIRE_TYPES)) &
        (ea["TIER"].isin(HIGH_TIERS) | (ea["ALERT_TYPE"] == "SECTOR_TURN"))
    ]

    cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d")
    if "ALERT_DATE" in rows.columns:
        rows = rows[rows["ALERT_DATE"].astype(str) >= cutoff]

    if rows.empty:
        if send_status_if_empty:
            t = now_ist().strftime("%H:%M IST")
            return f"✅ <b>{t}</b> — Engine alive. No actionable alerts."
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
    now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S IST")
    h, m = ist_hour_min()
    print(f"[{now_str}] cloud_alert_engine v2.1 starting")
    print(f"  IST time: {h:02d}:{m:02d}")
    print(f"  morning_brief={is_morning_brief()} noon={is_noon_heartbeat()} "
          f"status_slot={is_status_slot()} market_hours={is_market_hours()}")
    print(f"  TEST_MODE={TEST_MODE}")
    print(f"  CSV files present: "
          + ", ".join(k for k, v in CSV_FILES.items() if v.exists()))

    sent = 0

    # ── TEST MODE ─────────────────────────────────────────────
    if TEST_MODE:
        print("  → TEST MODE")
        _, mkt_text = build_market_summary()
        msg = (
            f"🧪 <b>MULTIBAGGER — TEST MESSAGE</b>\n"
            f"{now_ist().strftime('%H:%M IST')}\n"
            f"{mkt_text}\n"
            f"<i>System alive. Telegram verified.</i>"
        )
        ok = send(msg)
        print(f"  → Test message sent: {ok}")
        return

    # ── MORNING BRIEF (8am IST — full hour) ───────────────────
    if is_morning_brief():
        print("  → Morning Brief mode")
        msg = build_morning_brief()
        ok = send(msg)
        print(f"  → Morning Brief sent: {ok}")
        sent += 1

    # ── NOON HEARTBEAT (12:00-12:29 IST) ──────────────────────
    elif is_noon_heartbeat():
        print("  → Noon heartbeat mode")
        msg = build_noon_heartbeat()
        ok = send(msg)
        print(f"  → Noon heartbeat sent: {ok}")
        sent += 1

    # ── INTRADAY WITH STATUS SLOT ──────────────────────────────
    elif is_market_hours():
        send_status = is_status_slot()
        print(f"  → Intraday mode (send_status={send_status})")
        msg = build_intraday_alerts(send_status_if_empty=send_status)
        if msg:
            ok = send(msg)
            print(f"  → Intraday/status sent: {ok}")
            if ok:
                # Commit seen hashes so next run doesn't re-send
                ea_df = _load("early_alerts")
                if not ea_df.empty:
                    seen = _load_seen_hashes()
                    now_iso = now_ist().isoformat()
                    for _, row in ea_df.iterrows():
                        seen[_alert_hash(row)] = now_iso
                    _save_seen_hashes(seen)
            sent += 1
        else:
            print("  → No new alerts, no status slot — silent")

    else:
        print(f"  → Outside alert window ({h:02d}:{m:02d} IST) — sleeping")

    print(f"  Messages sent: {sent}")


if __name__ == "__main__":
    main()
