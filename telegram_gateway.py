#!/usr/bin/env python3
# ============================================================
# telegram_gateway.py — canonical Telegram sender for the engine
#
# Single entry point for ALL Telegram messages.
# Features: retry (3×), 429 handling, 4000-char chunking,
# multi-recipient routing, thread-safe throttle.
# Dedup is NOT owned here — each caller keeps its own registry.
# ============================================================
from __future__ import annotations

import os
import time
import threading
from typing import Iterable, Optional

import ssl
import requests
from requests.adapters import HTTPAdapter

class _SystemSSLAdapter(HTTPAdapter):
    """Use the OS certificate store instead of certifi — fixes Windows SSL verify failures."""
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

_tg_session = requests.Session()
_tg_session.mount('https://api.telegram.org', _SystemSSLAdapter())

_MAX_CHARS = 4000
_RETRIES = 3
_RETRY_BACKOFF_S = 2
_DEFAULT_TIMEOUT = 15
_MIN_GAP_S = 0.05

_lock = threading.Lock()
_last_send_at = 0.0
_warned_no_cred = False


def _load_creds() -> tuple[str, list[str]]:
    bot = (os.environ.get("TELEGRAM_BOT_TOKEN", "") or os.environ.get("TELEGRAM_TOKEN", "")).strip()
    raw = (os.environ.get("TELEGRAM_CHAT_IDS", "") or os.environ.get("TELEGRAM_CHAT_ID", "")).strip()
    ids = [x.strip() for x in raw.split(",") if x.strip()]
    return bot, ids


def _throttle() -> None:
    global _last_send_at
    with _lock:
        delta = time.time() - _last_send_at
        if delta < _MIN_GAP_S:
            time.sleep(_MIN_GAP_S - delta)
        _last_send_at = time.time()


def _post_one(url: str, chat_id: str, text: str, parse_mode: str) -> bool:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    for attempt in range(_RETRIES):
        try:
            _throttle()
            r = _tg_session.post(url, json=payload, timeout=_DEFAULT_TIMEOUT)
            if r.ok:
                print(f"  [tg→..{chat_id[-4:]}] ok {len(text)} chars", flush=True)
                return True
            if r.status_code == 429:
                try:
                    wait = int(r.json().get("parameters", {}).get("retry_after", 5))
                except Exception:
                    wait = 5
                print(f"  [tg→..{chat_id[-4:]}] 429 retry in {wait}s", flush=True)
                time.sleep(min(wait, 30))
                continue
            print(f"  [tg→..{chat_id[-4:]}] HTTP {r.status_code}: {r.text[:120]}", flush=True)
            if 400 <= r.status_code < 500:
                return False
        except Exception as e:
            print(f"  [tg→..{chat_id[-4:]}] err {attempt+1}/{_RETRIES}: {e}", flush=True)
        if attempt < _RETRIES - 1:
            time.sleep(_RETRY_BACKOFF_S * (attempt + 1))
    return False


def send(text: str, parse_mode: str = "HTML", chat_ids: Optional[Iterable[str]] = None) -> bool:
    """Send text to all configured Telegram recipients.

    Args:
        text: Message body (HTML or Markdown).
        parse_mode: "HTML" (default) or "Markdown".
        chat_ids: Override recipient list. Uses env vars if None.

    Returns True if every chunk reached every recipient.
    """
    global _warned_no_cred
    if not text or not text.strip():
        return False
    bot, env_ids = _load_creds()
    targets = list(chat_ids) if chat_ids else env_ids
    if not bot or not targets:
        if not _warned_no_cred:
            print("[tg] TELEGRAM_BOT_TOKEN / CHAT_IDS unset — skipping", flush=True)
            _warned_no_cred = True
        return False
    url = f"https://api.telegram.org/bot{bot}/sendMessage"
    chunks = [text[i:i + _MAX_CHARS] for i in range(0, len(text), _MAX_CHARS)]
    ok = True
    for cid in targets:
        for i, chunk in enumerate(chunks):
            suffix = f"\n<i>Part {i+1}/{len(chunks)}</i>" if len(chunks) > 1 else ""
            if not _post_one(url, cid, chunk + suffix, parse_mode):
                ok = False
    return ok
