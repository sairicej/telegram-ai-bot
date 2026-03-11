#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, request

# =========================
# CONFIG
# =========================

BOT_LABEL = os.getenv("BOT_LABEL", "Imbalance Bot").strip()

LIVE_FEED_SOURCE_URL = os.getenv("LIVE_FEED_SOURCE_URL", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.15"))   # -15%
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.10"))   # -10%
SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "600"))  # 10 min
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
STATE_FILE = os.getenv("STATE_FILE", "alert_state.json")
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
QUIET_MODE = os.getenv("QUIET_MODE", "false").lower() == "true"
PORT = int(os.getenv("PORT", "10000"))

# =========================
# APP
# =========================

app = Flask(__name__)

# =========================
# DATA MODEL
# =========================

@dataclass
class MarketCandidate:
    market_id: str
    title: str
    ticker: str
    imbalance: float
    volume: Optional[float] = None
    liquidity: Optional[float] = None
    url: Optional[str] = None
    category: str = "SKIP"  # WATCH / ALERT / SKIP
    raw: Optional[Dict[str, Any]] = None


# =========================
# LOGGING
# =========================

def log(msg: str) -> None:
    if not QUIET_MODE:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] {msg}", flush=True)


# =========================
# STATE
# =========================

def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"alerted": {}, "watch_alerted": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"alerted": {}, "watch_alerted": {}}

def save_state(state: Dict[str, Any]) -> None:
    tmp = f"{STATE_FILE}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


# =========================
# TELEGRAM
# =========================

def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram config missing. Skipping send.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
        if r.ok:
            return True
        log(f"Telegram send failed: {r.status_code} {r.text[:300]}")
        return False
    except Exception as e:
        log(f"Telegram exception: {e}")
        return False


# =========================
# HELPERS
# =========================

def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().replace(",", "")
    s = s.replace("$", "")
    s = s.replace("%", "")

    try:
        return float(s)
    except Exception:
        return None

def normalize_imbalance(value: Any) -> Optional[float]:
    """
    Converts either:
    - -0.153 -> -0.153
    - -15.3  -> -0.153
    - '-15.3%' -> -0.153
    """
    num = parse_float(value)
    if num is None:
        return None

    if abs(num) > 1.0:
        return num / 100.0
    return num

def safe_str(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value).strip()

def get_first(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


# =========================
# FEED FETCH
# =========================

def fetch_live_feed() -> str:
    if not LIVE_FEED_SOURCE_URL:
        raise ValueError("LIVE_FEED_SOURCE_URL is not set")

    r = requests.get(LIVE_FEED_SOURCE_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text

def try_parse_json(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        return None


# =========================
# PARSERS
# =========================

def parse_json_feed(data: Any) -> List[MarketCandidate]:
    items: List[Dict[str, Any]] = []

    if isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
    elif isinstance(data, dict):
        for key in ["markets", "data", "items", "results", "candidates"]:
            if isinstance(data.get(key), list):
                items = [x for x in data[key] if isinstance(x, dict)]
                break
        if not items:
            items = [data]

    candidates: List[MarketCandidate] = []

    for item in items:
        title = safe_str(get_first(item, ["title", "name", "question", "market", "label"], "Untitled Market"))
        ticker = safe_str(get_first(item, ["ticker", "symbol", "slug", "code"], title[:24]))
        market_id = safe_str(get_first(item, ["id", "market_id", "slug", "ticker"], ticker or title))

        imbalance_raw = get_first(
            item,
            ["imbalance", "rsv", "rsv_close", "closing_imbalance", "imbalance_pct", "imbalance_percent"],
            None,
        )
        imbalance = normalize_imbalance(imbalance_raw)
        if imbalance is None:
            continue

        volume = parse_float(get_first(item, ["volume", "vol", "daily_volume", "market_volume"], None))
        liquidity = parse_float(get_first(item, ["liquidity", "liq"], None))
        url = safe_str(get_first(item, ["url", "link", "market_url"], ""), "")

        candidates.append(
            MarketCandidate(
                market_id=market_id,
                title=title,
                ticker=ticker,
                imbalance=imbalance,
                volume=volume,
                liquidity=liquidity,
                url=url or None,
                raw=item,
            )
        )

    return candidates

def parse_text_feed(text: str) -> List[MarketCandidate]:
    """
    Flexible text parser for feed blocks like:
    Title: ...
    Ticker: ...
    Imbalance: -16.2%
    Volume: 12345
    Liquidity: 9999
    URL: ...
    """
    blocks = re.split(r"\n\s*\n", text.strip())
    candidates: List[MarketCandidate] = []

    for i, block in enumerate(blocks, start=1):
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue

        merged = "\n".join(lines)

        title_match = re.search(r"(?im)^(?:title|market|name|question)\s*:\s*(.+)$", merged)
        ticker_match = re.search(r"(?im)^(?:ticker|symbol|code|slug)\s*:\s*(.+)$", merged)
        imbalance_match = re.search(
            r"(?im)^(?:imbalance|rsv|rsv_close|closing_imbalance|imbalance_pct|imbalance_percent)\s*:\s*([\-+]?\d+(?:\.\d+)?%?)$",
            merged,
        )
        volume_match = re.search(r"(?im)^(?:volume|vol)\s*:\s*([$0-9,.\-]+)$", merged)
        liquidity_match = re.search(r"(?im)^(?:liquidity|liq)\s*:\s*([$0-9,.\-]+)$", merged)
        url_match = re.search(r"(?im)^(?:url|link)\s*:\s*(https?://\S+)$", merged)

        if not imbalance_match:
            pct_inline = re.search(r"([\-+]?\d+(?:\.\d+)?)\s*%", merged)
            if pct_inline:
                imbalance_val = normalize_imbalance(pct_inline.group(1) + "%")
            else:
                continue
        else:
            imbalance_val = normalize_imbalance(imbalance_match.group(1))

        if imbalance_val is None:
            continue

        title = title_match.group(1).strip() if title_match else f"Market Block {i}"
        ticker = ticker_match.group(1).strip() if ticker_match else f"MKT{i}"
        market_id = ticker or f"market_{i}"

        volume = parse_float(volume_match.group(1)) if volume_match else None
        liquidity = parse_float(liquidity_match.group(1)) if liquidity_match else None
        url = url_match.group(1).strip() if url_match else None

        candidates.append(
            MarketCandidate(
                market_id=market_id,
                title=title,
                ticker=ticker,
                imbalance=imbalance_val,
                volume=volume,
                liquidity=liquidity,
                url=url,
                raw={"block": merged},
            )
        )

    return candidates

def parse_feed(raw_text: str) -> List[MarketCandidate]:
    parsed_json = try_parse_json(raw_text)
    if parsed_json is not None:
        return parse_json_feed(parsed_json)
    return parse_text_feed(raw_text)


# =========================
# CLASSIFICATION
# =========================

def classify_candidate(c: MarketCandidate) -> MarketCandidate:
    if c.imbalance <= ALERT_THRESHOLD:
        c.category = "ALERT"
    elif c.imbalance <= WATCH_THRESHOLD:
        c.category = "WATCH"
    else:
        c.category = "SKIP"
    return c


# =========================
# MESSAGE BUILDERS
# =========================

def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:.2f}%"

def fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if abs(x) >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"
    if abs(x) >= 1_000:
        return f"{x / 1_000:.2f}K"
    return f"{x:.2f}"

def build_alert_message(c: MarketCandidate) -> str:
    lines = [
        f"{BOT_LABEL} | {c.category}",
        f"Market: {c.title}",
        f"Ticker: {c.ticker}",
        f"Imbalance: {fmt_pct(c.imbalance)}",
        f"Threshold hit: YES (<= {fmt_pct(ALERT_THRESHOLD)})" if c.category == "ALERT" else f"Threshold hit: watch (<= {fmt_pct(WATCH_THRESHOLD)})",
        f"Volume: {fmt_num(c.volume)}",
        f"Liquidity: {fmt_num(c.liquidity)}",
        "Expected reaction window: 2:00–4:00 a.m. ET, strongest 2:00–3:00 a.m. ET",
    ]
    if c.url:
        lines.append(f"Link: {c.url}")
    return "\n".join(lines)

def build_summary(candidates: List[MarketCandidate]) -> str:
    total = len(candidates)
    alerts = sum(1 for x in candidates if x.category == "ALERT")
    watch = sum(1 for x in candidates if x.category == "WATCH")
    skip = sum(1 for x in candidates if x.category == "SKIP")
    return (
        f"Candidate market counts:\n"
        f"- Total valid markets: {total}\n"
        f"- ALERT: {alerts}\n"
        f"- WATCH: {watch}\n"
        f"- SKIP: {skip}"
    )


# =========================
# SCAN LOGIC
# =========================

def scan_once(send_summary: bool = False) -> Dict[str, Any]:
    state = load_state()

    raw_text = fetch_live_feed()
    candidates = parse_feed(raw_text)
    candidates = [classify_candidate(c) for c in candidates]

    log(build_summary(candidates))

    sent_alerts = 0
    sent_watch = 0

    for c in candidates:
        dedupe_key = f"{c.market_id}|{c.category}|{round(c.imbalance, 4)}"

        if c.category == "ALERT":
            if dedupe_key not in state["alerted"]:
                ok = send_telegram_message(build_alert_message(c))
                if ok:
                    state["alerted"][dedupe_key] = datetime.now(timezone.utc).isoformat()
                    sent_alerts += 1

        elif c.category == "WATCH" and SEND_WATCH_ALERTS:
            if dedupe_key not in state["watch_alerted"]:
                ok = send_telegram_message(build_alert_message(c))
                if ok:
                    state["watch_alerted"][dedupe_key] = datetime.now(timezone.utc).isoformat()
                    sent_watch += 1

    save_state(state)

    result = {
        "ok": True,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "total": len(candidates),
            "alert": sum(1 for x in candidates if x.category == "ALERT"),
            "watch": sum(1 for x in candidates if x.category == "WATCH"),
            "skip": sum(1 for x in candidates if x.category == "SKIP"),
        },
        "sent": {
            "alert": sent_alerts,
            "watch": sent_watch,
        },
        "top_candidates": [asdict(c) for c in candidates if c.category in ("ALERT", "WATCH")][:10],
    }

    if send_summary:
        send_telegram_message(
            f"{BOT_LABEL} summary\n"
            f"Alert sent: {sent_alerts}\n"
            f"Watch sent: {sent_watch}\n"
            f"Total scanned: {len(candidates)}"
        )

    return result


# =========================
# BACKGROUND LOOP
# =========================

_worker_started = False

def scanner_loop() -> None:
    log("Background scanner started.")
    while True:
        try:
            result = scan_once(send_summary=False)
            log(f"Scan complete: {json.dumps(result['counts'])}")
        except Exception as e:
            log(f"Scan error: {e}")
        time.sleep(SCAN_EVERY_SECONDS)

def start_background_worker() -> None:
    global _worker_started
    if _worker_started:
        return
    _worker_started = True
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()


# =========================
# ROUTES
# =========================

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "service": BOT_LABEL,
        "message": "Webhook + background alert scanner running"
    })

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})

@app.route("/scan-now", methods=["GET", "POST"])
def scan_now():
    try:
        result = scan_once(send_summary=False)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    """
    Optional webhook endpoint.
    If you later wire Telegram webhook here, it can reply to simple commands.
    """
    try:
        payload = request.get_json(force=True, silent=True) or {}
        message = payload.get("message", {})
        text = (message.get("text") or "").strip().lower()

        if text == "/scan":
            result = scan_once(send_summary=False)
            send_telegram_message(
                f"{BOT_LABEL} manual scan complete\n"
                f"Total: {result['counts']['total']}\n"
                f"Alert: {result['counts']['alert']}\n"
                f"Watch: {result['counts']['watch']}\n"
                f"Skip: {result['counts']['skip']}"
            )
        elif text == "/status":
            send_telegram_message(
                f"{BOT_LABEL} status\n"
                f"Threshold: {fmt_pct(ALERT_THRESHOLD)}\n"
                f"Watch: {fmt_pct(WATCH_THRESHOLD)}\n"
                f"Scan every: {SCAN_EVERY_SECONDS} sec"
            )

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# STARTUP
# =========================

start_background_worker()

if __name__ == "__main__":
    log(f"Starting {BOT_LABEL} on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)
