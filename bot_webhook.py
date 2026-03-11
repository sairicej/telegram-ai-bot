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

# Since your current feed does NOT contain true imbalance data,
# these thresholds are for a derived proxy score instead.
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.20"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.12"))

SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "600"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
STATE_FILE = os.getenv("STATE_FILE", "alert_state.json")
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
QUIET_MODE = os.getenv("QUIET_MODE", "false").lower() == "true"
PORT = int(os.getenv("PORT", "10000"))

app = Flask(__name__)

# =========================
# DATA MODEL
# =========================

@dataclass
class MarketCandidate:
    market_id: str
    title: str
    ticker: str
    imbalance: float  # used here as proxy_score
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


def fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    if abs(x) >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"
    if abs(x) >= 1_000:
        return f"{x / 1_000:.2f}K"
    return f"{x:.2f}"


def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:.2f}%"


def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# =========================
# FEED FETCH
# =========================

def fetch_live_feed() -> str:
    if not LIVE_FEED_SOURCE_URL:
        raise ValueError("LIVE_FEED_SOURCE_URL is not set")

    r = requests.get(LIVE_FEED_SOURCE_URL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


# =========================
# PARSER FOR YOUR CURRENT FEED
# =========================

def parse_text_feed(text: str) -> List[MarketCandidate]:
    """
    Parses feed entries like:

    Market BTC settles above 85000 by Apr 1 Mode settle Spot 68981 Strike 85000 Vol 26.94M
    Expiry 20 days Yes ask 0.18 No ask 0.83 Liquidity 5.23M Spread 0.01

    Works even if the whole file is one long line.
    """

    normalized = normalize_text(text)

    pattern = re.compile(
        r"Market\s+(?P<title>.*?)\s+"
        r"Mode\s+(?P<mode>\w+)\s+"
        r"Spot\s+(?P<spot>[-+]?\d+(?:\.\d+)?)\s+"
        r"Strike\s+(?P<strike>[-+]?\d+(?:\.\d+)?)\s+"
        r"Vol\s+(?P<vol>[-+]?\d+(?:\.\d+)?(?:[KkMm])?)\s+"
        r"Expiry\s+(?P<expiry>\d+)\s+days\s+"
        r"Yes ask\s+(?P<yes_ask>[-+]?\d+(?:\.\d+)?)\s+"
        r"No ask\s+(?P<no_ask>[-+]?\d+(?:\.\d+)?)\s+"
        r"Liquidity\s+(?P<liquidity>[-+]?\d+(?:\.\d+)?(?:[KkMm])?)\s+"
        r"Spread\s+(?P<spread>[-+]?\d+(?:\.\d+)?)",
        re.IGNORECASE,
    )

    matches = list(pattern.finditer(normalized))
    candidates: List[MarketCandidate] = []

    for i, m in enumerate(matches, start=1):
        title = m.group("title").strip()
        mode = m.group("mode").strip().lower()
        spot = parse_human_number(m.group("spot")) or 0.0
        strike = parse_human_number(m.group("strike")) or 0.0
        vol = parse_human_number(m.group("vol"))
        expiry_days = parse_human_number(m.group("expiry")) or 0.0
        yes_ask = parse_human_number(m.group("yes_ask")) or 0.0
        no_ask = parse_human_number(m.group("no_ask")) or 0.0
        liquidity = parse_human_number(m.group("liquidity"))
        spread = parse_human_number(m.group("spread")) or 0.0

        ticker = infer_ticker(title, i)
        market_id = f"{ticker}_{i}"

        # Derived proxy score:
        # Negative score = farther OTM with relatively cheap yes pricing and wider frictions.
        # This is NOT true order imbalance.
        distance_pct = safe_div((strike - spot), spot)

        # Base proxy
        proxy_score = yes_ask - distance_pct - spread

        # Mild penalty for low liquidity
        if liquidity is not None:
            if liquidity < 5000:
                proxy_score -= 0.03
            elif liquidity < 15000:
                proxy_score -= 0.01

        # Mild penalty for very short expiry
        if expiry_days <= 3:
            proxy_score -= 0.02

        candidate = MarketCandidate(
            market_id=market_id,
            title=title,
            ticker=ticker,
            imbalance=proxy_score,
            volume=vol,
            liquidity=liquidity,
            url=None,
            raw={
                "mode": mode,
                "spot": spot,
                "strike": strike,
                "expiry_days": expiry_days,
                "yes_ask": yes_ask,
                "no_ask": no_ask,
                "spread": spread,
                "distance_pct": distance_pct,
                "proxy_score": proxy_score,
            },
        )
        candidates.append(candidate)

    return candidates


def parse_human_number(value: str) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    mult = 1.0

    if s.lower().endswith("k"):
        mult = 1_000.0
        s = s[:-1]
    elif s.lower().endswith("m"):
        mult = 1_000_000.0
        s = s[:-1]

    try:
        return float(s) * mult
    except Exception:
        return None


def infer_ticker(title: str, idx: int) -> str:
    # Try to infer ticker from common crypto/market shorthand in title
    upper = title.upper()
    for token in ["BTC", "ETH", "SOL", "DOGE", "XRP", "ADA", "AVAX"]:
        if token in upper:
            return token

    # Fallback: first alphanumeric word
    m = re.search(r"[A-Z0-9]+", upper)
    if m:
        return m.group(0)[:12]

    return f"MKT{idx}"


def parse_feed(raw_text: str) -> List[MarketCandidate]:
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

def build_alert_message(c: MarketCandidate) -> str:
    raw = c.raw or {}
    lines = [
        f"{BOT_LABEL} | {c.category}",
        f"Market: {c.title}",
        f"Ticker: {c.ticker}",
        f"Proxy score: {c.imbalance:.3f}",
        f"Spot: {raw.get('spot', 'n/a')}",
        f"Strike: {raw.get('strike', 'n/a')}",
        f"Distance: {fmt_pct(raw.get('distance_pct'))}",
        f"Yes ask: {raw.get('yes_ask', 'n/a')}",
        f"No ask: {raw.get('no_ask', 'n/a')}",
        f"Expiry days: {raw.get('expiry_days', 'n/a')}",
        f"Volume: {fmt_num(c.volume)}",
        f"Liquidity: {fmt_num(c.liquidity)}",
        f"Spread: {raw.get('spread', 'n/a')}",
        f"Alert threshold: {ALERT_THRESHOLD:.2f}",
    ]
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
    log(f"Fetched feed length: {len(raw_text)}")
    log(f"Feed preview: {raw_text[:500]}")

    candidates = parse_feed(raw_text)
    log(f"Parsed candidates before classify: {len(candidates)}")

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

    top_candidates = [
        asdict(c) for c in sorted(candidates, key=lambda x: x.imbalance)[:10]
    ]

    return {
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
        "top_candidates": top_candidates,
    }


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


@app.route("/debug-feed", methods=["GET"])
def debug_feed():
    try:
        raw_text = fetch_live_feed()
        return jsonify({
            "ok": True,
            "length": len(raw_text),
            "preview": raw_text[:4000],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/scan-now", methods=["GET", "POST"])
def scan_now():
    try:
        result = scan_once(send_summary=False)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/telegram", methods=["POST"])
def telegram_webhook():
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
                f"Alert threshold: {ALERT_THRESHOLD}\n"
                f"Watch threshold: {WATCH_THRESHOLD}\n"
                f"Scan every: {SCAN_EVERY_SECONDS} sec"
            )
        elif text == "/debug":
            raw_text = fetch_live_feed()
            send_telegram_message(
                f"{BOT_LABEL} debug\n"
                f"Feed length: {len(raw_text)}\n"
                f"Preview: {raw_text[:1000]}"
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
