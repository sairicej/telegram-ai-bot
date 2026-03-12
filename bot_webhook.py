import os
import json
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# -----------------------------
# ENV VARIABLES
# -----------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Public raw file example:
# https://raw.githubusercontent.com/<owner>/<repo>/<branch>/live_feed_markets.txt
LIVE_FEED_URL = os.getenv("LIVE_FEED_URL", "").strip()

# Optional fallback: paste JSON directly into Render env var
# Example:
# [
#   {"market_id":"BTC_150K_MARCH","token_id":"12345","label":"BTC hits 150k in March","baseline_prob":0.62},
#   {"market_id":"SOL_3","token_id":"67890","label":"SOL market","baseline_prob":0.41}
# ]
LIVE_FEED_JSON = os.getenv("LIVE_FEED_JSON", "").strip()

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.20"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.12"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"

MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "0"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

# Polymarket CLOB endpoint base
CLOB_BASE = "https://clob.polymarket.com"


# -----------------------------
# HELPERS
# -----------------------------
def now_utc():
    return datetime.now(timezone.utc).isoformat()


def send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram settings missing.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }

    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.exception("Telegram send failed: %s", e)
        return False


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


# -----------------------------
# LOAD WATCHLIST
# -----------------------------
def load_markets_from_url():
    if not LIVE_FEED_URL:
        return None

    try:
        r = requests.get(LIVE_FEED_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        text = r.text.strip()

        # Supports either JSON or line-delimited JSON
        if text.startswith("["):
            return json.loads(text)

        rows = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    except Exception as e:
        logging.exception("Failed loading LIVE_FEED_URL: %s", e)
        return None


def load_markets_from_env():
    if not LIVE_FEED_JSON:
        return None
    try:
        return json.loads(LIVE_FEED_JSON)
    except Exception as e:
        logging.exception("Failed loading LIVE_FEED_JSON: %s", e)
        return None


def load_markets():
    # First try GitHub raw file
    markets = load_markets_from_url()
    if markets:
        logging.info("Loaded %s markets from LIVE_FEED_URL", len(markets))
        return markets

    # Fallback to Render environment variable
    markets = load_markets_from_env()
    if markets:
        logging.info("Loaded %s markets from LIVE_FEED_JSON", len(markets))
        return markets

    logging.warning("No market list loaded.")
    return []


# -----------------------------
# POLYMARKET LIVE DATA
# -----------------------------
def fetch_orderbook(token_id: str):
    """
    Reads orderbook for a token.
    """
    url = f"{CLOB_BASE}/book"
    params = {"token_id": token_id}
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def compute_live_probability(orderbook: dict):
    """
    Uses midpoint when possible.
    Falls back to last trade price if midpoint cannot be derived.
    """
    bids = orderbook.get("bids", []) or []
    asks = orderbook.get("asks", []) or []

    best_bid = safe_float(bids[0]["price"], None) if bids else None
    best_ask = safe_float(asks[0]["price"], None) if asks else None

    midpoint = None
    spread = None

    if best_bid is not None and best_ask is not None:
        midpoint = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

    last_price = safe_float(orderbook.get("price") or orderbook.get("last_trade_price"), None)

    if midpoint is not None:
        if spread is not None and spread <= 0.10:
            return {
                "probability": midpoint,
                "source": "midpoint",
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "last_price": last_price
            }

    if last_price is not None:
        return {
            "probability": last_price,
            "source": "last_trade",
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "last_price": last_price
        }

    return {
        "probability": None,
        "source": "missing",
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "last_price": last_price
    }


def estimate_liquidity(orderbook: dict):
    """
    Lightweight liquidity estimate from top visible book levels.
    """
    bids = orderbook.get("bids", []) or []
    asks = orderbook.get("asks", []) or []

    total = 0.0
    for side in (bids[:5] + asks[:5]):
        price = safe_float(side.get("price"), 0.0)
        size = safe_float(side.get("size"), 0.0)
        total += price * size
    return total


# -----------------------------
# SCORING
# -----------------------------
def classify_market(baseline_prob: float, live_prob: float, liquidity: float):
    """
    Negative means live probability is below baseline.
    Example:
      baseline 0.60, live 0.40 => imbalance = -0.20
    """
    imbalance = live_prob - baseline_prob

    if liquidity < MIN_LIQUIDITY:
        return "SKIP", imbalance

    if imbalance <= ALERT_THRESHOLD:
        return "ALERT", imbalance

    if imbalance <= WATCH_THRESHOLD:
        return "WATCH", imbalance

    return "SKIP", imbalance


def evaluate_market(row: dict):
    market_id = row.get("market_id", "unknown")
    token_id = str(row.get("token_id", "")).strip()
    label = row.get("label", market_id)
    baseline_prob = safe_float(row.get("baseline_prob"), None)

    if not token_id or baseline_prob is None:
        return {
            "market_id": market_id,
            "label": label,
            "category": "SKIP",
            "reason": "missing_token_or_baseline"
        }

    try:
        orderbook = fetch_orderbook(token_id)
        live = compute_live_probability(orderbook)
        live_prob = live["probability"]

        if live_prob is None:
            return {
                "market_id": market_id,
                "label": label,
                "category": "SKIP",
                "reason": "missing_live_probability"
            }

        liquidity = estimate_liquidity(orderbook)
        category, imbalance = classify_market(baseline_prob, live_prob, liquidity)

        return {
            "market_id": market_id,
            "label": label,
            "category": category,
            "imbalance": imbalance,
            "baseline_prob": baseline_prob,
            "live_prob": live_prob,
            "liquidity": liquidity,
            "source": live["source"],
            "best_bid": live["best_bid"],
            "best_ask": live["best_ask"],
            "spread": live["spread"],
            "last_price": live["last_price"],
            "raw": {
                "distance_pct": abs(imbalance),
                "token_id": token_id
            }
        }

    except Exception as e:
        logging.exception("Market eval failed for %s: %s", market_id, e)
        return {
            "market_id": market_id,
            "label": label,
            "category": "SKIP",
            "reason": f"error: {str(e)}"
        }


# -----------------------------
# SCAN
# -----------------------------
def run_scan():
    markets = load_markets()

    results = [evaluate_market(row) for row in markets]

    counts = {
        "total": len(results),
        "alert": sum(1 for r in results if r.get("category") == "ALERT"),
        "watch": sum(1 for r in results if r.get("category") == "WATCH"),
        "skip": sum(1 for r in results if r.get("category") == "SKIP"),
    }

    ranked = sorted(
        [r for r in results if r.get("imbalance") is not None],
        key=lambda x: x["imbalance"]
    )

    top_candidates = ranked[:5]

    sent = {"alert": 0, "watch": 0}

    for r in top_candidates:
        cat = r["category"]
        if cat == "ALERT" or (cat == "WATCH" and SEND_WATCH_ALERTS):
            text = (
                f"📌 {cat}\n"
                f"Market: {r['label']}\n"
                f"ID: {r['market_id']}\n"
                f"Baseline: {r['baseline_prob']:.3f}\n"
                f"Live: {r['live_prob']:.3f}\n"
                f"Imbalance: {r['imbalance']:.3f}\n"
                f"Liquidity: {r['liquidity']:.2f}\n"
                f"Source: {r['source']}\n"
                f"Spread: {r['spread'] if r['spread'] is not None else 'n/a'}\n"
                f"Time UTC: {now_utc()}"
            )
            ok = send_telegram_message(text)
            if ok:
                if cat == "ALERT":
                    sent["alert"] += 1
                elif cat == "WATCH":
                    sent["watch"] += 1

    return {
        "ok": True,
        "timestamp_utc": now_utc(),
        "counts": counts,
        "sent": sent,
        "top_candidates": top_candidates
    }


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "message": "Bot is live",
        "timestamp_utc": now_utc()
    })


@app.route("/scan", methods=["GET", "POST"])
def scan():
    try:
        return jsonify(run_scan())
    except Exception as e:
        logging.exception("Scan error: %s", e)
        return jsonify({
            "ok": False,
            "error": str(e),
            "timestamp_utc": now_utc()
        }), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Simple Telegram webhook endpoint.
    If user sends /scan in Telegram, run scan.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        text = (
            data.get("message", {}).get("text", "")
            or data.get("edited_message", {}).get("text", "")
        ).strip().lower()

        if text == "/scan":
            result = run_scan()
            send_telegram_message(json.dumps(result, indent=2))
            return jsonify({"ok": True})

        return jsonify({"ok": True, "message": "ignored"})

    except Exception as e:
        logging.exception("Webhook error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
