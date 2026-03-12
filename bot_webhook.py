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

# Option A: put your markets directly in Render env var
LIVE_FEED_JSON = os.getenv("LIVE_FEED_JSON", "").strip()

# Optional fallback source
LIVE_FEED_SOURCE_URL = os.getenv("LIVE_FEED_SOURCE_URL", "").strip()

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.20"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.12"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").strip().lower() == "true"
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "0"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

CLOB_BASE = "https://clob.polymarket.com"


# -----------------------------
# HELPERS
# -----------------------------
def now_utc():
    return datetime.now(timezone.utc).isoformat()


def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram settings missing.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return True
    except Exception as exc:
        logging.exception("Telegram send failed: %s", exc)
        return False


# -----------------------------
# LOAD WATCHLIST
# -----------------------------
def load_markets_from_env():
    if not LIVE_FEED_JSON:
        return None

    try:
        markets = json.loads(LIVE_FEED_JSON)
        if isinstance(markets, list):
            logging.info("Loaded %s markets from LIVE_FEED_JSON", len(markets))
            return markets
        logging.warning("LIVE_FEED_JSON is not a list.")
        return None
    except Exception as exc:
        logging.exception("Failed loading LIVE_FEED_JSON: %s", exc)
        return None


def load_markets_from_url():
    if not LIVE_FEED_SOURCE_URL:
        return None

    try:
        response = requests.get(LIVE_FEED_SOURCE_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        text = response.text.strip()

        if not text:
            logging.warning("LIVE_FEED_SOURCE_URL returned empty content.")
            return None

        # Supports JSON array or line-delimited JSON
        if text.startswith("["):
            markets = json.loads(text)
        else:
            markets = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                markets.append(json.loads(line))

        if isinstance(markets, list):
            logging.info("Loaded %s markets from LIVE_FEED_SOURCE_URL", len(markets))
            return markets

        logging.warning("LIVE_FEED_SOURCE_URL data is not a list.")
        return None
    except Exception as exc:
        logging.exception("Failed loading LIVE_FEED_SOURCE_URL: %s", exc)
        return None


def load_markets():
    # Option A first
    markets = load_markets_from_env()
    if markets:
        return markets

    # Fallback only if env is empty or broken
    markets = load_markets_from_url()
    if markets:
        return markets

    logging.warning("No market list loaded.")
    return []


# -----------------------------
# POLYMARKET LIVE DATA
# -----------------------------
def fetch_orderbook(token_id):
    url = f"{CLOB_BASE}/book"
    params = {"token_id": str(token_id)}
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def compute_live_probability(orderbook):
    bids = orderbook.get("bids", []) or []
    asks = orderbook.get("asks", []) or []

    best_bid = safe_float(bids[0].get("price")) if bids else None
    best_ask = safe_float(asks[0].get("price")) if asks else None

    midpoint = None
    spread = None

    if best_bid is not None and best_ask is not None:
        midpoint = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

    last_price = safe_float(
        orderbook.get("price", orderbook.get("last_trade_price"))
    )

    # Prefer midpoint if available
    if midpoint is not None:
        return {
            "probability": midpoint,
            "source": "midpoint",
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "last_price": last_price,
        }

    # Fallback to last price
    if last_price is not None:
        return {
            "probability": last_price,
            "source": "last_trade",
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "last_price": last_price,
        }

    return {
        "probability": None,
        "source": "missing",
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "last_price": last_price,
    }


def estimate_liquidity(orderbook):
    bids = orderbook.get("bids", []) or []
    asks = orderbook.get("asks", []) or []

    total = 0.0

    for side in bids[:5] + asks[:5]:
        price = safe_float(side.get("price"), 0.0)
        size = safe_float(side.get("size"), 0.0)
        total += price * size

    return total


# -----------------------------
# SCORING
# -----------------------------
def classify_market(baseline_prob, live_prob, liquidity):
    imbalance = live_prob - baseline_prob

    if liquidity < MIN_LIQUIDITY:
        return "SKIP", imbalance

    if imbalance <= ALERT_THRESHOLD:
        return "ALERT", imbalance

    if imbalance <= WATCH_THRESHOLD:
        return "WATCH", imbalance

    return "SKIP", imbalance


def evaluate_market(row):
    market_id = row.get("market_id", "unknown")
    label = row.get("label", market_id)
    token_id = str(row.get("token_id", "")).strip()
    baseline_prob = safe_float(row.get("baseline_prob"))

    if not token_id or baseline_prob is None:
        return {
            "market_id": market_id,
            "label": label,
            "category": "SKIP",
            "reason": "missing_token_or_baseline",
            "input": row,
        }

    try:
        orderbook = fetch_orderbook(token_id)
        live = compute_live_probability(orderbook)
        live_prob = live.get("probability")

        if live_prob is None:
            return {
                "market_id": market_id,
                "label": label,
                "category": "SKIP",
                "reason": "missing_live_probability",
                "baseline_prob": baseline_prob,
                "token_id": token_id,
                "input": row,
            }

        liquidity = estimate_liquidity(orderbook)
        category, imbalance = classify_market(baseline_prob, live_prob, liquidity)

        return {
            "market_id": market_id,
            "label": label,
            "category": category,
            "reason": None,
            "token_id": token_id,
            "baseline_prob": baseline_prob,
            "live_prob": live_prob,
            "imbalance": imbalance,
            "liquidity": liquidity,
            "source": live.get("source"),
            "best_bid": live.get("best_bid"),
            "best_ask": live.get("best_ask"),
            "spread": live.get("spread"),
            "last_price": live.get("last_price"),
        }

    except Exception as exc:
        logging.exception("Market eval failed for %s: %s", market_id, exc)
        return {
            "market_id": market_id,
            "label": label,
            "category": "SKIP",
            "reason": f"error: {str(exc)}",
            "token_id": token_id,
            "baseline_prob": baseline_prob,
            "input": row,
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

    sent = {
        "alert": 0,
        "watch": 0,
    }

    for result in top_candidates:
        category = result.get("category")

        if category == "ALERT" or (category == "WATCH" and SEND_WATCH_ALERTS):
            message = (
                f"📌 {category}\n"
                f"Market: {result.get('label')}\n"
                f"ID: {result.get('market_id')}\n"
                f"Baseline: {result.get('baseline_prob'):.3f}\n"
                f"Live: {result.get('live_prob'):.3f}\n"
                f"Imbalance: {result.get('imbalance'):.3f}\n"
                f"Liquidity: {result.get('liquidity'):.2f}\n"
                f"Source: {result.get('source')}\n"
                f"Spread: {result.get('spread')}\n"
                f"Time UTC: {now_utc()}"
            )

            ok = send_telegram_message(message)
            if ok:
                if category == "ALERT":
                    sent["alert"] += 1
                elif category == "WATCH":
                    sent["watch"] += 1

    return {
        "ok": True,
        "timestamp_utc": now_utc(),
        "counts": counts,
        "sent": sent,
        "top_candidates": top_candidates,
        "results": results,
    }


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "message": "Bot is live",
        "timestamp_utc": now_utc(),
    })


@app.route("/scan", methods=["GET", "POST"])
def scan():
    try:
        result = run_scan()
        return jsonify(result)
    except Exception as exc:
        logging.exception("Scan error: %s", exc)
        return jsonify({
            "ok": False,
            "error": str(exc),
            "timestamp_utc": now_utc(),
        }), 500


@app.route("/webhook", methods=["POST"])
def webhook():
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

        return jsonify({
            "ok": True,
            "message": "ignored",
        })

    except Exception as exc:
        logging.exception("Webhook error: %s", exc)
        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
