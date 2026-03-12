import os
import time
import json
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
MARKETS_URL = os.getenv(
    "MARKETS_URL",
    "https://raw.githubusercontent.com/sairicej/telegram-ai-bot/main/live_feed_markets.txt"
).strip()

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.20"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.12"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "0"))

# suppress duplicate alerts for same market/category for a short window
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "3600"))
sent_cache = {}

def send_telegram_message(chat_id, text):
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )
    except Exception as e:
        print(f"Telegram send error: {e}")

def cleanup_cache():
    now = time.time()
    expired = [k for k, v in sent_cache.items() if now - v > DEDUP_SECONDS]
    for k in expired:
        del sent_cache[k]

def already_sent(key):
    cleanup_cache()
    return key in sent_cache

def mark_sent(key):
    sent_cache[key] = time.time()

def fetch_watchlist():
    try:
        r = requests.get(MARKETS_URL, timeout=20)
        r.raise_for_status()
        lines = [x.strip() for x in r.text.splitlines() if x.strip()]
        return lines
    except Exception as e:
        print(f"Watchlist fetch error: {e}")
        return []

def fetch_live_market_data(market_id):
    """
    Replace this with your live market endpoint logic.
    Must return a dict with:
    market_id, label, live_prob, baseline_prob, liquidity, best_bid, best_ask, spread
    """
    # placeholder example
    return {
        "market_id": market_id,
        "label": market_id,
        "live_prob": 0.50,
        "baseline_prob": 0.50,
        "liquidity": 1000,
        "best_bid": 0.49,
        "best_ask": 0.51,
        "spread": 0.02,
    }

def classify_market(m):
    liquidity = float(m.get("liquidity", 0) or 0)
    if liquidity < MIN_LIQUIDITY:
        return "SKIP", None

    live_prob = float(m.get("live_prob", 0.5))
    baseline_prob = float(m.get("baseline_prob", 0.5))
    imbalance = live_prob - baseline_prob

    m["imbalance"] = imbalance

    if imbalance <= ALERT_THRESHOLD:
        return "ALERT", f"Strong signal: {imbalance:.3f}"
    if imbalance <= WATCH_THRESHOLD:
        return "WATCH", f"Watch signal: {imbalance:.3f}"
    return "SKIP", None

def scan_markets():
    watchlist = fetch_watchlist()
    results = []
    counts = {"total": 0, "alert": 0, "watch": 0, "skip": 0}

    for market_id in watchlist:
        counts["total"] += 1
        try:
            m = fetch_live_market_data(market_id)
            category, reason = classify_market(m)
            m["category"] = category
            m["reason"] = reason
            results.append(m)

            if category == "ALERT":
                counts["alert"] += 1
            elif category == "WATCH":
                counts["watch"] += 1
            else:
                counts["skip"] += 1

        except Exception as e:
            print(f"Market scan error for {market_id}: {e}")
            counts["skip"] += 1

    results.sort(key=lambda x: x.get("imbalance", 999))
    return {"counts": counts, "results": results[:10]}

def format_scan_summary(scan):
    counts = scan["counts"]
    lines = [
        "Scan complete",
        f"Total: {counts['total']}",
        f"Alerts: {counts['alert']}",
        f"Watch: {counts['watch']}",
        f"Skip: {counts['skip']}",
        ""
    ]

    top = [r for r in scan["results"] if r["category"] in ("ALERT", "WATCH")]
    if not top:
        lines.append("No qualifying markets found.")
        return "\n".join(lines)

    for r in top[:5]:
        lines.append(
            f"{r['category']} | {r['label']}\n"
            f"live={r.get('live_prob', 0):.3f} "
            f"base={r.get('baseline_prob', 0):.3f} "
            f"diff={r.get('imbalance', 0):.3f} "
            f"liq={r.get('liquidity', 0)}"
        )

    return "\n\n".join(lines)

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "service": "telegram-market-bot",
        "webhook_route": "/webhook",
        "markets_url": MARKETS_URL,
        "alert_threshold": ALERT_THRESHOLD,
        "watch_threshold": WATCH_THRESHOLD,
        "send_watch_alerts": SEND_WATCH_ALERTS,
        "min_liquidity": MIN_LIQUIDITY,
    })

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    message = data.get("message", {})
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return jsonify({"ok": True, "ignored": True})

    if text == "/start":
        send_telegram_message(chat_id, "Bot is live.\nUse /scan or /health")
        return jsonify({"ok": True})

    if text == "/health":
        msg = (
            "Health check\n"
            f"markets_url={MARKETS_URL}\n"
            f"alert_threshold={ALERT_THRESHOLD}\n"
            f"watch_threshold={WATCH_THRESHOLD}\n"
            f"send_watch_alerts={SEND_WATCH_ALERTS}\n"
            f"min_liquidity={MIN_LIQUIDITY}"
        )
        send_telegram_message(chat_id, msg)
        return jsonify({"ok": True})

    if text == "/scan":
        try:
            scan = scan_markets()
            send_telegram_message(chat_id, format_scan_summary(scan))
        except Exception as e:
            send_telegram_message(chat_id, f"Scan error: {e}")
        return jsonify({"ok": True})

    send_telegram_message(chat_id, "Unknown command. Use /scan or /health")
    return jsonify({"ok": True})

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
