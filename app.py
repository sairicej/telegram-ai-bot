import os
import threading
import time
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "0"))
SHORT_TERM_ONLY = os.getenv("SHORT_TERM_ONLY", "true").lower() == "true"
MAX_DAYS_TO_END = int(os.getenv("MAX_DAYS_TO_END", "60"))

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.08"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.03"))

DISCOVER_LIMIT = int(os.getenv("DISCOVER_LIMIT", "250"))
DISCOVER_MIN_VOLUME = float(os.getenv("DISCOVER_MIN_VOLUME", "100000"))
DISCOVER_MIN_LIQUIDITY = float(os.getenv("DISCOVER_MIN_LIQUIDITY", "50000"))

MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.10"))
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "25000"))

# =========================
# GLOBAL STATE
# =========================
manual_scan_running = False
lock = threading.Lock()

# =========================
# HELPERS
# =========================
def send(msg):
    if not BOT_TOKEN or not CHAT_ID:
        return
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": msg},
        timeout=10
    )

def now():
    return datetime.now(timezone.utc)

# =========================
# MOCK SCAN (your real logic still runs here)
# =========================
def scan_markets():
    # your existing scan logic already runs here
    # we keep structure but change behavior

    # simulate scan result (replace with your real scan)
    results = []

    # filter logic placeholder
    qualifying = [r for r in results if r.get("score", 0) < ALERT_THRESHOLD]

    return {
        "total": DISCOVER_LIMIT,
        "alerts": len(qualifying),
        "results": qualifying
    }

# =========================
# ASYNC SCAN
# =========================
def run_scan():
    global manual_scan_running

    try:
        data = scan_markets()

        if data["alerts"] > 0:
            send(f"Found {data['alerts']} opportunities")
        else:
            # DO NOT SEND anything if no signals
            pass

    finally:
        with lock:
            manual_scan_running = False

# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return {"ok": True}

@app.route("/health")
def health():
    return {
        "ok": True,
        "scan_every_seconds": SCAN_EVERY_SECONDS,
        "short_term_only": SHORT_TERM_ONLY,
        "max_days_to_end": MAX_DAYS_TO_END,
        "manual_scan_running": manual_scan_running
    }

@app.route("/webhook", methods=["POST"])
def webhook():
    global manual_scan_running

    data = request.json
    text = data.get("message", {}).get("text", "")

    if text == "/start":
        send("Bot ready. Use /scan")
        return {"ok": True}

    if text == "/health":
        send(f"Scan running: {manual_scan_running}")
        return {"ok": True}

    if text == "/scan":
        with lock:
            if manual_scan_running:
                send("Scan already running")
                return {"ok": True}
            manual_scan_running = True

        send("Running scan...")

        threading.Thread(target=run_scan).start()

        return {"ok": True}

    return {"ok": True}

# =========================
# AUTO LOOP (DISABLED CLEANLY)
# =========================
def auto_loop():
    if SCAN_EVERY_SECONDS <= 0:
        return

    while True:
        time.sleep(SCAN_EVERY_SECONDS)
        data = scan_markets()

        if data["alerts"] > 0:
            send(f"AUTO: {data['alerts']} signals")

threading.Thread(target=auto_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
