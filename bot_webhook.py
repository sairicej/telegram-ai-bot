import ast
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# =========================
# Environment / settings
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Keep this aligned with your render.yaml
MARKETS_URL = os.getenv(
    "MARKETS_URL",
    "https://raw.githubusercontent.com/sairicej/telegram-ai-bot/main/live_feed_markets.txt",
).strip()

# Thresholds
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.20"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.12"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "0"))
BASELINE_PROB = float(os.getenv("BASELINE_PROB", "0.50"))

# Timing / runtime
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "3600"))
SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "300"))

# Optional push-alert target for auto scans
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Polymarket endpoints
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

# In-memory cache for duplicate suppression
sent_cache: Dict[str, float] = {}

# Background worker guard
_background_started = False
_background_lock = threading.Lock()


# =========================
# Helpers
# =========================
def send_telegram_message(chat_id: str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    try:
        requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        print(f"Telegram send error: {e}")


def safe_get_json(url: str, params: Optional[dict] = None) -> Any:
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def cleanup_cache() -> None:
    now = time.time()
    expired = [k for k, v in sent_cache.items() if now - v > DEDUP_SECONDS]
    for k in expired:
        del sent_cache[k]


def already_sent(key: str) -> bool:
    cleanup_cache()
    return key in sent_cache


def mark_sent(key: str) -> None:
    sent_cache[key] = time.time()


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def parse_jsonish_list(value: Any) -> List[Any]:
    """
    Handles:
    - real lists
    - JSON-style strings like '["a","b"]'
    - Python-style strings like "['a', 'b']"
    - comma-separated strings
    """
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []

        # Try literal parsing first
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

        # Try basic comma split fallback
        if "," in s:
            return [x.strip().strip('"').strip("'") for x in s.split(",") if x.strip()]

        return [s]

    return []


def parse_watchlist_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Supports:
      slug
      slug|0.50
    """
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    parts = [p.strip() for p in raw.split("|")]
    slug = parts[0]
    baseline = BASELINE_PROB

    if len(parts) >= 2 and parts[1]:
        baseline = to_float(parts[1], BASELINE_PROB)

    return {
        "slug": slug,
        "baseline_prob": baseline,
    }


def fetch_watchlist() -> List[Dict[str, Any]]:
    try:
        r = requests.get(MARKETS_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        lines = [x.strip() for x in r.text.splitlines() if x.strip()]
        items = []
        for line in lines:
            parsed = parse_watchlist_line(line)
            if parsed:
                items.append(parsed)
        return items
    except Exception as e:
        print(f"Watchlist fetch error: {e}")
        return []


# =========================
# Polymarket live fetch
# =========================
def fetch_market_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    try:
        return safe_get_json(f"{GAMMA_BASE}/markets/slug/{slug}")
    except Exception:
        return None


def fetch_event_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    try:
        return safe_get_json(f"{GAMMA_BASE}/events/slug/{slug}")
    except Exception:
        return None


def pick_best_market_from_event(event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    If the watchlist line is an event slug, choose the most liquid active/open child market.
    """
    markets = event_data.get("markets") or []
    if not isinstance(markets, list) or not markets:
        return None

    filtered = []
    for m in markets:
        active = bool(m.get("active", True))
        closed = bool(m.get("closed", False))
        liquidity = to_float(m.get("liquidity"), 0.0)

        if active and not closed:
            filtered.append((liquidity, m))

    if filtered:
        filtered.sort(key=lambda x: x[0], reverse=True)
        return filtered[0][1]

    # fallback: highest liquidity even if status flags are odd
    all_markets = [(to_float(m.get("liquidity"), 0.0), m) for m in markets]
    all_markets.sort(key=lambda x: x[0], reverse=True)
    return all_markets[0][1] if all_markets else None


def extract_yes_token_id(market_data: Dict[str, Any]) -> Optional[str]:
    """
    Polymarket docs state the first clobTokenIds entry is the Yes token.
    """
    token_ids = parse_jsonish_list(market_data.get("clobTokenIds"))
    if token_ids:
        return str(token_ids[0])

    # Very defensive fallback
    token_id = market_data.get("clobTokenId") or market_data.get("asset_id")
    if token_id:
        return str(token_id)

    return None


def fetch_order_book(token_id: str) -> Dict[str, Any]:
    return safe_get_json(f"{CLOB_BASE}/book", params={"token_id": token_id})


def resolve_slug_to_market(slug: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Tries market slug first, then event slug.
    Returns (market_data, source_type)
    """
    market = fetch_market_by_slug(slug)
    if market:
        return market, "market"

    event = fetch_event_by_slug(slug)
    if event:
        best_market = pick_best_market_from_event(event)
        if best_market:
            return best_market, "event"

    return None, None


def choose_live_prob(best_bid: float, best_ask: float, last_trade: float) -> float:
    """
    Polymarket midpoint is the implied probability, but when spread > 0.10
    Polymarket displays last traded price instead. We mirror that.
    """
    spread = round(best_ask - best_bid, 6) if best_bid > 0 and best_ask > 0 else 0.0

    if best_bid > 0 and best_ask > 0:
        midpoint = round((best_bid + best_ask) / 2, 6)
        if spread > 0.10 and last_trade > 0:
            return last_trade
        return midpoint

    if last_trade > 0:
        return last_trade

    if best_bid > 0:
        return best_bid

    if best_ask > 0:
        return best_ask

    return 0.0


def fetch_live_market_data(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Watchlist item shape:
      {"slug": "...", "baseline_prob": 0.50}
    """
    slug = item["slug"]
    baseline_prob = to_float(item.get("baseline_prob"), BASELINE_PROB)

    market_data, source_type = resolve_slug_to_market(slug)
    if not market_data:
        raise ValueError(f"Slug not found in Gamma API: {slug}")

    token_id = extract_yes_token_id(market_data)
    if not token_id:
        raise ValueError(f"No yes token ID found for slug: {slug}")

    book = fetch_order_book(token_id)

    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = to_float(bids[0].get("price")) if bids else 0.0
    best_ask = to_float(asks[0].get("price")) if asks else 0.0
    last_trade = to_float(book.get("last_trade_price"), 0.0)

    spread = round(best_ask - best_bid, 6) if best_bid > 0 and best_ask > 0 else 0.0
    live_prob = choose_live_prob(best_bid, best_ask, last_trade)

    liquidity = to_float(
        market_data.get("liquidityClob", market_data.get("liquidity", 0.0)),
        0.0,
    )

    label = (
        market_data.get("question")
        or market_data.get("title")
        or market_data.get("slug")
        or slug
    )

    return {
        "market_id": market_data.get("id", slug),
        "slug": slug,
        "source_type": source_type,
        "label": label,
        "live_prob": live_prob,
        "baseline_prob": baseline_prob,
        "liquidity": liquidity,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "last_trade_price": last_trade,
        "token_id": token_id,
    }


# =========================
# Classification / scan
# =========================
def classify_market(m: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    liquidity = to_float(m.get("liquidity", 0), 0.0)
    if liquidity < MIN_LIQUIDITY:
        return "SKIP", None

    live_prob = to_float(m.get("live_prob", 0.0), 0.0)
    baseline_prob = to_float(m.get("baseline_prob", BASELINE_PROB), BASELINE_PROB)
    imbalance = live_prob - baseline_prob

    m["imbalance"] = imbalance

    if imbalance <= ALERT_THRESHOLD:
        return "ALERT", f"Strong signal: {imbalance:.3f}"
    if imbalance <= WATCH_THRESHOLD:
        return "WATCH", f"Watch signal: {imbalance:.3f}"
    return "SKIP", None


def scan_markets() -> Dict[str, Any]:
    watchlist = fetch_watchlist()
    results: List[Dict[str, Any]] = []
    counts = {"total": 0, "alert": 0, "watch": 0, "skip": 0}

    for item in watchlist:
        counts["total"] += 1
        try:
            m = fetch_live_market_data(item)
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
            print(f"Market scan error for {item}: {e}")
            results.append(
                {
                    "market_id": item.get("slug"),
                    "slug": item.get("slug"),
                    "label": item.get("slug"),
                    "category": "SKIP",
                    "reason": f"Fetch error: {e}",
                    "live_prob": 0.0,
                    "baseline_prob": item.get("baseline_prob", BASELINE_PROB),
                    "liquidity": 0.0,
                    "best_bid": 0.0,
                    "best_ask": 0.0,
                    "spread": 0.0,
                    "imbalance": 0.0,
                }
            )
            counts["skip"] += 1

    results.sort(key=lambda x: x.get("imbalance", 999))
    return {"counts": counts, "results": results[:10]}


def format_scan_summary(scan: Dict[str, Any]) -> str:
    counts = scan["counts"]
    lines = [
        "Scan complete",
        f"Total: {counts['total']}",
        f"Alerts: {counts['alert']}",
        f"Watch: {counts['watch']}",
        f"Skip: {counts['skip']}",
        "",
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
            f"bid={r.get('best_bid', 0):.3f} "
            f"ask={r.get('best_ask', 0):.3f} "
            f"spread={r.get('spread', 0):.3f} "
            f"liq={r.get('liquidity', 0):.0f}"
        )

    return "\n\n".join(lines)


def format_auto_alert(r: Dict[str, Any]) -> str:
    return (
        f"{r['category']} | {r['label']}\n"
        f"slug={r.get('slug', '')}\n"
        f"live={r.get('live_prob', 0):.3f} "
        f"base={r.get('baseline_prob', 0):.3f} "
        f"diff={r.get('imbalance', 0):.3f}\n"
        f"bid={r.get('best_bid', 0):.3f} "
        f"ask={r.get('best_ask', 0):.3f} "
        f"spread={r.get('spread', 0):.3f} "
        f"liq={r.get('liquidity', 0):.0f}"
    )


# =========================
# Background live scanner
# =========================
def auto_scan_loop() -> None:
    if not TELEGRAM_CHAT_ID:
        print("Auto scan disabled: TELEGRAM_CHAT_ID not set")
        return

    if SCAN_EVERY_SECONDS <= 0:
        print("Auto scan disabled: SCAN_EVERY_SECONDS <= 0")
        return

    print(f"Auto scan loop started. Every {SCAN_EVERY_SECONDS} seconds.")

    while True:
        try:
            scan = scan_markets()
            candidates = []

            for r in scan["results"]:
                if r["category"] == "ALERT":
                    candidates.append(r)
                elif r["category"] == "WATCH" and SEND_WATCH_ALERTS:
                    candidates.append(r)

            for r in candidates:
                dedupe_key = f"{r.get('slug')}|{r.get('category')}"
                if already_sent(dedupe_key):
                    continue

                send_telegram_message(TELEGRAM_CHAT_ID, format_auto_alert(r))
                mark_sent(dedupe_key)

        except Exception as e:
            print(f"Auto scan error: {e}")

        time.sleep(SCAN_EVERY_SECONDS)


def start_background_worker_once() -> None:
    global _background_started
    with _background_lock:
        if _background_started:
            return
        _background_started = True

        t = threading.Thread(target=auto_scan_loop, daemon=True)
        t.start()


start_background_worker_once()


# =========================
# Flask routes
# =========================
@app.route("/", methods=["GET"])
def home():
    return jsonify(
        {
            "ok": True,
            "service": "telegram-market-bot",
            "webhook_route": "/webhook",
            "markets_url": MARKETS_URL,
            "alert_threshold": ALERT_THRESHOLD,
            "watch_threshold": WATCH_THRESHOLD,
            "send_watch_alerts": SEND_WATCH_ALERTS,
            "min_liquidity": MIN_LIQUIDITY,
            "baseline_prob": BASELINE_PROB,
            "scan_every_seconds": SCAN_EVERY_SECONDS,
            "auto_scan_enabled": bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0),
        }
    )


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    message = data.get("message", {})
    chat = message.get("chat", {})
    chat_id = str(chat.get("id", "")).strip()
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
            f"min_liquidity={MIN_LIQUIDITY}\n"
            f"baseline_prob={BASELINE_PROB}\n"
            f"scan_every_seconds={SCAN_EVERY_SECONDS}\n"
            f"auto_scan_enabled={bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0)}"
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
