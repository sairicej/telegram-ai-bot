import os
import json
import math
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify

# =========================
# CONFIG
# =========================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Public URL of your live feed file
# IMPORTANT:
# raw GitHub format should be:
# https://raw.githubusercontent.com/<owner>/<repo>/<branch>/<file>
# not .../refs/heads/...
LIVE_FEED_URL = os.getenv(
    "LIVE_FEED_URL",
    "https://raw.githubusercontent.com/sairicej/telegram-ai-bot/main/live_feed_markets.txt",
).strip()

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.20"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.12"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "0"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
PORT = int(os.getenv("PORT", "10000"))

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({"User-Agent": "telegram-polymarket-bot/1.0"})

# =========================
# HELPERS
# =========================

def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def fetch_json(url: str, params: Optional[dict] = None) -> Any:
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def post_json(url: str, payload: dict) -> Any:
    resp = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def telegram_api(method: str, payload: Optional[dict] = None) -> dict:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    resp = session.post(url, json=payload or {}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok", False):
        raise RuntimeError(f"Telegram API error: {data}")
    return data


def send_telegram_message(text: str, chat_id: Optional[str] = None) -> dict:
    target_chat_id = (chat_id or TELEGRAM_CHAT_ID).strip()
    if not target_chat_id:
        raise RuntimeError("Missing TELEGRAM_CHAT_ID")
    payload = {
        "chat_id": target_chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    return telegram_api("sendMessage", payload)


def set_telegram_webhook(base_url: str) -> dict:
    base_url = base_url.rstrip("/")
    webhook_url = f"{base_url}/webhook"
    payload = {
        "url": webhook_url,
        "drop_pending_updates": True
    }
    return telegram_api("setWebhook", payload)


def get_webhook_info() -> dict:
    return telegram_api("getWebhookInfo", {})


# =========================
# LIVE FEED FILE PARSING
# =========================

def parse_feed_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Supported formats per line:

    1) market_id|slug|baseline_prob
    2) market_id,slug,baseline_prob
    3) slug
    4) market_id|slug
    5) market_id,slug

    baseline_prob defaults to 0.5
    """
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    delimiter = "|" if "|" in raw else ","
    parts = [p.strip() for p in raw.split(delimiter)]

    if len(parts) == 1:
        slug = parts[0]
        return {
            "market_id": slug.upper().replace("-", "_"),
            "slug": slug,
            "baseline_prob": 0.5,
        }

    if len(parts) == 2:
        market_id, slug = parts
        return {
            "market_id": market_id,
            "slug": slug,
            "baseline_prob": 0.5,
        }

    market_id, slug, baseline_prob = parts[0], parts[1], parts[2]
    return {
        "market_id": market_id,
        "slug": slug,
        "baseline_prob": safe_float(baseline_prob, 0.5),
    }


def load_watchlist() -> List[Dict[str, Any]]:
    resp = session.get(LIVE_FEED_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    items: List[Dict[str, Any]] = []
    for line in resp.text.splitlines():
        parsed = parse_feed_line(line)
        if parsed:
            items.append(parsed)

    if not items:
        raise RuntimeError("No valid watchlist items found in live_feed_markets.txt")

    return items


# =========================
# POLYMARKET DATA
# =========================

def get_market_by_slug(slug: str) -> Dict[str, Any]:
    """
    Gamma API market by slug:
    GET /markets/slug/{slug}
    """
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    return fetch_json(url)


def extract_yes_token_id(market: Dict[str, Any]) -> Optional[str]:
    """
    Tries several common fields to locate a usable token/outcome id.
    """
    candidates = [
        market.get("clobTokenIds"),
        market.get("outcomeTokenIds"),
        market.get("tokenIds"),
    ]

    for candidate in candidates:
        if isinstance(candidate, list) and candidate:
            return str(candidate[0])

        if isinstance(candidate, str):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, list) and parsed:
                    return str(parsed[0])
            except Exception:
                pass

    outcomes = market.get("outcomes")
    if isinstance(outcomes, list) and outcomes:
        first = outcomes[0]
        if isinstance(first, dict):
            for key in ["tokenId", "id"]:
                if key in first and first[key]:
                    return str(first[key])

    return None


def normalize_market(market_meta: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    market_id = market_meta["market_id"]
    baseline_prob = safe_float(market_meta.get("baseline_prob"), 0.5) or 0.5

    question = (
        market.get("question")
        or market.get("title")
        or market.get("slug")
        or market_meta["slug"]
    )

    best_bid = safe_float(
        market.get("bestBid")
        or market.get("bid")
        or market.get("yesBid")
    )

    best_ask = safe_float(
        market.get("bestAsk")
        or market.get("ask")
        or market.get("yesAsk")
    )

    last_price = safe_float(
        market.get("lastTradePrice")
        or market.get("lastPrice")
        or market.get("price")
        or market.get("yesPrice")
    )

    liquidity = safe_float(market.get("liquidity"), 0.0) or 0.0

    midpoint = None
    source = None
    spread = None

    if best_bid is not None and best_ask is not None:
        midpoint = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        source = "midpoint"

    if midpoint is None and last_price is not None:
        midpoint = last_price
        spread = None
        source = "last_price"

    live_prob = midpoint if midpoint is not None else baseline_prob
    imbalance = live_prob - baseline_prob

    category = "SKIP"
    if liquidity >= MIN_LIQUIDITY:
        if imbalance <= ALERT_THRESHOLD:
            category = "ALERT"
        elif imbalance <= WATCH_THRESHOLD:
            category = "WATCH"

    token_id = extract_yes_token_id(market)

    return {
        "market_id": market_id,
        "label": question,
        "slug": market.get("slug") or market_meta["slug"],
        "baseline_prob": round(baseline_prob, 6),
        "live_prob": round(live_prob, 6),
        "imbalance": round(imbalance, 6),
        "best_bid": None if best_bid is None else round(best_bid, 6),
        "best_ask": None if best_ask is None else round(best_ask, 6),
        "last_price": None if last_price is None else round(last_price, 6),
        "spread": None if spread is None else round(spread, 6),
        "liquidity": round(liquidity, 6),
        "token_id": token_id,
        "source": source,
        "category": category,
        "reason": None,
    }


def run_scan() -> Dict[str, Any]:
    watchlist = load_watchlist()
    results: List[Dict[str, Any]] = []

    for item in watchlist:
        try:
            market = get_market_by_slug(item["slug"])
            normalized = normalize_market(item, market)
            results.append(normalized)
        except Exception as exc:
            logger.exception("Failed to scan %s", item)
            results.append({
                "market_id": item.get("market_id"),
                "label": item.get("slug"),
                "slug": item.get("slug"),
                "baseline_prob": item.get("baseline_prob", 0.5),
                "live_prob": item.get("baseline_prob", 0.5),
                "imbalance": 0.0,
                "best_bid": None,
                "best_ask": None,
                "last_price": None,
                "spread": None,
                "liquidity": 0.0,
                "token_id": None,
                "source": None,
                "category": "SKIP",
                "reason": f"scan_error: {str(exc)}",
            })

    counts = {
        "alert": sum(1 for r in results if r["category"] == "ALERT"),
        "watch": sum(1 for r in results if r["category"] == "WATCH"),
        "skip": sum(1 for r in results if r["category"] == "SKIP"),
        "total": len(results),
    }

    sorted_results = sorted(
        results,
        key=lambda x: abs(x.get("imbalance") or 0.0),
        reverse=True
    )

    sent = {"alert": 0, "watch": 0}

    for r in sorted_results:
        if r["category"] == "ALERT":
            try:
                send_telegram_message(format_alert_message(r))
                sent["alert"] += 1
            except Exception as exc:
                logger.exception("Telegram alert send failed: %s", exc)
        elif r["category"] == "WATCH" and SEND_WATCH_ALERTS:
            try:
                send_telegram_message(format_watch_message(r))
                sent["watch"] += 1
            except Exception as exc:
                logger.exception("Telegram watch send failed: %s", exc)

    return {
        "ok": True,
        "timestamp_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "counts": counts,
        "results": results,
        "top_candidates": sorted_results[:10],
        "sent": sent,
    }


# =========================
# MESSAGE FORMATTING
# =========================

def pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_alert_message(r: Dict[str, Any]) -> str:
    return (
        f"ALERT\n"
        f"{r['label']}\n"
        f"market_id: {r['market_id']}\n"
        f"live_prob: {pct(r['live_prob'])}\n"
        f"baseline_prob: {pct(r['baseline_prob'])}\n"
        f"imbalance: {pct(r['imbalance'])}\n"
        f"best_bid: {pct(r['best_bid'])}\n"
        f"best_ask: {pct(r['best_ask'])}\n"
        f"last_price: {pct(r['last_price'])}\n"
        f"liquidity: {r['liquidity']}\n"
        f"spread: {pct(r['spread'])}\n"
        f"source: {r['source']}\n"
        f"slug: {r['slug']}"
    )


def format_watch_message(r: Dict[str, Any]) -> str:
    return (
        f"WATCH\n"
        f"{r['label']}\n"
        f"market_id: {r['market_id']}\n"
        f"live_prob: {pct(r['live_prob'])}\n"
        f"baseline_prob: {pct(r['baseline_prob'])}\n"
        f"imbalance: {pct(r['imbalance'])}\n"
        f"liquidity: {r['liquidity']}\n"
        f"slug: {r['slug']}"
    )


def format_scan_summary(scan: Dict[str, Any]) -> str:
    counts = scan["counts"]
    top = scan.get("top_candidates", [])[:3]

    lines = [
        "Scan complete",
        f"alerts: {counts['alert']}",
        f"watch: {counts['watch']}",
        f"skip: {counts['skip']}",
        f"total: {counts['total']}",
        "",
        "Top candidates:"
    ]

    if not top:
        lines.append("none")
    else:
        for item in top:
            lines.append(
                f"- {item['market_id']} | {item['category']} | imbalance {pct(item['imbalance'])} | liquidity {item['liquidity']}"
            )

    return "\n".join(lines)


# =========================
# TELEGRAM UPDATE HANDLING
# =========================

def extract_message(update: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (chat_id, text)
    """
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return None, None

    chat = msg.get("chat", {})
    chat_id = str(chat.get("id")) if chat.get("id") is not None else None
    text = msg.get("text") or ""
    return chat_id, text.strip()


def handle_command(chat_id: str, text: str) -> None:
    lower = text.lower()

    if lower.startswith("/start"):
        send_telegram_message(
            "Bot is live.\nCommands:\n/scan\n/webhookinfo\n/health",
            chat_id=chat_id
        )
        return

    if lower.startswith("/health"):
        send_telegram_message("ok", chat_id=chat_id)
        return

    if lower.startswith("/webhookinfo"):
        info = get_webhook_info()
        send_telegram_message(json.dumps(info, indent=2), chat_id=chat_id)
        return

    if lower.startswith("/scan"):
        try:
            scan = run_scan()
            send_telegram_message(format_scan_summary(scan), chat_id=chat_id)
        except Exception as exc:
            logger.exception("Scan failed")
            send_telegram_message(f"Scan error: {str(exc)}", chat_id=chat_id)
        return

    send_telegram_message(
        "Unknown command.\nUse /scan, /webhookinfo, or /health",
        chat_id=chat_id
    )


# =========================
# FLASK ROUTES
# =========================

@app.get("/")
def home():
    return jsonify({
        "ok": True,
        "service": "telegram-polymarket-bot",
        "webhook_path": "/webhook"
    })


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/webhook")
def webhook_get():
    # This prevents confusion when you manually open /webhook in browser.
    return jsonify({
        "ok": True,
        "message": "Webhook endpoint is live. Telegram should POST here."
    })


@app.post("/webhook")
def webhook_post():
    try:
        update = request.get_json(silent=True) or {}
        logger.info("Webhook update received: %s", json.dumps(update)[:2000])

        chat_id, text = extract_message(update)
        if chat_id and text:
            handle_command(chat_id, text)

        return jsonify({"ok": True})
    except Exception as exc:
        logger.exception("Webhook processing error")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/scan")
def scan_route():
    try:
        result = run_scan()
        return jsonify(result)
    except Exception as exc:
        logger.exception("Manual scan route error")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/set-webhook")
def set_webhook_route():
    try:
        data = request.get_json(silent=True) or {}
        base_url = (data.get("base_url") or os.getenv("RENDER_EXTERNAL_URL") or "").strip()
        if not base_url:
            return jsonify({
                "ok": False,
                "error": "Provide base_url in JSON body or set RENDER_EXTERNAL_URL env var"
            }), 400

        result = set_telegram_webhook(base_url)
        return jsonify(result)
    except Exception as exc:
        logger.exception("Set webhook error")
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    logger.info("Starting app on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT)
