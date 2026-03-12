import os
import re
import json
import math
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# =========================
# ENV HELPERS
# =========================
def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
MARKET_LIST_URL = env_str("MARKET_LIST_URL")
ALERT_THRESHOLD = env_float("ALERT_THRESHOLD", -0.35)
WATCH_THRESHOLD = env_float("WATCH_THRESHOLD", -0.20)
MIN_LIQUIDITY = env_float("MIN_LIQUIDITY", 50000)
MAX_SPREAD = env_float("MAX_SPREAD", 0.10)
MAX_MARKETS = env_int("MAX_MARKETS", 100)
SEND_WATCH_ALERTS = env_bool("SEND_WATCH_ALERTS", False)
ENABLE_YES_NO_ONLY = env_bool("ENABLE_YES_NO_ONLY", True)
TEST_MARKET_SLUG = env_str("TEST_MARKET_SLUG")
MIN_BID = env_float("MIN_BID", 0.01)
MAX_ASK = env_float("MAX_ASK", 0.99)
REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 20)

if not TELEGRAM_BOT_TOKEN:
    logging.warning("TELEGRAM_BOT_TOKEN is missing.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"


# =========================
# TELEGRAM HELPERS
# =========================
def send_telegram_message(chat_id: str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("Cannot send Telegram message. TELEGRAM_BOT_TOKEN missing.")
        return

    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logging.exception("Telegram send failed: %s", e)


# =========================
# SOURCE LIST HELPERS
# =========================
def slug_from_line(line: str) -> Optional[str]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # full polymarket URL
    if "polymarket.com" in line:
        m = re.search(r"/event/([^/?#]+)", line)
        if m:
            return m.group(1).strip()

    # assume slug if plain text
    return line.strip()


def load_market_slugs() -> List[str]:
    slugs: List[str] = []

    if TEST_MARKET_SLUG:
        return [TEST_MARKET_SLUG]

    if not MARKET_LIST_URL:
        logging.warning("MARKET_LIST_URL missing.")
        return slugs

    try:
        r = requests.get(MARKET_LIST_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        for raw_line in r.text.splitlines():
            slug = slug_from_line(raw_line)
            if slug:
                slugs.append(slug)
    except Exception as e:
        logging.exception("Failed to load market list: %s", e)
        return []

    # de-dupe while preserving order
    seen = set()
    ordered = []
    for s in slugs:
        if s not in seen:
            ordered.append(s)
            seen.add(s)

    return ordered[:MAX_MARKETS]


# =========================
# MARKET FETCH / NORMALIZE
# =========================
def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def normalize_outcomes(raw_market: Dict[str, Any]) -> List[str]:
    # possible shapes:
    # outcomes = '["Yes","No"]'
    # outcomes = ["Yes", "No"]
    # tokens = [{"outcome":"Yes"}, {"outcome":"No"}]
    outcomes = raw_market.get("outcomes")

    if isinstance(outcomes, str):
        try:
            parsed = json.loads(outcomes)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed]
        except Exception:
            pass

    if isinstance(outcomes, list):
        return [str(x).strip() for x in outcomes]

    tokens = raw_market.get("tokens", [])
    if isinstance(tokens, list):
        token_outcomes = []
        for t in tokens:
            outcome = t.get("outcome")
            if outcome is not None:
                token_outcomes.append(str(outcome).strip())
        if token_outcomes:
            return token_outcomes

    return []


def is_yes_no_market(raw_market: Dict[str, Any]) -> bool:
    outcomes = normalize_outcomes(raw_market)
    cleaned = {o.strip().lower() for o in outcomes if o is not None}
    return cleaned == {"yes", "no"}


def normalize_market(raw_market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if ENABLE_YES_NO_ONLY and not is_yes_no_market(raw_market):
        return None

    label = (
        raw_market.get("question")
        or raw_market.get("title")
        or raw_market.get("name")
        or raw_market.get("slug")
        or "Unknown market"
    )

    slug = raw_market.get("slug") or raw_market.get("market_slug") or ""
    market_id = raw_market.get("conditionId") or raw_market.get("id") or slug

    best_bid = (
        safe_float(raw_market.get("bestBid"))
        or safe_float(raw_market.get("best_bid"))
        or safe_float(raw_market.get("bid"))
        or 0.0
    )

    best_ask = (
        safe_float(raw_market.get("bestAsk"))
        or safe_float(raw_market.get("best_ask"))
        or safe_float(raw_market.get("ask"))
        or 0.0
    )

    last_price = (
        safe_float(raw_market.get("lastTradePrice"))
        or safe_float(raw_market.get("last_trade_price"))
        or safe_float(raw_market.get("lastPrice"))
        or safe_float(raw_market.get("price"))
    )

    liquidity = (
        safe_float(raw_market.get("liquidity"))
        or safe_float(raw_market.get("liquidityNum"))
        or safe_float(raw_market.get("volumeNum"))
        or 0.0
    )

    active = raw_market.get("active", True)
    closed = raw_market.get("closed", False)
    archived = raw_market.get("archived", False)

    if closed or archived or not active:
        return None

    spread = None
    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid

    # Use midpoint if bid/ask look usable, otherwise fallback to last price
    live_prob = None
    source = None

    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        live_prob = (best_bid + best_ask) / 2.0
        source = "midpoint"
    elif last_price is not None:
        live_prob = last_price
        source = "last_price"

    if live_prob is None:
        return None

    return {
        "market_id": str(market_id),
        "label": str(label).strip(),
        "slug": str(slug).strip(),
        "best_bid": round(best_bid, 6) if best_bid is not None else None,
        "best_ask": round(best_ask, 6) if best_ask is not None else None,
        "last_price": round(last_price, 6) if last_price is not None else None,
        "spread": round(spread, 6) if spread is not None else None,
        "liquidity": float(liquidity or 0.0),
        "live_prob": round(live_prob, 6),
        "source": source,
        "raw": raw_market,
    }


def fetch_markets_by_slug(slug: str) -> List[Dict[str, Any]]:
    """
    Tries both /markets and /events because Polymarket responses can vary
    depending on endpoint and slug type.
    """
    results: List[Dict[str, Any]] = []

    # Try direct markets lookup
    try:
        r = requests.get(GAMMA_MARKETS_URL, params={"slug": slug}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            for item in data:
                nm = normalize_market(item)
                if nm:
                    results.append(nm)
            if results:
                return results
    except Exception as e:
        logging.info("Market lookup failed for slug=%s via /markets: %s", slug, e)

    # Try event lookup, then expand event markets
    try:
        r = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            for event in data:
                for item in event.get("markets", []):
                    nm = normalize_market(item)
                    if nm:
                        results.append(nm)
    except Exception as e:
        logging.info("Event lookup failed for slug=%s via /events: %s", slug, e)

    return results


# =========================
# SCORING / FILTERING
# =========================
def classify_market(market: Dict[str, Any]) -> Tuple[str, Optional[str], float]:
    """
    Current model:
    - baseline = 0.50 for pure yes/no
    - diff = live_prob - baseline
    - strong negative diff = alert/watch
    """

    live_prob = market["live_prob"]
    baseline_prob = 0.50
    diff = live_prob - baseline_prob

    bid = market.get("best_bid") or 0.0
    ask = market.get("best_ask") or 0.0
    spread = market.get("spread")
    liquidity = market.get("liquidity", 0.0)

    if liquidity < MIN_LIQUIDITY:
        return "SKIP", "low_liquidity", diff

    if spread is None or spread < 0:
        return "SKIP", "bad_spread", diff

    if spread > MAX_SPREAD:
        return "SKIP", "spread_too_wide", diff

    # Skip broken / unusable quotes
    if bid <= MIN_BID and ask >= MAX_ASK:
        return "SKIP", "dead_quotes", diff

    if diff <= ALERT_THRESHOLD:
        return "ALERT", None, diff

    if diff <= WATCH_THRESHOLD:
        return "WATCH", None, diff

    return "SKIP", None, diff


def run_scan() -> Dict[str, Any]:
    slugs = load_market_slugs()
    results: List[Dict[str, Any]] = []

    counts = {
        "total": 0,
        "alert": 0,
        "watch": 0,
        "skip": 0,
    }

    if not slugs:
        return {
            "ok": False,
            "error": "No market slugs found. Check MARKET_LIST_URL or TEST_MARKET_SLUG.",
            "counts": counts,
            "results": [],
        }

    for slug in slugs:
        try:
            markets = fetch_markets_by_slug(slug)
        except Exception as e:
            logging.exception("Fetch failed for %s: %s", slug, e)
            continue

        for market in markets:
            counts["total"] += 1
            category, reason, diff = classify_market(market)

            row = {
                "category": category,
                "reason": reason,
                "label": market["label"],
                "slug": market["slug"],
                "market_id": market["market_id"],
                "live_prob": market["live_prob"],
                "baseline_prob": 0.50,
                "diff": round(diff, 6),
                "best_bid": market.get("best_bid"),
                "best_ask": market.get("best_ask"),
                "last_price": market.get("last_price"),
                "spread": market.get("spread"),
                "liquidity": market.get("liquidity"),
                "source": market.get("source"),
            }

            results.append(row)
            counts[category.lower()] += 1

    # strongest signals first
    results.sort(key=lambda x: x["diff"])

    return {
        "ok": True,
        "counts": counts,
        "results": results,
        "top_candidates": results[:10],
    }


def build_scan_message(scan: Dict[str, Any]) -> str:
    if not scan.get("ok"):
        return f"Scan error: {scan.get('error', 'Unknown error')}"

    counts = scan["counts"]
    results = scan["results"]

    lines = []
    lines.append("Scan complete")
    lines.append("")
    lines.append(f"Total: {counts['total']}")
    lines.append(f"Alerts: {counts['alert']}")
    lines.append(f"Watch: {counts['watch']}")
    lines.append(f"Skip: {counts['skip']}")
    lines.append("")

    shown = 0
    for item in results:
        if item["category"] not in {"ALERT", "WATCH"}:
            continue
        if item["category"] == "WATCH" and not SEND_WATCH_ALERTS:
            continue

        lines.append(f"{item['category']} | {item['label']}")
        lines.append(f"slug={item['slug']}")
        lines.append(
            f"live={item['live_prob']:.3f} base={item['baseline_prob']:.3f} diff={item['diff']:.3f}"
        )
        lines.append(
            f"bid={item['best_bid']:.3f} ask={item['best_ask']:.3f} spread={item['spread']:.3f} liq={item['liquidity']:.0f}"
        )
        lines.append("")
        shown += 1

        if shown >= 10:
            break

    if shown == 0:
        lines.append("No alert-level markets after filters.")

    return "\n".join(lines).strip()


# =========================
# ROUTES
# =========================
@app.route("/", methods=["GET"])
def root():
    return jsonify({"ok": True, "message": "Bot is running"}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200


@app.route("/scan", methods=["GET"])
def scan_http():
    result = run_scan()
    return jsonify(result), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True, silent=True) or {}
        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat", {})
        chat_id = str(chat.get("id", "")).strip()
        text = (message.get("text") or "").strip()

        if not chat_id:
            return jsonify({"ok": True, "message": "No chat id"}), 200

        if text.lower() in {"/start", "start"}:
            send_telegram_message(
                chat_id,
                "Bot is live.\n\nUse /scan to run the market scan."
            )
            return jsonify({"ok": True}), 200

        if text.lower() in {"/scan", "scan"}:
            try:
                scan = run_scan()
                msg = build_scan_message(scan)
                send_telegram_message(chat_id, msg)
            except Exception as e:
                logging.exception("Scan failed: %s", e)
                send_telegram_message(chat_id, f"Scan error: {e}")
            return jsonify({"ok": True}), 200

        send_telegram_message(chat_id, "Use /scan")
        return jsonify({"ok": True}), 200

    except Exception as e:
        logging.exception("Webhook error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
