import ast
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# =========================================
# ENV / SETTINGS
# =========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

MARKETS_URL = os.getenv(
    "MARKETS_URL",
    "https://raw.githubusercontent.com/sairicej/telegram-ai-bot/main/live_feed_markets.txt",
).strip()

TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BOT_LABEL = os.getenv("BOT_LABEL", "telegram-market-bot").strip()

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.35"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.25"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"

BASELINE_PROB = float(os.getenv("BASELINE_PROB", "0.50"))
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "100000"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.05"))
MIN_BID = float(os.getenv("MIN_BID", "0.01"))
MAX_ASK = float(os.getenv("MAX_ASK", "0.99"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "3600"))
SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "300"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))

AUTO_DISCOVER = os.getenv("AUTO_DISCOVER", "false").lower() == "true"
DISCOVER_LIMIT = int(os.getenv("DISCOVER_LIMIT", "100"))
DISCOVER_PAGE_SIZE = int(os.getenv("DISCOVER_PAGE_SIZE", "100"))
DISCOVER_MIN_VOLUME = float(os.getenv("DISCOVER_MIN_VOLUME", "250000"))
DISCOVER_MIN_LIQUIDITY = float(os.getenv("DISCOVER_MIN_LIQUIDITY", "100000"))
DISCOVER_KEYWORDS = os.getenv(
    "DISCOVER_KEYWORDS",
    "bitcoin,btc,ethereum,eth,solana,sol,crypto,election,politics,president,primary,senate,house",
).strip()

ENABLE_YES_NO_ONLY = os.getenv("ENABLE_YES_NO_ONLY", "true").lower() == "true"
TEST_MARKET_SLUG = os.getenv("TEST_MARKET_SLUG", "").strip()

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

sent_cache: Dict[str, float] = {}
_background_started = False
_background_lock = threading.Lock()


# =========================================
# BASICS
# =========================================
def send_telegram_message(chat_id: str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return

    try:
        requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        print(f"Telegram send error: {e}")


def safe_get_json(url: str, params: Optional[dict] = None) -> Any:
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


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


def parse_jsonish_list(value: Any) -> List[Any]:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []

        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

        if "," in s:
            return [x.strip().strip('"').strip("'") for x in s.split(",") if x.strip()]

        return [s]

    return []


# =========================================
# MARKET STRUCTURE HELPERS
# =========================================
def normalize_outcomes(market_data: Dict[str, Any]) -> List[str]:
    outcomes = market_data.get("outcomes")
    if outcomes:
        parsed = parse_jsonish_list(outcomes)
        if parsed:
            return [str(x).strip() for x in parsed if str(x).strip()]

    tokens = market_data.get("tokens") or []
    if isinstance(tokens, list):
        token_outcomes = []
        for t in tokens:
            outcome = t.get("outcome")
            if outcome is not None:
                token_outcomes.append(str(outcome).strip())
        if token_outcomes:
            return token_outcomes

    return []


def is_yes_no_market(market_data: Dict[str, Any]) -> bool:
    outcomes = normalize_outcomes(market_data)
    cleaned = {x.lower() for x in outcomes if x}
    return cleaned == {"yes", "no"}


# =========================================
# WATCHLIST INPUT
# =========================================
def parse_watchlist_line(line: str) -> Optional[Dict[str, Any]]:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    parts = [p.strip() for p in raw.split("|")]
    slug = parts[0]
    baseline = BASELINE_PROB

    if len(parts) >= 2 and parts[1]:
        baseline = to_float(parts[1], BASELINE_PROB)

    return {"slug": slug, "baseline_prob": baseline}


def fetch_watchlist_file() -> List[Dict[str, Any]]:
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


def keyword_list() -> List[str]:
    return [x.strip().lower() for x in DISCOVER_KEYWORDS.split(",") if x.strip()]


def matches_keywords(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in keyword_list())


def discover_markets() -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    seen = set()
    offset = 0

    while len(found) < DISCOVER_LIMIT:
        params = {
            "active": "true",
            "closed": "false",
            "limit": DISCOVER_PAGE_SIZE,
            "offset": offset,
            "liquidity_num_min": DISCOVER_MIN_LIQUIDITY,
            "volume_num_min": DISCOVER_MIN_VOLUME,
        }

        try:
            batch = safe_get_json(f"{GAMMA_BASE}/markets", params=params)
        except Exception as e:
            print(f"Discover markets error: {e}")
            break

        if not isinstance(batch, list) or not batch:
            break

        for m in batch:
            slug = (m.get("slug") or "").strip()
            question = (m.get("question") or m.get("title") or "").strip()

            if not slug or slug in seen:
                continue

            if ENABLE_YES_NO_ONLY and not is_yes_no_market(m):
                continue

            if not matches_keywords(f"{slug} {question}"):
                continue

            seen.add(slug)
            found.append(
                {
                    "slug": slug,
                    "baseline_prob": BASELINE_PROB,
                }
            )

            if len(found) >= DISCOVER_LIMIT:
                break

        offset += DISCOVER_PAGE_SIZE

    return found


def fetch_watchlist() -> List[Dict[str, Any]]:
    if TEST_MARKET_SLUG:
        return [{"slug": TEST_MARKET_SLUG, "baseline_prob": BASELINE_PROB}]

    if AUTO_DISCOVER:
        discovered = discover_markets()
        if discovered:
            return discovered

    return fetch_watchlist_file()


# =========================================
# POLYMARKET LOOKUPS
# =========================================
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
    markets = event_data.get("markets") or []
    if not isinstance(markets, list) or not markets:
        return None

    filtered = []
    for m in markets:
        active = bool(m.get("active", True))
        closed = bool(m.get("closed", False))
        liquidity = to_float(m.get("liquidityClob", m.get("liquidity", 0.0)), 0.0)

        if ENABLE_YES_NO_ONLY and not is_yes_no_market(m):
            continue

        if active and not closed:
            filtered.append((liquidity, m))

    if filtered:
        filtered.sort(key=lambda x: x[0], reverse=True)
        return filtered[0][1]

    return None


def extract_yes_token_id(market_data: Dict[str, Any]) -> Optional[str]:
    token_ids = parse_jsonish_list(market_data.get("clobTokenIds"))
    outcomes = normalize_outcomes(market_data)

    if token_ids and len(token_ids) >= 1:
        if outcomes and len(outcomes) == len(token_ids):
            for outcome, token_id in zip(outcomes, token_ids):
                if str(outcome).strip().lower() == "yes":
                    return str(token_id)
        return str(token_ids[0])

    token_id = market_data.get("clobTokenId") or market_data.get("asset_id")
    if token_id:
        return str(token_id)

    return None


def fetch_order_book(token_id: str) -> Dict[str, Any]:
    return safe_get_json(f"{CLOB_BASE}/book", params={"token_id": token_id})


def resolve_slug_to_market(slug: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    market = fetch_market_by_slug(slug)
    if market:
        if ENABLE_YES_NO_ONLY and not is_yes_no_market(market):
            return None, None
        return market, "market"

    event = fetch_event_by_slug(slug)
    if event:
        best_market = pick_best_market_from_event(event)
        if best_market:
            return best_market, "event"

    return None, None


def choose_live_prob(best_bid: float, best_ask: float, last_trade: float) -> Tuple[float, str]:
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        midpoint = round((best_bid + best_ask) / 2, 6)
        return midpoint, "midpoint"

    if last_trade > 0:
        return last_trade, "last_trade"

    if best_bid > 0:
        return best_bid, "best_bid"

    if best_ask > 0:
        return best_ask, "best_ask"

    return 0.0, "none"


def fetch_live_market_data(item: Dict[str, Any]) -> Dict[str, Any]:
    slug = item["slug"]
    baseline_prob = to_float(item.get("baseline_prob"), BASELINE_PROB)

    market_data, source_type = resolve_slug_to_market(slug)
    if not market_data:
        raise ValueError(f"Slug not found or not clean yes/no: {slug}")

    token_id = extract_yes_token_id(market_data)
    if not token_id:
        raise ValueError(f"No YES token ID found for slug: {slug}")

    book = fetch_order_book(token_id)

    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = to_float(bids[0].get("price")) if bids else 0.0
    best_ask = to_float(asks[0].get("price")) if asks else 0.0
    last_trade = to_float(book.get("last_trade_price"), 0.0)
    spread = round(best_ask - best_bid, 6) if best_bid > 0 and best_ask > 0 else 0.0
    live_prob, price_source = choose_live_prob(best_bid, best_ask, last_trade)
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
        "price_source": price_source,
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


# =========================================
# CLASSIFICATION
# =========================================
def classify_market(m: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    liquidity = to_float(m.get("liquidity", 0), 0.0)
    if liquidity < MIN_LIQUIDITY:
        return "SKIP", "Liquidity too low"

    spread = to_float(m.get("spread", 0), 0.0)
    if spread > MAX_SPREAD:
        return "SKIP", "Spread too wide"

    best_bid = to_float(m.get("best_bid", 0), 0.0)
    best_ask = to_float(m.get("best_ask", 0), 0.0)
    price_source = (m.get("price_source") or "").strip()

    # Skip incomplete / broken books
    if best_bid <= 0 or best_ask <= 0:
        return "SKIP", "Incomplete book"

    # Skip last trade fallback when book is not usable
    if price_source == "last_trade":
        return "SKIP", "Last trade only"

    # Skip dead books
    if best_bid <= MIN_BID and best_ask >= MAX_ASK:
        return "SKIP", "Dead quote"

    live_prob = to_float(m.get("live_prob", 0.0), 0.0)
    baseline_prob = to_float(m.get("baseline_prob", BASELINE_PROB), BASELINE_PROB)
    imbalance = live_prob - baseline_prob
    no_prob = 1.0 - live_prob

    m["imbalance"] = imbalance
    m["yes_prob"] = live_prob
    m["no_prob"] = no_prob

    if imbalance <= ALERT_THRESHOLD:
        return "ALERT", f"Strong signal: {imbalance:.3f}"
    if imbalance <= WATCH_THRESHOLD:
        return "WATCH", f"Watch signal: {imbalance:.3f}"
    return "SKIP", None


# =========================================
# SCAN
# =========================================
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
            counts["skip"] += 1
            results.append(
                {
                    "market_id": item.get("slug"),
                    "slug": item.get("slug"),
                    "label": item.get("slug"),
                    "category": "SKIP",
                    "reason": f"Fetch error: {e}",
                    "live_prob": 0.0,
                    "yes_prob": 0.0,
                    "no_prob": 1.0,
                    "baseline_prob": item.get("baseline_prob", BASELINE_PROB),
                    "liquidity": 0.0,
                    "best_bid": 0.0,
                    "best_ask": 0.0,
                    "spread": 0.0,
                    "imbalance": 0.0,
                    "price_source": "none",
                }
            )

    results.sort(key=lambda x: x.get("imbalance", 999))
    return {"counts": counts, "results": results[:20]}


def format_market_block(r: Dict[str, Any]) -> str:
    yes_prob = to_float(r.get("yes_prob", r.get("live_prob", 0.0)), 0.0)
    no_prob = to_float(r.get("no_prob", 1.0 - yes_prob), 1.0 - yes_prob)

    return (
        f"{r['category']} | {r['label']}\n"
        f"slug={r.get('slug', '')}\n"
        f"YES={yes_prob:.1%} NO={no_prob:.1%} "
        f"base={r.get('baseline_prob', 0):.1%} "
        f"diff={r.get('imbalance', 0):.3f}\n"
        f"bid={r.get('best_bid', 0):.3f} "
        f"ask={r.get('best_ask', 0):.3f} "
        f"spread={r.get('spread', 0):.3f} "
        f"liq={r.get('liquidity', 0):.0f} "
        f"src={r.get('price_source', '')}"
    )


def format_scan_summary(scan: Dict[str, Any]) -> str:
    counts = scan["counts"]
    lines = [
        "Scan complete",
        "",
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
        lines.append(format_market_block(r))
        lines.append("")

    return "\n".join(lines).strip()


def format_auto_alert(r: Dict[str, Any]) -> str:
    return format_market_block(r)


# =========================================
# AUTO SCAN LOOP
# =========================================
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

            candidates = candidates[:MAX_ALERTS_PER_SCAN]

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


# =========================================
# ROUTES
# =========================================
@app.route("/", methods=["GET"])
def home():
    return jsonify(
        {
            "ok": True,
            "service": BOT_LABEL,
            "webhook_route": "/webhook",
            "markets_url": MARKETS_URL,
            "alert_threshold": ALERT_THRESHOLD,
            "watch_threshold": WATCH_THRESHOLD,
            "send_watch_alerts": SEND_WATCH_ALERTS,
            "baseline_prob": BASELINE_PROB,
            "min_liquidity": MIN_LIQUIDITY,
            "max_spread": MAX_SPREAD,
            "min_bid": MIN_BID,
            "max_ask": MAX_ASK,
            "scan_every_seconds": SCAN_EVERY_SECONDS,
            "max_alerts_per_scan": MAX_ALERTS_PER_SCAN,
            "auto_scan_enabled": bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0),
            "auto_discover": AUTO_DISCOVER,
            "discover_limit": DISCOVER_LIMIT,
            "discover_min_volume": DISCOVER_MIN_VOLUME,
            "discover_min_liquidity": DISCOVER_MIN_LIQUIDITY,
            "discover_keywords": DISCOVER_KEYWORDS,
            "enable_yes_no_only": ENABLE_YES_NO_ONLY,
            "test_market_slug": TEST_MARKET_SLUG,
        }
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "ok": True,
            "service": BOT_LABEL,
            "auto_scan_enabled": bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0),
            "auto_discover": AUTO_DISCOVER,
            "enable_yes_no_only": ENABLE_YES_NO_ONLY,
            "test_market_slug": TEST_MARKET_SLUG,
            "scan_every_seconds": SCAN_EVERY_SECONDS,
        }
    )


@app.route("/scan", methods=["GET"])
def scan_route():
    try:
        scan = scan_markets()
        return jsonify({"ok": True, **scan})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
            f"alert_threshold={ALERT_THRESHOLD}\n"
            f"watch_threshold={WATCH_THRESHOLD}\n"
            f"send_watch_alerts={SEND_WATCH_ALERTS}\n"
            f"baseline_prob={BASELINE_PROB}\n"
            f"min_liquidity={MIN_LIQUIDITY}\n"
            f"max_spread={MAX_SPREAD}\n"
            f"min_bid={MIN_BID}\n"
            f"max_ask={MAX_ASK}\n"
            f"scan_every_seconds={SCAN_EVERY_SECONDS}\n"
            f"max_alerts_per_scan={MAX_ALERTS_PER_SCAN}\n"
            f"auto_scan_enabled={bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0)}\n"
            f"auto_discover={AUTO_DISCOVER}\n"
            f"discover_limit={DISCOVER_LIMIT}\n"
            f"discover_min_volume={DISCOVER_MIN_VOLUME}\n"
            f"discover_min_liquidity={DISCOVER_MIN_LIQUIDITY}\n"
            f"discover_keywords={DISCOVER_KEYWORDS}\n"
            f"enable_yes_no_only={ENABLE_YES_NO_ONLY}\n"
            f"test_market_slug={TEST_MARKET_SLUG or 'none'}"
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
