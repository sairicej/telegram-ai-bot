import ast
import os
import threading
import time
from datetime import datetime, timedelta, timezone
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

# YES-side thresholds: YES may be underpriced
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.08"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.03"))

# NO-side thresholds: YES may be overpriced, so NO may be better
NO_ALERT_THRESHOLD = float(os.getenv("NO_ALERT_THRESHOLD", "0.08"))
NO_WATCH_THRESHOLD = float(os.getenv("NO_WATCH_THRESHOLD", "0.03"))

SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "true").lower() == "true"
DEBUG_SCAN_OUTPUT = os.getenv("DEBUG_SCAN_OUTPUT", "false").lower() == "true"

BASELINE_PROB = float(os.getenv("BASELINE_PROB", "0.40"))

MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "50000"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.06"))
MIN_BID = float(os.getenv("MIN_BID", "0.05"))
MAX_ASK = float(os.getenv("MAX_ASK", "0.90"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "3600"))

# 24/7 default = every 120 minutes
SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "7200"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))
ZERO_SUMMARY_EVERY_SECONDS = int(os.getenv("ZERO_SUMMARY_EVERY_SECONDS", "21600"))

AUTO_DISCOVER = os.getenv("AUTO_DISCOVER", "true").lower() == "true"
DISCOVER_LIMIT = int(os.getenv("DISCOVER_LIMIT", "50"))
DISCOVER_PAGE_SIZE = int(os.getenv("DISCOVER_PAGE_SIZE", "100"))
DISCOVER_MIN_VOLUME = float(os.getenv("DISCOVER_MIN_VOLUME", "100000"))
DISCOVER_MIN_LIQUIDITY = float(os.getenv("DISCOVER_MIN_LIQUIDITY", "50000"))

DISCOVER_KEYWORDS = os.getenv(
    "DISCOVER_KEYWORDS",
    "fed,fomc,rates,rate hike,rate cut,interest,cpi,inflation,ppi,unemployment,bitcoin,btc,ethereum,eth,solana,sol,price,target,range,hit,reach,above,below,over,under,close,settle,expiry,by date,end of,weekly,monthly,sp500,s&p,nasdaq,index,dow,oil,crude,wti,gold,yield,bond,treasury",
).strip()

ENABLE_YES_NO_ONLY = os.getenv("ENABLE_YES_NO_ONLY", "true").lower() == "true"
TEST_MARKET_SLUG = os.getenv("TEST_MARKET_SLUG", "").strip()

SHORT_TERM_ONLY = os.getenv("SHORT_TERM_ONLY", "true").lower() == "true"
MAX_DAYS_TO_END = int(os.getenv("MAX_DAYS_TO_END", "30"))

# $1-$5 total trade budget
TARGET_MIN_TEST_POSITION_USD = float(os.getenv("TARGET_MIN_TEST_POSITION_USD", "1"))
TARGET_MAX_TEST_POSITION_USD = float(os.getenv("TARGET_MAX_TEST_POSITION_USD", "5"))

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

CATEGORY_WHITELIST = [
    "fed", "fomc", "rates", "rate", "interest", "cpi", "inflation", "ppi", "unemployment",
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "price", "target", "range", "hit", "reach", "above", "below", "over", "under", "close", "settle",
    "sp500", "s&p", "nasdaq", "index", "dow",
    "oil", "crude", "wti", "gold", "yield", "bond", "treasury"
]

NEGATIVE_BLOCKLIST = [
    "war", "military", "offensive", "strike", "israel", "iran", "lebanon",
    "gaza", "missile", "attack", "ground offensive", "troops", "yemen",
    "post", "tweets", "tweet", "celebrity", "actor", "singer"
]

CATEGORY_PRIORITY = {
    "crypto": 0,
    "macro": 1,
    "index_commodity": 2,
    "other": 3,
}

sent_cache: Dict[str, float] = {}
_background_started = False
_background_lock = threading.Lock()

manual_scan_lock = threading.Lock()
manual_scan_in_progress = False

zero_scan_count = 0
zero_window_started_at = time.time()


# =========================================
# BASICS
# =========================================
def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def send_telegram_message(chat_id: str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not chat_id or not text.strip():
        print(f"[{now_str()}] Telegram send skipped | missing token/chat_id/text")
        return

    try:
        r = requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        print(f"[{now_str()}] Telegram message sent.")
    except Exception as e:
        print(f"[{now_str()}] Telegram send error: {e}")


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


def normalize_command(text: str) -> str:
    t = (text or "").strip()
    if not t.startswith("/"):
        return t

    first = t.split()[0].strip().lower()
    if "@" in first:
        first = first.split("@")[0]
    return first


# =========================================
# DATE / SHORT-TERM FILTERS
# =========================================
def parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    return None


def extract_end_datetime(market_data: Dict[str, Any]) -> Optional[datetime]:
    candidates = [
        market_data.get("endDate"),
        market_data.get("end_date"),
        market_data.get("expirationDate"),
        market_data.get("expiration_date"),
        market_data.get("closeDate"),
        market_data.get("close_date"),
        market_data.get("endTime"),
        market_data.get("closeTime"),
    ]

    for c in candidates:
        dt = parse_dt(c)
        if dt:
            return dt

    return None


def is_within_short_term_window(market_data: Dict[str, Any]) -> bool:
    if not SHORT_TERM_ONLY:
        return True

    end_dt = extract_end_datetime(market_data)
    if not end_dt:
        return False

    now = datetime.now(timezone.utc)
    max_dt = now + timedelta(days=MAX_DAYS_TO_END)
    return now <= end_dt <= max_dt


# =========================================
# TOPIC FILTERS
# =========================================
def classify_topic(text: str) -> str:
    t = (text or "").lower()

    if any(k in t for k in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol"]):
        return "crypto"
    if any(k in t for k in ["fed", "fomc", "rate", "rates", "interest", "cpi", "inflation", "ppi", "unemployment", "yield", "bond", "treasury"]):
        return "macro"
    if any(k in t for k in ["sp500", "s&p", "nasdaq", "index", "dow", "oil", "crude", "wti", "gold"]):
        return "index_commodity"

    return "other"


def is_blocked_topic(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in NEGATIVE_BLOCKLIST)


def is_allowed_topic(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in CATEGORY_WHITELIST)


# =========================================
# MARKET HELPERS
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


def is_dead_book(best_bid: float, best_ask: float) -> Tuple[bool, str]:
    if best_bid <= 0:
        return True, "no-bid"
    if best_ask <= 0:
        return True, "no-ask"
    if best_ask >= 0.995:
        return True, "ask-pinned"
    if best_ask - best_bid <= 0:
        return True, "no-spread"
    return False, ""


# =========================================
# WATCHLIST / DISCOVERY
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
            if not parsed:
                continue

            slug_text = parsed["slug"].lower()
            if is_blocked_topic(slug_text):
                continue
            if not is_allowed_topic(slug_text):
                continue

            parsed["topic_category"] = classify_topic(slug_text)
            items.append(parsed)

        print(f"[{now_str()}] Watchlist file loaded | items={len(items)}")
        return items
    except Exception as e:
        print(f"[{now_str()}] Watchlist fetch error: {e}")
        return []


def keyword_list() -> List[str]:
    raw = (DISCOVER_KEYWORDS or "").strip().strip('"').strip("'")
    if raw.lower().startswith("discover_keywords="):
        raw = raw.split("=", 1)[1].strip()
    return [x.strip().lower() for x in raw.split(",") if x.strip()]


def matches_keywords(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in keyword_list())


def discover_markets() -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    seen = set()
    offset = 0

    print(
        f"[{now_str()}] Discover start | "
        f"limit={DISCOVER_LIMIT} page_size={DISCOVER_PAGE_SIZE} "
        f"min_volume={DISCOVER_MIN_VOLUME} min_liquidity={DISCOVER_MIN_LIQUIDITY} "
        f"short_term_only={SHORT_TERM_ONLY} max_days_to_end={MAX_DAYS_TO_END}"
    )

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
            print(f"[{now_str()}] Discover markets error: {e}")
            break

        if not isinstance(batch, list) or not batch:
            break

        for m in batch:
            slug = (m.get("slug") or "").strip()
            question = (m.get("question") or m.get("title") or "").strip()
            combined = f"{slug} {question}".lower()

            if not slug or slug in seen:
                continue
            if ENABLE_YES_NO_ONLY and not is_yes_no_market(m):
                continue
            if SHORT_TERM_ONLY and not is_within_short_term_window(m):
                continue
            if is_blocked_topic(combined):
                continue
            if not is_allowed_topic(combined):
                continue
            if not matches_keywords(combined):
                continue

            seen.add(slug)
            found.append({
                "slug": slug,
                "baseline_prob": BASELINE_PROB,
                "topic_category": classify_topic(combined),
            })

            if len(found) >= DISCOVER_LIMIT:
                break

        offset += DISCOVER_PAGE_SIZE

    print(f"[{now_str()}] Discover complete | found={len(found)}")
    return found


def fetch_watchlist() -> List[Dict[str, Any]]:
    if TEST_MARKET_SLUG:
        return [{
            "slug": TEST_MARKET_SLUG,
            "baseline_prob": BASELINE_PROB,
            "topic_category": "other",
        }]

    if AUTO_DISCOVER:
        discovered = discover_markets()
        if discovered:
            return discovered

    fallback = fetch_watchlist_file()
    filtered = []
    for item in fallback:
        category = item.get("topic_category") or classify_topic(item.get("slug", ""))
        if category in {"crypto", "macro", "index_commodity"}:
            item["topic_category"] = category
            filtered.append(item)
    return filtered


# =========================================
# MARKET DATA
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
        text = f"{m.get('slug', '')} {m.get('question', '')} {m.get('title', '')}".lower()

        if ENABLE_YES_NO_ONLY and not is_yes_no_market(m):
            continue
        if SHORT_TERM_ONLY and not is_within_short_term_window(m):
            continue
        if is_blocked_topic(text):
            continue
        if not is_allowed_topic(text):
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
        text = f"{market.get('slug', '')} {market.get('question', '')} {market.get('title', '')}".lower()
        if ENABLE_YES_NO_ONLY and not is_yes_no_market(market):
            return None, None
        if SHORT_TERM_ONLY and not is_within_short_term_window(market):
            return None, None
        if is_blocked_topic(text):
            return None, None
        if not is_allowed_topic(text):
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


def estimate_test_position_quality(best_bid: float, best_ask: float, liquidity: float) -> Tuple[float, str]:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0, "no-two-sided-book"
    if best_ask >= 0.995:
        return 0.0, "ask-pinned"
    if best_bid <= 0.001:
        return 0.0, "bid-dead"

    spread = max(0.0, best_ask - best_bid)
    spread_component = max(0.0, 0.10 - spread) / 0.10
    liquidity_component = min(liquidity / max(MIN_LIQUIDITY * 4.0, 1.0), 1.0)

    score = (spread_component * 0.65) + (liquidity_component * 0.35)

    if score >= 0.75:
        return round(score, 4), "small-size-friendly"
    if score >= 0.45:
        return round(score, 4), "small-size-possible"
    return round(score, 4), "small-size-weak"


def estimate_shares(total_usd: float, price_per_share: float) -> float:
    if price_per_share <= 0:
        return 0.0
    return round(total_usd / price_per_share, 2)


def fetch_live_market_data(item: Dict[str, Any]) -> Dict[str, Any]:
    slug = item["slug"]
    baseline_prob = to_float(item.get("baseline_prob"), BASELINE_PROB)
    topic_category = item.get("topic_category", "other")

    market_data, source_type = resolve_slug_to_market(slug)
    if not market_data:
        raise ValueError(f"Slug not found or not allowed short-term clean yes/no: {slug}")

    token_id = extract_yes_token_id(market_data)
    if not token_id:
        raise ValueError(f"No YES token ID found for slug: {slug}")

    book = fetch_order_book(token_id)

    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = to_float(bids[0].get("price")) if bids else 0.0
    best_ask = to_float(asks[0].get("price")) if asks else 0.0
    last_trade = to_float(book.get("last_trade_price"), 0.0)

    dead_book, dead_reason = is_dead_book(best_bid, best_ask)
    spread = round(best_ask - best_bid, 6) if best_bid > 0 and best_ask > 0 else 0.0
    live_prob, price_source = choose_live_prob(best_bid, best_ask, last_trade)
    liquidity = to_float(market_data.get("liquidityClob", market_data.get("liquidity", 0.0)), 0.0)

    tiny_position_score, tiny_position_note = estimate_test_position_quality(best_bid, best_ask, liquidity)

    label = market_data.get("question") or market_data.get("title") or market_data.get("slug") or slug
    end_dt = extract_end_datetime(market_data)

    yes_entry_price = best_ask if best_ask > 0 else 0.0
    no_entry_price = round(1.0 - best_bid, 6) if best_bid > 0 else 0.0

    return {
        "market_id": market_data.get("id", slug),
        "slug": slug,
        "source_type": source_type,
        "topic_category": topic_category,
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
        "end_dt": end_dt.isoformat() if end_dt else "",
        "tiny_position_score": tiny_position_score,
        "tiny_position_note": tiny_position_note,
        "dead_book": dead_book,
        "dead_reason": dead_reason,
        "yes_entry_price": yes_entry_price,
        "no_entry_price": no_entry_price,
    }


# =========================================
# CLASSIFICATION / SCORING
# =========================================
def classify_market(m: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    liquidity = to_float(m.get("liquidity", 0), 0.0)
    spread = to_float(m.get("spread", 0), 0.0)
    best_bid = to_float(m.get("best_bid", 0), 0.0)
    best_ask = to_float(m.get("best_ask", 0), 0.0)
    price_source = (m.get("price_source") or "").strip()
    live_prob = to_float(m.get("live_prob", 0.0), 0.0)
    baseline_prob = to_float(m.get("baseline_prob", BASELINE_PROB), BASELINE_PROB)
    tiny_position_score = to_float(m.get("tiny_position_score", 0.0), 0.0)

    if m.get("dead_book"):
        return "SKIP", f"Dead book ({m.get('dead_reason')})"
    if liquidity < MIN_LIQUIDITY:
        return "SKIP", "Liquidity too low"
    if best_bid <= 0 or best_ask <= 0:
        return "SKIP", "Incomplete book"
    if best_bid < MIN_BID:
        return "SKIP", "Bid too low"
    if best_ask >= MAX_ASK:
        return "SKIP", "Ask too high"
    if spread > MAX_SPREAD:
        return "SKIP", "Spread too wide"
    if price_source == "last_trade":
        return "SKIP", "Last trade only"
    if tiny_position_score < 0.45:
        return "SKIP", f"Small-size weak ({m.get('tiny_position_note', 'weak')})"

    # Evaluate both sides
    yes_edge = baseline_prob - live_prob
    no_edge = live_prob - baseline_prob

    yes_strength = 0.0
    no_strength = 0.0

    if yes_edge >= abs(ALERT_THRESHOLD):
        yes_strength = 1.0
    elif yes_edge >= abs(WATCH_THRESHOLD):
        yes_strength = 0.6

    if no_edge >= NO_ALERT_THRESHOLD:
        no_strength = 1.0
    elif no_edge >= NO_WATCH_THRESHOLD:
        no_strength = 0.6

    spread_score = max(0.0, MAX_SPREAD - spread) / max(MAX_SPREAD, 1e-9)
    liquidity_score = min(liquidity / max(MIN_LIQUIDITY * 4.0, 1.0), 1.0)
    category_bonus = {
        "crypto": 1.00,
        "macro": 0.95,
        "index_commodity": 0.90,
        "other": 0.75,
    }.get(m.get("topic_category", "other"), 0.75)

    tradability_core = (
        (spread_score * 0.35)
        + (liquidity_score * 0.20)
        + (tiny_position_score * 0.30)
        + (0.15)  # book quality already filtered, give fixed bonus
    )

    yes_composite = tradability_core * (0.70 + 0.30 * yes_strength) * category_bonus
    no_composite = tradability_core * (0.70 + 0.30 * no_strength) * category_bonus

    if yes_composite >= no_composite:
        side = "YES"
        edge = yes_edge
        side_strength = yes_strength
        entry_price = to_float(m.get("yes_entry_price", 0.0), 0.0)
        m["signal_side"] = side
        m["edge"] = edge
        m["composite_score"] = round(yes_composite, 4)
    else:
        side = "NO"
        edge = no_edge
        side_strength = no_strength
        entry_price = to_float(m.get("no_entry_price", 0.0), 0.0)
        m["signal_side"] = side
        m["edge"] = edge
        m["composite_score"] = round(no_composite, 4)

    m["yes_prob"] = live_prob
    m["no_prob"] = 1.0 - live_prob
    m["imbalance"] = live_prob - baseline_prob
    m["entry_price"] = entry_price
    m["est_shares_min"] = estimate_shares(TARGET_MIN_TEST_POSITION_USD, entry_price)
    m["est_shares_max"] = estimate_shares(TARGET_MAX_TEST_POSITION_USD, entry_price)

    if side_strength >= 1.0 and m["composite_score"] >= 0.62:
        return "ALERT", f"{side} side score={m['composite_score']:.3f} edge={edge:.3f}"

    if side_strength >= 0.6 and m["composite_score"] >= 0.50:
        return "WATCH", f"{side} side score={m['composite_score']:.3f} edge={edge:.3f}"

    return "SKIP", "No qualifying signal"


# =========================================
# SCAN
# =========================================
def scan_markets() -> Dict[str, Any]:
    watchlist = fetch_watchlist()
    results: List[Dict[str, Any]] = []
    counts = {"total": 0, "alert": 0, "watch": 0, "skip": 0}

    print(f"[{now_str()}] Scan start | watchlist_items={len(watchlist)}")

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
            print(f"[{now_str()}] Market scan error for {item}: {e}")
            counts["skip"] += 1

    results.sort(
        key=lambda x: (
            0 if x.get("category") == "ALERT" else 1 if x.get("category") == "WATCH" else 2,
            CATEGORY_PRIORITY.get(x.get("topic_category", "other"), 9),
            -to_float(x.get("composite_score"), 0.0),
            -to_float(x.get("edge"), 0.0),
        )
    )

    print(
        f"[{now_str()}] Scan complete | "
        f"total={counts['total']} alert={counts['alert']} "
        f"watch={counts['watch']} skip={counts['skip']}"
    )
    return {"counts": counts, "results": results[:20]}


def qualifying_results(scan: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        r for r in scan["results"]
        if r["category"] == "ALERT" or (r["category"] == "WATCH" and SEND_WATCH_ALERTS)
    ]


def format_market_block(r: Dict[str, Any]) -> str:
    end_text = r.get("end_dt", "")
    end_line = f"\nends={end_text}" if end_text else ""

    return (
        f"{r['category']} | {r['label']}\n"
        f"slug={r.get('slug', '')}{end_line}\n"
        f"topic={r.get('topic_category', 'other')} "
        f"side={r.get('signal_side', '?')} "
        f"score={r.get('composite_score', 0):.3f} "
        f"edge={r.get('edge', 0):.3f}\n"
        f"entry_price={r.get('entry_price', 0):.3f} "
        f"test_size=${TARGET_MIN_TEST_POSITION_USD:.0f}-${TARGET_MAX_TEST_POSITION_USD:.0f} "
        f"est_shares={r.get('est_shares_min', 0)}-{r.get('est_shares_max', 0)}\n"
        f"tiny_size={r.get('tiny_position_note', '')} "
        f"tiny_score={r.get('tiny_position_score', 0):.3f}\n"
        f"bid={r.get('best_bid', 0):.3f} "
        f"ask={r.get('best_ask', 0):.3f} "
        f"spread={r.get('spread', 0):.3f} "
        f"liq={r.get('liquidity', 0):.0f} "
        f"src={r.get('price_source', '')}"
    )


def format_zero_summary(zero_count: int, seconds_in_window: int) -> str:
    minutes = max(1, round(seconds_in_window / 60))
    return f"No qualifying markets in last {minutes} minutes.\nEmpty scans: {zero_count}"


def format_manual_scan(scan: Dict[str, Any]) -> Optional[str]:
    top = qualifying_results(scan)
    counts = scan.get("counts", {})
    results = scan.get("results", [])

    if top:
        lines = [
            "Scan complete",
            "",
            f"Total: {counts.get('total', 0)}",
            f"Alerts: {counts.get('alert', 0)}",
            f"Watch: {counts.get('watch', 0)}",
            f"Skip: {counts.get('skip', 0)}",
            "",
            f"Qualifying markets: {len(top)}",
            "",
        ]

        for r in top[:5]:
            lines.append(format_market_block(r))
            lines.append("")

        return "\n".join(lines).strip()

    if DEBUG_SCAN_OUTPUT:
        lines = [
            "Scan finished. No qualifying markets.",
            "",
            f"Total: {counts.get('total', 0)}",
            f"Alerts: {counts.get('alert', 0)}",
            f"Watch: {counts.get('watch', 0)}",
            f"Skip: {counts.get('skip', 0)}",
        ]

        fallback = [r for r in results if not r.get("dead_book")][:3]
        if not fallback:
            fallback = results[:3]

        if fallback:
            lines.extend(["", "Top candidates:", ""])
            for r in fallback:
                lines.append(
                    f"{r.get('category', 'SKIP')} | {r.get('label', '')}\n"
                    f"slug={r.get('slug', '')}\n"
                    f"reason={r.get('reason') or r.get('dead_reason') or 'No qualifying signal'}\n"
                    f"topic={r.get('topic_category', 'other')} side={r.get('signal_side', '?')}\n"
                    f"ends={r.get('end_dt', '') or 'unknown'}\n"
                    f"entry_price={r.get('entry_price', 0):.3f} "
                    f"est_shares={r.get('est_shares_min', 0)}-{r.get('est_shares_max', 0)}\n"
                    f"tiny_size={r.get('tiny_position_note', '')} tiny_score={r.get('tiny_position_score', 0):.3f}\n"
                    f"score={r.get('composite_score', 0):.3f} "
                    f"bid={r.get('best_bid', 0):.3f} "
                    f"ask={r.get('best_ask', 0):.3f} "
                    f"spread={r.get('spread', 0):.3f} "
                    f"liq={r.get('liquidity', 0):.0f}"
                )
                lines.append("")

        return "\n".join(lines).strip()

    return None


def run_manual_scan_async(chat_id: str) -> None:
    global manual_scan_in_progress

    try:
        scan = scan_markets()
        msg = format_manual_scan(scan)
        if msg:
            send_telegram_message(chat_id, msg)
    except Exception as e:
        print(f"[{now_str()}] Manual scan error: {e}")
        send_telegram_message(chat_id, f"Scan error: {e}")
    finally:
        with manual_scan_lock:
            manual_scan_in_progress = False


# =========================================
# AUTO LOOP
# =========================================
def auto_scan_loop() -> None:
    global zero_scan_count, zero_window_started_at

    if not TELEGRAM_CHAT_ID:
        print(f"[{now_str()}] Auto scan disabled: TELEGRAM_CHAT_ID not set")
        return

    if SCAN_EVERY_SECONDS <= 0:
        print(f"[{now_str()}] Auto scan disabled: SCAN_EVERY_SECONDS <= 0")
        return

    print(f"[{now_str()}] Auto scan loop started. Every {SCAN_EVERY_SECONDS} seconds.")

    while True:
        try:
            scan = scan_markets()
            candidates = qualifying_results(scan)[:MAX_ALERTS_PER_SCAN]
            now = time.time()

            if candidates:
                zero_scan_count = 0
                zero_window_started_at = now

                for r in candidates:
                    dedupe_key = f"{r.get('slug')}|{r.get('category')}|{r.get('signal_side')}"
                    if already_sent(dedupe_key):
                        continue

                    send_telegram_message(TELEGRAM_CHAT_ID, format_market_block(r))
                    mark_sent(dedupe_key)
            else:
                zero_scan_count += 1
                elapsed = now - zero_window_started_at

                if elapsed >= ZERO_SUMMARY_EVERY_SECONDS and zero_scan_count > 0:
                    send_telegram_message(
                        TELEGRAM_CHAT_ID,
                        format_zero_summary(zero_scan_count, int(elapsed)),
                    )
                    zero_scan_count = 0
                    zero_window_started_at = now

        except Exception as e:
            print(f"[{now_str()}] Auto scan error: {e}")

        time.sleep(SCAN_EVERY_SECONDS)


def start_background_worker_once() -> None:
    global _background_started
    with _background_lock:
        if _background_started:
            return
        _background_started = True
        t = threading.Thread(target=auto_scan_loop, daemon=True)
        t.start()
        print(f"[{now_str()}] Background worker started.")


start_background_worker_once()


# =========================================
# ROUTES
# =========================================
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "service": BOT_LABEL,
        "auto_scan_enabled": bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0),
        "scan_every_seconds": SCAN_EVERY_SECONDS,
        "zero_summary_every_seconds": ZERO_SUMMARY_EVERY_SECONDS,
        "short_term_only": SHORT_TERM_ONLY,
        "max_days_to_end": MAX_DAYS_TO_END,
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "service": BOT_LABEL,
        "auto_discover": AUTO_DISCOVER,
        "auto_scan_enabled": bool(TELEGRAM_CHAT_ID and SCAN_EVERY_SECONDS > 0),
        "enable_yes_no_only": ENABLE_YES_NO_ONLY,
        "scan_every_seconds": SCAN_EVERY_SECONDS,
        "zero_summary_every_seconds": ZERO_SUMMARY_EVERY_SECONDS,
        "short_term_only": SHORT_TERM_ONLY,
        "max_days_to_end": MAX_DAYS_TO_END,
        "discover_limit": DISCOVER_LIMIT,
        "discover_min_volume": DISCOVER_MIN_VOLUME,
        "discover_min_liquidity": DISCOVER_MIN_LIQUIDITY,
        "min_liquidity": MIN_LIQUIDITY,
        "max_spread": MAX_SPREAD,
        "alert_threshold": ALERT_THRESHOLD,
        "watch_threshold": WATCH_THRESHOLD,
        "no_alert_threshold": NO_ALERT_THRESHOLD,
        "no_watch_threshold": NO_WATCH_THRESHOLD,
        "send_watch_alerts": SEND_WATCH_ALERTS,
        "debug_scan_output": DEBUG_SCAN_OUTPUT,
        "manual_scan_in_progress": manual_scan_in_progress,
        "target_min_test_position_usd": TARGET_MIN_TEST_POSITION_USD,
        "target_max_test_position_usd": TARGET_MAX_TEST_POSITION_USD,
    })


@app.route("/scan", methods=["GET"])
def scan_route():
    try:
        scan = scan_markets()
        return jsonify({"ok": True, **scan})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    global manual_scan_in_progress

    data = request.get_json(silent=True) or {}
    print(f"[{now_str()}] WEBHOOK HIT: {data}")

    message = data.get("message", {})
    chat = message.get("chat", {})
    chat_id = str(chat.get("id", "")).strip()
    text = (message.get("text") or "").strip()
    command = normalize_command(text)

    print(f"[{now_str()}] Command received | raw_text={text} normalized={command} chat_id={chat_id}")

    if not chat_id or not text:
        return jsonify({"ok": True, "ignored": True})

    if command == "/start":
        send_telegram_message(chat_id, "Bot is live.\nUse /scan or /health")
        return jsonify({"ok": True})

    if command == "/health":
        msg = (
            "Health check\n"
            f"alert_threshold={ALERT_THRESHOLD}\n"
            f"watch_threshold={WATCH_THRESHOLD}\n"
            f"no_alert_threshold={NO_ALERT_THRESHOLD}\n"
            f"no_watch_threshold={NO_WATCH_THRESHOLD}\n"
            f"send_watch_alerts={SEND_WATCH_ALERTS}\n"
            f"debug_scan_output={DEBUG_SCAN_OUTPUT}\n"
            f"scan_every_seconds={SCAN_EVERY_SECONDS}\n"
            f"short_term_only={SHORT_TERM_ONLY}\n"
            f"max_days_to_end={MAX_DAYS_TO_END}\n"
            f"discover_limit={DISCOVER_LIMIT}\n"
            f"discover_min_volume={DISCOVER_MIN_VOLUME}\n"
            f"discover_min_liquidity={DISCOVER_MIN_LIQUIDITY}\n"
            f"min_liquidity={MIN_LIQUIDITY}\n"
            f"max_spread={MAX_SPREAD}\n"
            f"auto_discover={AUTO_DISCOVER}\n"
            f"enable_yes_no_only={ENABLE_YES_NO_ONLY}\n"
            f"manual_scan_in_progress={manual_scan_in_progress}\n"
            f"target_min_test_position_usd={TARGET_MIN_TEST_POSITION_USD}\n"
            f"target_max_test_position_usd={TARGET_MAX_TEST_POSITION_USD}\n"
            f"test_market_slug={TEST_MARKET_SLUG or 'none'}"
        )
        send_telegram_message(chat_id, msg)
        return jsonify({"ok": True})

    if command == "/scan":
        with manual_scan_lock:
            if manual_scan_in_progress:
                send_telegram_message(chat_id, "Scan already running. Wait for result.")
                return jsonify({"ok": True})
            manual_scan_in_progress = True

        send_telegram_message(chat_id, "Running scan...")
        threading.Thread(target=run_manual_scan_async, args=(chat_id,), daemon=True).start()
        return jsonify({"ok": True})

    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
