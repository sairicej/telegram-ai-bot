import ast
import os
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

SCRIPT_VERSION = "v8-rolling-30d-event-universe"

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

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.10"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.05"))

SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
DEBUG_SCAN_OUTPUT = os.getenv("DEBUG_SCAN_OUTPUT", "true").lower() == "true"

BASELINE_PROB = float(os.getenv("BASELINE_PROB", "0.40"))

MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "10000"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.05"))
MIN_BID = float(os.getenv("MIN_BID", "0.03"))
MAX_ASK = float(os.getenv("MAX_ASK", "0.90"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "3600"))

SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "7200"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))
ZERO_SUMMARY_EVERY_SECONDS = int(os.getenv("ZERO_SUMMARY_EVERY_SECONDS", "21600"))

AUTO_DISCOVER = os.getenv("AUTO_DISCOVER", "true").lower() == "true"
DISCOVER_LIMIT = int(os.getenv("DISCOVER_LIMIT", "250"))
DISCOVER_PAGE_SIZE = int(os.getenv("DISCOVER_PAGE_SIZE", "100"))
DISCOVER_MIN_VOLUME = float(os.getenv("DISCOVER_MIN_VOLUME", "25000"))
DISCOVER_MIN_LIQUIDITY = float(os.getenv("DISCOVER_MIN_LIQUIDITY", "10000"))

DISCOVER_KEYWORDS = os.getenv(
    "DISCOVER_KEYWORDS",
    "cpi,ppi,fomc,fed,rates,rate cut,rate hike,unemployment,inflation,approval,decision,announce,ruling,hearing,court,judge,legal,policy,vote,debate,primary,election,tariff,shutdown,meeting,today,tonight,tomorrow,this week,ftx,bankruptcy,payout,relaunch,sentencing,sec,etf",
).strip()

ENABLE_YES_NO_ONLY = os.getenv("ENABLE_YES_NO_ONLY", "true").lower() == "true"
TEST_MARKET_SLUG = os.getenv("TEST_MARKET_SLUG", "").strip()

SHORT_TERM_ONLY = os.getenv("SHORT_TERM_ONLY", "true").lower() == "true"
MAX_DAYS_TO_END = int(os.getenv("MAX_DAYS_TO_END", "14"))

TARGET_MIN_TEST_POSITION_USD = float(os.getenv("TARGET_MIN_TEST_POSITION_USD", "1"))
TARGET_MAX_TEST_POSITION_USD = float(os.getenv("TARGET_MAX_TEST_POSITION_USD", "5"))

EARLY_MIN_BID = float(os.getenv("EARLY_MIN_BID", "0.03"))
EARLY_MAX_ASK = float(os.getenv("EARLY_MAX_ASK", "0.97"))
EARLY_MAX_SPREAD = float(os.getenv("EARLY_MAX_SPREAD", "0.20"))
MID_PRICE_MIN = float(os.getenv("MID_PRICE_MIN", "0.05"))
MID_PRICE_MAX = float(os.getenv("MID_PRICE_MAX", "0.95"))

BLOCK_CRYPTO_LADDERS = os.getenv("BLOCK_CRYPTO_LADDERS", "true").lower() == "true"

BOOK_LEVELS = int(os.getenv("BOOK_LEVELS", "3"))
MIN_TOP_DEPTH = float(os.getenv("MIN_TOP_DEPTH", "10"))
MIN_FILL_CONFIDENCE = float(os.getenv("MIN_FILL_CONFIDENCE", "0.45"))
MICRO_ALERT_SCORE = float(os.getenv("MICRO_ALERT_SCORE", "0.62"))
MICRO_WATCH_SCORE = float(os.getenv("MICRO_WATCH_SCORE", "0.52"))

PREFLIGHT_MAX_SPREAD = float(os.getenv("PREFLIGHT_MAX_SPREAD", "0.12"))
PREFLIGHT_MIN_BID = float(os.getenv("PREFLIGHT_MIN_BID", "0.03"))
PREFLIGHT_MAX_ASK = float(os.getenv("PREFLIGHT_MAX_ASK", "0.97"))
PREFLIGHT_MAX_CANDIDATES = int(os.getenv("PREFLIGHT_MAX_CANDIDATES", "60"))

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

NEGATIVE_BLOCKLIST = [
    "war", "military", "offensive", "strike", "israel", "iran", "lebanon",
    "gaza", "missile", "attack", "ground offensive", "troops", "yemen",
    "post", "tweets", "tweet", "celebrity", "actor", "singer",
    "up or down", "price to beat", "fdv above", "fdv below", "one day after launch",
]

CATEGORY_WHITELIST = [
    "cpi", "ppi", "fomc", "fed", "rates", "rate", "inflation", "unemployment",
    "approval", "decision", "announce", "ruling", "hearing", "court", "judge", "legal",
    "policy", "election", "primary", "vote", "debate", "president", "senate", "house", "governor",
    "tariff", "shutdown", "meeting", "today", "tonight", "tomorrow", "this week",
    "by monday", "by tuesday", "by wednesday", "by thursday", "by friday", "by saturday", "by sunday",
    "ftx", "bankruptcy", "payout", "relaunch", "sentencing", "sec", "etf",
]

CATEGORY_PRIORITY = {
    "politics_event": 0,
    "legal_special": 1,
    "macro": 2,
    "crypto_event": 3,
    "other": 4,
}

sent_cache: Dict[str, float] = {}
_background_started = False
_background_lock = threading.Lock()

manual_scan_lock = threading.Lock()
manual_scan_in_progress = False

zero_scan_count = 0
zero_window_started_at = time.time()

last_skip_counts: Counter = Counter()
last_pipeline_stats: Dict[str, Any] = {}
last_preflight_reason_counts: Dict[str, Any] = {}
last_near_passes: Dict[str, Any] = {"discover": [], "fallback": []}
last_discovery_samples: Dict[str, Any] = {"accepted": [], "hard_skipped": []}

CANDIDATE_CACHE_TTL_SECONDS = 90
ROLLING_DISCOVERY_DAYS = 30

MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

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


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


# =========================================
# DATE / SHORT TERM
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
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
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


def days_to_end(end_iso: str) -> Optional[float]:
    dt = parse_dt(end_iso)
    if not dt:
        return None
    delta = dt - datetime.now(timezone.utc)
    return delta.total_seconds() / 86400.0


def month_name(dt: datetime) -> str:
    return MONTH_NAMES[dt.month - 1]


def rolling_window_end() -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=ROLLING_DISCOVERY_DAYS)


def rolling_date_terms(days: int = ROLLING_DISCOVERY_DAYS) -> List[str]:
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=days)
    terms = {
        "today", "tonight", "tomorrow", "this week",
        "by friday", "by saturday", "by sunday", "by monday", "by tuesday", "by wednesday", "by thursday",
        f"end of {month_name(now)}",
        f"end of {month_name(end)}",
    }

    cursor = now
    while cursor <= end:
        terms.add(f"by {month_name(cursor)} {cursor.day}")
        cursor += timedelta(days=1)

    return sorted(terms)


def looks_like_ranking_market(text: str) -> bool:
    t = (text or "").lower()
    ranking_phrases = [
        "best model", "second-best model", "second best model", "top model",
        "best ai model", "top ai model", "leaderboard", "ranked",
        "have the second-best", "have the second best", "best-performing model",
        "best performing model", "most used model", "highest ranked model",
    ]
    return any(p in t for p in ranking_phrases)


def looks_like_low_urgency_theme_market(text: str) -> bool:
    t = (text or "").lower()
    theme_phrases = [
        "at the end of", "by end of"
    ]
    if looks_like_ranking_market(t):
        return True
    return any(p in t for p in theme_phrases) and any(k in t for k in ["best model", "second-best", "leaderboard", "top model", "top ai"])


def is_rolling_window_phrase(text: str) -> bool:
    t = (text or "").lower()
    return any(term in t for term in rolling_date_terms())


# =========================================
# TOPICS
# =========================================
def classify_topic(text: str) -> str:
    t = (text or "").lower()

    if any(k in t for k in ["ftx", "bankruptcy", "payout", "relaunch", "sentencing", "court", "judge", "hearing", "ruling", "sec"]):
        return "legal_special"
    if any(k in t for k in ["election", "primary", "vote", "debate", "president", "senate", "house", "governor"]):
        return "politics_event"
    if any(k in t for k in ["fed", "fomc", "rate", "rates", "interest", "cpi", "inflation", "ppi", "unemployment", "tariff", "shutdown", "policy", "meeting"]):
        return "macro"
    if any(k in t for k in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "etf", "approval"]):
        return "crypto_event"
    return "other"


def is_blocked_topic(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in NEGATIVE_BLOCKLIST)


def is_secondary_special_situation(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in ["ftx", "bankruptcy", "payout", "relaunch", "sentencing", "court", "judge", "sec"])


def is_allowed_topic(text: str) -> bool:
    t = (text or "").lower()
    if looks_like_ranking_market(t) or looks_like_low_urgency_theme_market(t):
        return False
    if not any(k in t for k in CATEGORY_WHITELIST):
        return False
    if is_event_bound_market(t):
        return True
    return is_secondary_special_situation(t)


def looks_like_crypto_ladder_market(text: str) -> bool:
    if not BLOCK_CRYPTO_LADDERS:
        return False

    t = (text or "").lower()
    if not any(k in t for k in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol"]):
        return False

    ladder_phrases = [
        "price of bitcoin be above",
        "price of bitcoin be below",
        "price of ethereum be above",
        "price of ethereum be below",
        "price of solana be above",
        "price of solana be below",
        "will bitcoin reach",
        "will ethereum reach",
        "will solana reach",
        "will bitcoin hit",
        "will ethereum hit",
        "will solana hit",
        "between $",
        "above $",
        "below $",
        "dip to",
    ]
    return any(p in t for p in ladder_phrases)


def looks_like_strike_ladder_market(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False

    obvious_phrases = [
        "hit (high)",
        "hit (low)",
        "(high)",
        "(low)",
        "all time high",
        "all-time-high",
        "ath by",
        "price target",
        "target by",
    ]
    if any(p in t for p in obvious_phrases):
        return True

    trigger_words = ["hit", "reach", "above", "below", "between", "touch"]
    assets = ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crude oil", "oil", "wti", "gold", "sp500", "s&p", "nasdaq", "dow"]
    date_words = ["by end of", "by friday", "by saturday", "by sunday", "by monday", "by tuesday", "by wednesday", "by thursday"] + rolling_date_terms()
    if any(w in t for w in trigger_words) and any(a in t for a in assets) and any(d in t for d in date_words):
        if "$" in t or " all time high" in t or " ath " in f" {t} ":
            return True

    return False


def canonical_market_family_key(slug: str, question: str) -> str:
    combined = f"{slug or ''} {question or ''}".lower().strip()
    if not combined:
        return ""

    normalized = combined
    replacements = {
        "(high)": "",
        "(low)": "",
        "all time high": "ath",
        "all-time-high": "ath",
        " by friday": " by_date",
        " by saturday": " by_date",
        " by sunday": " by_date",
        " by monday": " by_date",
        " by tuesday": " by_date",
        " by wednesday": " by_date",
        " by thursday": " by_date",
    }
    for a, b in replacements.items():
        normalized = normalized.replace(a, b)
    for term in rolling_date_terms():
        normalized = normalized.replace(f" by {term}", " by_date")
        normalized = normalized.replace(f" end of {term}", " by_date")
        if term.startswith("by "):
            normalized = normalized.replace(term, "by_date")

    for token in ["$100", "$105", "$110", "$115", "$120", "$125", "$130", "$150", "$200", "$40", "$50", "$60", "$70", "$80", "$85", "$90", "$95", "$100k", "$120k"]:
        normalized = normalized.replace(token.lower(), "$x")

    words = normalized.replace("?", " ").replace(",", " ").split()
    if not words:
        return ""

    if looks_like_crypto_ladder_market(normalized) or looks_like_strike_ladder_market(normalized):
        keep = []
        for w in words:
            if w in {"will", "the", "a", "an", "to", "of", "in", "on", "and", "or", "by_date", "by", "end"}:
                continue
            keep.append(w)
        return "family:" + " ".join(keep[:8])

    return "market:" + " ".join(words[:10])


def extract_asset_family_bucket(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["ftx", "bankruptcy", "payout", "relaunch", "sentencing"]):
        return "asset:ftx"
    if any(k in t for k in ["election", "primary", "vote", "debate", "president", "senate", "house", "governor"]):
        return "asset:politics"
    if any(k in t for k in ["fed", "fomc", "cpi", "ppi", "unemployment", "rates", "tariff", "shutdown"]):
        return "asset:macro"
    if any(k in t for k in ["crude oil", " oil ", " wti", "wti "]):
        return "asset:oil"
    if any(k in t for k in ["bitcoin", " btc"]):
        return "asset:btc"
    if any(k in t for k in ["ethereum", " eth"]):
        return "asset:eth"
    if any(k in t for k in ["solana", " sol"]):
        return "asset:sol"
    if "gold" in t:
        return "asset:gold"
    if any(k in t for k in ["sp500", "s&p", "nasdaq", "dow"]):
        return "asset:index"
    if any(k in t for k in ["fed", "fomc", "cpi", "ppi", "unemployment", "tariff", "shutdown", "vote", "debate", "primary", "approval", "ruling", "hearing", "decision"]):
        return "asset:event"
    return ""


def looks_like_shell_price_market(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    shell_phrases = [
        "all time high", "all-time-high", "ath by", "price target", "target by",
        "by end of", "end of",
    ]
    if any(p in t for p in shell_phrases) and any(a in t for a in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crude oil", "oil", "wti", "gold", "sp500", "s&p", "nasdaq", "dow"]):
        if any(w in t for w in ["hit", "reach", "above", "below", "between", "touch", "high", "low"]):
            return True
    return False


def looks_like_short_window_price_market(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    short_window_phrases = [
        "up or down", "price to beat", "fdv above", "fdv below",
        "one day after launch", "5 minutes", "15 minutes", "30 minutes", "4 hour", "4 hours",
    ]
    return any(p in t for p in short_window_phrases)


def should_hard_skip_discovery_market(text: str) -> Tuple[bool, str]:
    t = (text or "").lower()
    if not t:
        return False, ""
    if looks_like_short_window_price_market(t):
        return True, "hard_skip_short_window_price_market"
    if looks_like_ranking_market(t):
        return True, "hard_skip_ranking_market"
    if looks_like_crypto_ladder_market(t):
        return True, "hard_skip_crypto_ladder"
    if looks_like_strike_ladder_market(t):
        return True, "hard_skip_strike_ladder"
    if looks_like_shell_price_market(t):
        return True, "hard_skip_shell_price_market"
    return False, ""


def is_event_bound_market(text: str) -> bool:
    t = (text or "").lower()
    core_terms = [
        "decision", "announce", "approval", "ruling", "hearing", "vote", "debate",
        "primary", "meeting", "today", "tonight", "tomorrow", "this week",
        "cpi", "ppi", "fomc", "fed", "rates", "tariff",
        "shutdown", "legal", "etf", "policy", "court", "judge", "sentencing",
        "bankruptcy", "payout", "relaunch", "sec", "election"
    ]
    return any(k in t for k in core_terms) or is_rolling_window_phrase(t)


def is_dead_extreme_book(best_bid: float, best_ask: float) -> bool:
    if best_bid <= 0 or best_ask <= 0:
        return False
    return best_bid <= 0.002 and best_ask >= 0.998 and (best_ask - best_bid) >= 0.90


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
        result = []
        for t in tokens:
            outcome = t.get("outcome")
            if outcome is not None:
                result.append(str(outcome).strip())
        if result:
            return result

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

    spread = best_ask - best_bid
    if spread <= 0:
        return True, "no-spread"
    if best_bid < EARLY_MIN_BID:
        return True, "bid-too-low-early"
    if best_ask > EARLY_MAX_ASK:
        return True, "ask-too-high-early"
    if spread > EARLY_MAX_SPREAD:
        return True, "spread-too-wide-early"

    midpoint = (best_bid + best_ask) / 2.0
    if midpoint < MID_PRICE_MIN or midpoint > MID_PRICE_MAX:
        return True, "one-sided-midpoint"

    return False, ""


def keyword_list() -> List[str]:
    raw = (DISCOVER_KEYWORDS or "").strip().strip('"').strip("'")
    base = [x.strip().lower() for x in raw.split(",") if x.strip()]
    return sorted(set(base + rolling_date_terms()))


def matches_keywords(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    if looks_like_ranking_market(t):
        return False
    return any(k in t for k in keyword_list())


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


def level_size(level: Dict[str, Any]) -> float:
    for key in ("size", "amount", "quantity"):
        if key in level:
            return to_float(level.get(key), 0.0)
    return 0.0


def top_depth(levels: List[Dict[str, Any]], count: int) -> float:
    return round(sum(level_size(x) for x in levels[:count]), 6)


def best_level_size(levels: List[Dict[str, Any]]) -> float:
    if not levels:
        return 0.0
    return round(level_size(levels[0]), 6)


# =========================================
# DISCOVERY / RESOLUTION
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
        if looks_like_crypto_ladder_market(text) or looks_like_strike_ladder_market(text):
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
        if looks_like_crypto_ladder_market(text) or looks_like_strike_ladder_market(text):
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


def preflight_book_check(market_data: Dict[str, Any], token_id: str) -> Optional[Dict[str, Any]]:
    label = market_data.get("question") or market_data.get("title") or market_data.get("slug") or ""

    try:
        book = fetch_order_book(token_id)
    except Exception:
        return {"ok": False, "reason": "book_fetch_failed", "label": label, "distance_to_pass": 999.0}

    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = to_float(bids[0].get("price")) if bids else 0.0
    best_ask = to_float(asks[0].get("price")) if asks else 0.0
    bid_depth = top_depth(bids, BOOK_LEVELS)
    ask_depth = top_depth(asks, BOOK_LEVELS)
    spread = max(0.0, best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0.0
    midpoint = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0

    side_hint = "YES" if ask_depth >= bid_depth else "NO"

    base = {
        "label": label,
        "best_bid": round(best_bid, 6),
        "best_ask": round(best_ask, 6),
        "spread": round(spread, 6),
        "bid_depth": round(bid_depth, 6),
        "ask_depth": round(ask_depth, 6),
        "midpoint": round(midpoint, 6),
        "side_hint": side_hint,
    }

    if is_dead_extreme_book(best_bid, best_ask):
        return {
            **base,
            "ok": False,
            "reason": "preflight_dead_extreme_book",
            "distance_to_pass": 999.0,
            "distance_note": "book pinned at extremes",
        }

    if best_bid <= 0:
        return {**base, "ok": False, "reason": "preflight_no_bid", "distance_to_pass": 999.0}
    if best_ask <= 0:
        return {**base, "ok": False, "reason": "preflight_no_ask", "distance_to_pass": 999.0}
    if spread <= 0:
        return {**base, "ok": False, "reason": "preflight_no_spread", "distance_to_pass": 999.0}
    if best_bid < PREFLIGHT_MIN_BID:
        return {
            **base,
            "ok": False,
            "reason": "preflight_bid_too_low",
            "distance_to_pass": round(PREFLIGHT_MIN_BID - best_bid, 6),
            "distance_note": f"bid short by {round(PREFLIGHT_MIN_BID - best_bid, 6)}",
        }
    if best_ask > PREFLIGHT_MAX_ASK:
        return {
            **base,
            "ok": False,
            "reason": "preflight_ask_too_high",
            "distance_to_pass": round(best_ask - PREFLIGHT_MAX_ASK, 6),
            "distance_note": f"ask above cap by {round(best_ask - PREFLIGHT_MAX_ASK, 6)}",
        }
    if spread > PREFLIGHT_MAX_SPREAD:
        return {
            **base,
            "ok": False,
            "reason": "preflight_spread_too_wide",
            "distance_to_pass": round(spread - PREFLIGHT_MAX_SPREAD, 6),
            "distance_note": f"spread wide by {round(spread - PREFLIGHT_MAX_SPREAD, 6)}",
        }
    if bid_depth < MIN_TOP_DEPTH:
        return {
            **base,
            "ok": False,
            "reason": "preflight_bid_depth_thin",
            "distance_to_pass": round(MIN_TOP_DEPTH - bid_depth, 6),
            "distance_note": f"bid depth short by {round(MIN_TOP_DEPTH - bid_depth, 6)}",
        }
    if ask_depth < MIN_TOP_DEPTH:
        return {
            **base,
            "ok": False,
            "reason": "preflight_ask_depth_thin",
            "distance_to_pass": round(MIN_TOP_DEPTH - ask_depth, 6),
            "distance_note": f"ask depth short by {round(MIN_TOP_DEPTH - ask_depth, 6)}",
        }
    if midpoint < MID_PRICE_MIN or midpoint > MID_PRICE_MAX:
        midpoint_distance = (MID_PRICE_MIN - midpoint) if midpoint < MID_PRICE_MIN else (midpoint - MID_PRICE_MAX)
        return {
            **base,
            "ok": False,
            "reason": "preflight_midpoint_outside",
            "distance_to_pass": round(midpoint_distance, 6),
            "distance_note": f"midpoint outside by {round(midpoint_distance, 6)}",
        }

    resolved_text = f"{market_data.get('slug', '')} {market_data.get('question', '')} {market_data.get('title', '')}".lower()

    return {
        **base,
        "ok": True,
        "reason": "ok",
        "book": book,
        "topic_category": classify_topic(resolved_text),
        "label": label,
        "resolved_text": resolved_text,
        "distance_to_pass": 0.0,
        "side_hint": side_hint,
    }


def record_preflight_near_pass(stats: Dict[str, Any], candidate: Dict[str, Any], bucket_key: str) -> None:
    if not candidate or candidate.get("ok"):
        return

    reason = candidate.get("reason", "unknown")
    if reason in {"resolve_failed", "token_missing", "book_fetch_failed"}:
        return

    micro = candidate.get("micro") or {}
    label = micro.get("label") or candidate.get("slug") or ""
    entry = {
        "label": label,
        "slug": candidate.get("slug", ""),
        "reason": reason,
        "distance_to_pass": to_float(micro.get("distance_to_pass"), 999.0),
        "distance_note": micro.get("distance_note", ""),
        "best_bid": to_float(micro.get("best_bid"), 0.0),
        "best_ask": to_float(micro.get("best_ask"), 0.0),
        "spread": to_float(micro.get("spread"), 0.0),
        "bid_depth": to_float(micro.get("bid_depth"), 0.0),
        "ask_depth": to_float(micro.get("ask_depth"), 0.0),
        "midpoint": to_float(micro.get("midpoint"), 0.0),
        "side_hint": micro.get("side_hint", ""),
    }

    combined_text = f"{entry['slug']} {entry['label']}".lower()

    if reason == "preflight_dead_extreme_book":
        return
    if looks_like_crypto_ladder_market(combined_text) or looks_like_strike_ladder_market(combined_text):
        return
    if entry["best_bid"] < 0.003:
        return
    if entry["spread"] > 0.25:
        return
    if is_dead_extreme_book(entry["best_bid"], entry["best_ask"]):
        return

    key = f"{bucket_key}_near_passes"
    arr = stats.setdefault(key, [])
    arr.append(entry)
    arr.sort(key=lambda x: (to_float(x.get("distance_to_pass"), 999.0), to_float(x.get("spread"), 999.0), -max(to_float(x.get("bid_depth"), 0.0), to_float(x.get("ask_depth"), 0.0))))
    if len(arr) > 5:
        del arr[5:]


def discovery_priority(combined: str) -> float:
    topic = classify_topic(combined)
    base = {
        "politics_event": 1.00,
        "legal_special": 0.97,
        "macro": 0.95,
        "crypto_event": 0.86,
        "other": 0.65,
    }.get(topic, 0.65)

    if is_event_bound_market(combined):
        base += 0.08
    if is_secondary_special_situation(combined):
        base += 0.04
    if any(k in combined for k in ["approval", "decision", "ruling", "hearing", "vote", "debate", "cpi", "ppi", "fomc", "fed", "sentencing", "payout"]):
        base += 0.05
    if is_rolling_window_phrase(combined):
        base += 0.04
    if any(k in combined for k in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "oil", "crude", "wti", "gold", "sp500", "s&p", "nasdaq", "dow"]):
        base -= 0.06
    if looks_like_short_window_price_market(combined):
        base -= 0.50
    if looks_like_crypto_ladder_market(combined):
        base -= 0.55
    if looks_like_strike_ladder_market(combined):
        base -= 0.60
    if looks_like_shell_price_market(combined):
        base -= 0.60

    return round(base, 4)


def build_candidate_from_slug(
    slug: str,
    baseline_prob: float,
    discovery_priority_value: float,
    source_bucket: str,
) -> Dict[str, Any]:
    market_data, source_type = resolve_slug_to_market(slug)
    if not market_data:
        return {"ok": False, "reason": "resolve_failed", "slug": slug}

    token_id = extract_yes_token_id(market_data)
    if not token_id:
        return {"ok": False, "reason": "token_missing", "slug": slug, "market_data": market_data, "source_type": source_type}

    micro = preflight_book_check(market_data, token_id)
    if not micro.get("ok"):
        return {
            "ok": False,
            "reason": micro.get("reason", "preflight_failed"),
            "slug": slug,
            "market_data": market_data,
            "source_type": source_type,
            "token_id": token_id,
            "micro": micro,
        }

    return {
        "ok": True,
        "slug": slug,
        "baseline_prob": baseline_prob,
        "discovery_priority": discovery_priority_value,
        "source_bucket": source_bucket,
        "source_type": source_type,
        "topic_category": micro["topic_category"],
        "preflight_spread": micro["spread"],
        "preflight_midpoint": micro["midpoint"],
        "preflight_bid_depth": micro["bid_depth"],
        "preflight_ask_depth": micro["ask_depth"],
        "cached_market_data": market_data,
        "cached_token_id": token_id,
        "cached_book": micro["book"],
        "cached_book_ts": time.time(),
        "cached_label": micro["label"],
        "cached_resolved_text": micro["resolved_text"],
    }


def discover_markets() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    prelim: List[Dict[str, Any]] = []
    seen = set()
    offset = 0

    stats = {
        "discover_prelim": 0,
        "discover_checked": 0,
        "discover_kept": 0,
        "discover_resolve_failures": 0,
        "discover_preflight_failures": 0,
        "discover_preflight_reason_counts": {},
        "discover_near_passes": [],
        "discover_family_skips": 0,
        "discover_hard_skipped": 0,
        "discover_family_bucket_skips": 0,
        "discover_accept_samples": [],
        "discover_skip_samples": [],
    }

    print(
        f"[{now_str()}] Discover start | "
        f"limit={DISCOVER_LIMIT} page_size={DISCOVER_PAGE_SIZE} "
        f"min_volume={DISCOVER_MIN_VOLUME} min_liquidity={DISCOVER_MIN_LIQUIDITY} "
        f"short_term_only={SHORT_TERM_ONLY} max_days_to_end={MAX_DAYS_TO_END}"
    )

    family_seen = set()
    family_bucket_seen = set()

    while len(prelim) < DISCOVER_LIMIT:
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
            hard_skip, hard_reason = should_hard_skip_discovery_market(combined)
            if hard_skip:
                stats["discover_hard_skipped"] += 1
                samples = stats.setdefault("discover_skip_samples", [])
                if len(samples) < 5:
                    samples.append({"slug": slug, "reason": hard_reason, "question": question})
                continue
            if not is_allowed_topic(combined):
                continue
            if not matches_keywords(combined):
                continue

            family_key = canonical_market_family_key(slug, question)
            family_bucket = extract_asset_family_bucket(combined) if family_key.startswith("family:") else ""
            if family_key and family_key in family_seen:
                stats["discover_family_skips"] += 1
                continue
            if family_bucket and family_bucket in family_bucket_seen:
                stats["discover_family_bucket_skips"] += 1
                continue

            seen.add(slug)
            if family_key:
                family_seen.add(family_key)
            if family_bucket:
                family_bucket_seen.add(family_bucket)

            prelim.append({
                "slug": slug,
                "baseline_prob": BASELINE_PROB,
                "discovery_priority": discovery_priority(combined),
                "family_key": family_key,
                "family_bucket": family_bucket,
                "question": question,
            })
            accept_samples = stats.setdefault("discover_accept_samples", [])
            if len(accept_samples) < 5:
                accept_samples.append({"slug": slug, "question": question, "priority": discovery_priority(combined)})
            if len(prelim) >= DISCOVER_LIMIT:
                break

        offset += DISCOVER_PAGE_SIZE

    stats["discover_prelim"] = len(prelim)
    prelim.sort(key=lambda x: -to_float(x.get("discovery_priority"), 0.0))

    kept: List[Dict[str, Any]] = []
    for item in prelim:
        if stats["discover_checked"] >= PREFLIGHT_MAX_CANDIDATES:
            break

        stats["discover_checked"] += 1
        candidate = build_candidate_from_slug(
            slug=item["slug"],
            baseline_prob=to_float(item.get("baseline_prob"), BASELINE_PROB),
            discovery_priority_value=to_float(item.get("discovery_priority"), 0.0),
            source_bucket="discover",
        )
        if not candidate.get("ok"):
            reason = candidate.get("reason", "unknown")
            if reason in {"resolve_failed", "token_missing"}:
                stats["discover_resolve_failures"] += 1
            else:
                stats["discover_preflight_failures"] += 1
                reason_counts = stats.setdefault("discover_preflight_reason_counts", {})
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                record_preflight_near_pass(stats, candidate, "discover")
            continue

        kept.append(candidate)

    stats["discover_kept"] = len(kept)

    kept.sort(
        key=lambda x: (
            CATEGORY_PRIORITY.get(x.get("topic_category", "other"), 9),
            -to_float(x.get("discovery_priority"), 0.0),
            to_float(x.get("preflight_spread"), 9.0),
            -min(to_float(x.get("preflight_bid_depth"), 0.0), to_float(x.get("preflight_ask_depth"), 0.0)),
        )
    )

    print(
        f"[{now_str()}] Discover complete | prelim={stats['discover_prelim']} "
        f"checked={stats['discover_checked']} kept={stats['discover_kept']} family_skips={stats['discover_family_skips']} hard_skipped={stats['discover_hard_skipped']} bucket_skips={stats['discover_family_bucket_skips']}"
    )
    return kept, stats


def fetch_watchlist_file() -> List[Dict[str, Any]]:
    try:
        r = requests.get(MARKETS_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        lines = [x.strip() for x in r.text.splitlines() if x.strip()]
        items = []
        family_seen = set()
        family_bucket_seen = set()
        for line in lines:
            parsed = parse_watchlist_line(line)
            if not parsed:
                continue
            slug_text = parsed["slug"].lower()
            if is_blocked_topic(slug_text):
                continue
            hard_skip, _ = should_hard_skip_discovery_market(slug_text)
            if hard_skip:
                continue
            if not is_allowed_topic(slug_text):
                continue
            family_key = canonical_market_family_key(parsed["slug"], parsed["slug"])
            family_bucket = extract_asset_family_bucket(slug_text) if family_key.startswith("family:") else ""
            if family_key and family_key in family_seen:
                continue
            if family_bucket and family_bucket in family_bucket_seen:
                continue
            if family_key:
                family_seen.add(family_key)
            if family_bucket:
                family_bucket_seen.add(family_bucket)
            items.append(parsed)
        print(f"[{now_str()}] Watchlist file loaded | items={len(items)}")
        return items
    except Exception as e:
        print(f"[{now_str()}] Watchlist fetch error: {e}")
        return []


def build_prefiltered_fallback_candidates() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    file_items = fetch_watchlist_file()
    stats = {
        "fallback_items": len(file_items),
        "fallback_checked": 0,
        "fallback_kept": 0,
        "fallback_resolve_failures": 0,
        "fallback_preflight_failures": 0,
        "fallback_preflight_reason_counts": {},
        "fallback_used": False,
    }

    kept: List[Dict[str, Any]] = []
    for item in file_items[:PREFLIGHT_MAX_CANDIDATES]:
        stats["fallback_checked"] += 1
        candidate = build_candidate_from_slug(
            slug=item["slug"],
            baseline_prob=to_float(item.get("baseline_prob"), BASELINE_PROB),
            discovery_priority_value=0.50,
            source_bucket="fallback",
        )
        if not candidate.get("ok"):
            reason = candidate.get("reason", "unknown")
            if reason in {"resolve_failed", "token_missing"}:
                stats["fallback_resolve_failures"] += 1
            else:
                stats["fallback_preflight_failures"] += 1
                reason_counts = stats.setdefault("fallback_preflight_reason_counts", {})
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                record_preflight_near_pass(stats, candidate, "fallback")
            continue
        kept.append(candidate)

    stats["fallback_kept"] = len(kept)
    if kept:
        stats["fallback_used"] = True

    kept.sort(
        key=lambda x: (
            CATEGORY_PRIORITY.get(x.get("topic_category", "other"), 9),
            to_float(x.get("preflight_spread"), 9.0),
            -min(to_float(x.get("preflight_bid_depth"), 0.0), to_float(x.get("preflight_ask_depth"), 0.0)),
        )
    )
    return kept, stats


def fetch_watchlist() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if TEST_MARKET_SLUG:
        candidate = build_candidate_from_slug(
            slug=TEST_MARKET_SLUG,
            baseline_prob=BASELINE_PROB,
            discovery_priority_value=1.0,
            source_bucket="test",
        )
        stats = {
            "discover_prelim": 0,
            "discover_checked": 0,
            "discover_kept": 0,
            "discover_resolve_failures": 0,
            "discover_preflight_failures": 0,
            "discover_preflight_reason_counts": {},
            "fallback_items": 0,
            "fallback_checked": 0,
            "fallback_kept": 0,
            "fallback_resolve_failures": 0,
            "fallback_preflight_failures": 0,
            "fallback_preflight_reason_counts": {},
            "fallback_used": False,
            "watchlist_source": "test",
        }
        return ([candidate] if candidate else []), stats

    discover_stats = {
        "discover_prelim": 0,
        "discover_checked": 0,
        "discover_kept": 0,
        "discover_resolve_failures": 0,
        "discover_preflight_failures": 0,
        "discover_preflight_reason_counts": {},
        "discover_hard_skipped": 0,
        "discover_family_skips": 0,
        "discover_family_bucket_skips": 0,
        "discover_accept_samples": [],
        "discover_skip_samples": [],
    }

    if AUTO_DISCOVER:
        discovered, discover_stats = discover_markets()
        if discovered:
            discover_stats.update({
                "fallback_items": 0,
                "fallback_checked": 0,
                "fallback_kept": 0,
                "fallback_resolve_failures": 0,
                "fallback_preflight_failures": 0,
                "fallback_preflight_reason_counts": {},
                "fallback_used": False,
                "watchlist_source": "discover",
            })
            return discovered, discover_stats

    fallback, fallback_stats = build_prefiltered_fallback_candidates()
    combined = {}
    combined.update(discover_stats)
    combined.update(fallback_stats)
    combined["watchlist_source"] = "fallback" if fallback else "none"
    return fallback, combined


# =========================================
# LIVE MARKET DATA
# =========================================
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


def compute_order_book_microstructure(
    bids: List[Dict[str, Any]],
    asks: List[Dict[str, Any]],
    best_bid: float,
    best_ask: float,
) -> Dict[str, float]:
    bid_depth = top_depth(bids, BOOK_LEVELS)
    ask_depth = top_depth(asks, BOOK_LEVELS)
    best_bid_size = best_level_size(bids)
    best_ask_size = best_level_size(asks)

    total_depth = bid_depth + ask_depth
    obi = 0.0
    if total_depth > 0:
        obi = (bid_depth - ask_depth) / total_depth

    spread = max(0.0, best_ask - best_bid)
    midpoint = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0

    return {
        "bid_depth": round(bid_depth, 6),
        "ask_depth": round(ask_depth, 6),
        "best_bid_size": round(best_bid_size, 6),
        "best_ask_size": round(best_ask_size, 6),
        "obi": round(obi, 6),
        "midpoint": round(midpoint, 6),
        "spread": round(spread, 6),
    }


def estimate_fill_confidence(entry_price: float, top_side_depth: float, spread: float, midpoint: float) -> Tuple[float, str]:
    if entry_price <= 0:
        return 0.0, "no-entry"
    if spread <= 0:
        return 0.0, "no-spread"
    if top_side_depth <= 0:
        return 0.0, "no-depth"

    target_notional = TARGET_MAX_TEST_POSITION_USD
    est_shares = target_notional / max(entry_price, 1e-9)
    depth_ratio = top_side_depth / max(est_shares, 1e-9)

    depth_score = clamp(depth_ratio / 3.0, 0.0, 1.0)
    spread_score = clamp((0.10 - spread) / 0.10, 0.0, 1.0)
    midpoint_score = 1.0 if MID_PRICE_MIN <= midpoint <= MID_PRICE_MAX else 0.0

    score = (depth_score * 0.45) + (spread_score * 0.35) + (midpoint_score * 0.20)

    if score >= 0.75:
        note = "fill-strong"
    elif score >= 0.55:
        note = "fill-possible"
    else:
        note = "fill-weak"

    return round(score, 4), note


def estimate_test_position_quality(best_bid: float, best_ask: float, liquidity: float, midpoint: float) -> Tuple[float, str]:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0, "no-two-sided-book"
    if best_ask > EARLY_MAX_ASK:
        return 0.0, "ask-pinned"
    if best_bid < EARLY_MIN_BID:
        return 0.0, "bid-dead"

    spread = max(0.0, best_ask - best_bid)
    spread_component = clamp((0.10 - spread) / 0.10, 0.0, 1.0)
    liquidity_component = min(liquidity / max(MIN_LIQUIDITY * 4.0, 1.0), 1.0)
    midpoint_component = 1.0 if MID_PRICE_MIN <= midpoint <= MID_PRICE_MAX else 0.0

    score = (spread_component * 0.45) + (liquidity_component * 0.25) + (midpoint_component * 0.30)

    if score >= 0.75:
        return round(score, 4), "small-size-friendly"
    if score >= 0.55:
        return round(score, 4), "small-size-possible"
    return round(score, 4), "small-size-weak"


def estimate_shares(total_usd: float, price_per_share: float) -> float:
    if price_per_share <= 0:
        return 0.0
    return round(total_usd / price_per_share, 2)


def time_to_event_score(end_iso: str) -> float:
    dte = days_to_end(end_iso)
    if dte is None or dte < 0:
        return 0.0
    if dte <= 1:
        return 1.0
    if dte <= 3:
        return 0.90
    if dte <= 7:
        return 0.80
    if dte <= 10:
        return 0.65
    if dte <= 14:
        return 0.50
    return 0.25


def midpoint_zone_score(midpoint: float) -> float:
    if 0.20 <= midpoint <= 0.80:
        return 1.0
    if 0.15 <= midpoint <= 0.85:
        return 0.80
    if MID_PRICE_MIN <= midpoint <= MID_PRICE_MAX:
        return 0.55
    return 0.0


def fetch_live_market_data(item: Dict[str, Any], pipeline_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    slug = item["slug"]
    baseline_prob = to_float(item.get("baseline_prob"), BASELINE_PROB)

    market_data = item.get("cached_market_data")
    token_id = item.get("cached_token_id")
    source_type = item.get("source_type", "unknown")
    book = item.get("cached_book")
    cached_book_ts = to_float(item.get("cached_book_ts"), 0.0)

    if not market_data or not token_id:
        raise ValueError("Cached candidate missing market/token")

    cache_source = "candidate_cache"
    cache_age_seconds = round(max(0.0, time.time() - cached_book_ts), 3) if cached_book_ts else None
    stale_cache = bool(cached_book_ts and cache_age_seconds is not None and cache_age_seconds > CANDIDATE_CACHE_TTL_SECONDS)

    if pipeline_stats is not None:
        pipeline_stats.setdefault("cache_used", 0)
        pipeline_stats.setdefault("cache_refetched", 0)
        pipeline_stats.setdefault("cache_missing", 0)
        pipeline_stats.setdefault("cache_fetch_failures", 0)
        pipeline_stats.setdefault("cache_stale", 0)

    if not isinstance(book, dict):
        cache_source = "fresh_fetch_missing_cache"
        if pipeline_stats is not None:
            pipeline_stats["cache_missing"] += 1
        try:
            book = fetch_order_book(token_id)
            item["cached_book"] = book
            item["cached_book_ts"] = time.time()
            cache_age_seconds = 0.0
        except Exception:
            if pipeline_stats is not None:
                pipeline_stats["cache_fetch_failures"] += 1
            raise
    elif stale_cache:
        cache_source = "fresh_fetch_stale_cache"
        if pipeline_stats is not None:
            pipeline_stats["cache_stale"] += 1
            pipeline_stats["cache_refetched"] += 1
        try:
            book = fetch_order_book(token_id)
            item["cached_book"] = book
            item["cached_book_ts"] = time.time()
            cache_age_seconds = 0.0
        except Exception:
            if pipeline_stats is not None:
                pipeline_stats["cache_fetch_failures"] += 1
            raise
    else:
        if pipeline_stats is not None:
            pipeline_stats["cache_used"] += 1

    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = to_float(bids[0].get("price")) if bids else 0.0
    best_ask = to_float(asks[0].get("price")) if asks else 0.0
    last_trade = to_float(book.get("last_trade_price"), 0.0)

    dead_book, dead_reason = is_dead_book(best_bid, best_ask)
    spread = round(best_ask - best_bid, 6) if best_bid > 0 and best_ask > 0 else 0.0
    live_prob, price_source = choose_live_prob(best_bid, best_ask, last_trade)
    liquidity = to_float(market_data.get("liquidityClob", market_data.get("liquidity", 0.0)), 0.0)

    label = market_data.get("question") or market_data.get("title") or market_data.get("slug") or slug
    resolved_text = f"{market_data.get('slug', '')} {market_data.get('question', '')} {market_data.get('title', '')}".lower()
    topic_category = classify_topic(resolved_text)

    end_dt = extract_end_datetime(market_data)
    end_iso = end_dt.isoformat() if end_dt else ""

    micro = compute_order_book_microstructure(bids, asks, best_bid, best_ask)
    tiny_position_score, tiny_position_note = estimate_test_position_quality(
        best_bid, best_ask, liquidity, micro["midpoint"]
    )

    yes_entry_price = best_ask if best_ask > 0 else 0.0
    no_entry_price = round(1.0 - best_bid, 6) if best_bid > 0 else 0.0

    yes_fill_confidence, yes_fill_note = estimate_fill_confidence(
        yes_entry_price, micro["ask_depth"], spread, micro["midpoint"]
    )
    no_fill_confidence, no_fill_note = estimate_fill_confidence(
        no_entry_price, micro["bid_depth"], spread, 1.0 - micro["midpoint"] if micro["midpoint"] > 0 else 0.0
    )

    return {
        "market_id": market_data.get("id", slug),
        "slug": slug,
        "source_type": source_type,
        "source_bucket": item.get("source_bucket", "unknown"),
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
        "end_dt": end_iso,
        "tiny_position_score": tiny_position_score,
        "tiny_position_note": tiny_position_note,
        "dead_book": dead_book,
        "dead_reason": dead_reason,
        "yes_entry_price": yes_entry_price,
        "no_entry_price": no_entry_price,
        "resolved_text": resolved_text,
        "bid_depth": micro["bid_depth"],
        "ask_depth": micro["ask_depth"],
        "best_bid_size": micro["best_bid_size"],
        "best_ask_size": micro["best_ask_size"],
        "obi": micro["obi"],
        "midpoint": micro["midpoint"],
        "yes_fill_confidence": yes_fill_confidence,
        "yes_fill_note": yes_fill_note,
        "no_fill_confidence": no_fill_confidence,
        "no_fill_note": no_fill_note,
        "time_score": time_to_event_score(end_iso),
        "midpoint_zone_score": midpoint_zone_score(micro["midpoint"]),
        "discovery_priority": to_float(item.get("discovery_priority"), 0.0),
        "cache_source": cache_source,
        "cache_age_seconds": cache_age_seconds,
    }


# =========================================
# CLASSIFICATION / SCORING
# =========================================
def side_micro_score(
    side: str,
    live_prob: float,
    baseline_prob: float,
    spread: float,
    liquidity: float,
    tiny_position_score: float,
    midpoint_zone: float,
    time_score: float,
    obi: float,
    fill_confidence: float,
    category: str,
) -> Tuple[float, float]:
    yes_edge = baseline_prob - live_prob
    no_edge = live_prob - baseline_prob
    edge = yes_edge if side == "YES" else no_edge

    spread_score = clamp((MAX_SPREAD - spread) / max(MAX_SPREAD, 1e-9), 0.0, 1.0)
    liquidity_score = min(liquidity / max(MIN_LIQUIDITY * 4.0, 1.0), 1.0)

    if side == "YES":
        pressure = clamp((obi + 1.0) / 2.0, 0.0, 1.0)
    else:
        pressure = clamp((1.0 - obi) / 2.0, 0.0, 1.0)

    category_bonus = {
        "crypto": 1.00,
        "macro": 0.95,
        "index_commodity": 0.92,
        "politics_event": 0.86,
        "other": 0.78,
    }.get(category, 0.78)

    edge_score = 0.0
    if edge >= abs(ALERT_THRESHOLD):
        edge_score = 1.0
    elif edge >= abs(WATCH_THRESHOLD):
        edge_score = 0.60
    elif edge > 0:
        edge_score = clamp(edge / abs(WATCH_THRESHOLD), 0.0, 0.45)

    total = (
        (tiny_position_score * 0.28) +
        (pressure * 0.20) +
        (fill_confidence * 0.18) +
        (liquidity_score * 0.10) +
        (spread_score * 0.08) +
        (time_score * 0.10) +
        (midpoint_zone * 0.06)
    )

    total = total * (0.85 + 0.15 * edge_score) * category_bonus
    return round(total, 4), round(edge, 4)


def classify_market(m: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    liquidity = to_float(m.get("liquidity", 0), 0.0)
    spread = to_float(m.get("spread", 0), 0.0)
    best_bid = to_float(m.get("best_bid", 0), 0.0)
    best_ask = to_float(m.get("best_ask", 0), 0.0)
    price_source = (m.get("price_source") or "").strip()
    live_prob = to_float(m.get("live_prob", 0.0), 0.0)
    baseline_prob = to_float(m.get("baseline_prob"), BASELINE_PROB)
    tiny_position_score = to_float(m.get("tiny_position_score", 0.0), 0.0)
    midpoint_zone = to_float(m.get("midpoint_zone_score", 0.0), 0.0)
    time_score = to_float(m.get("time_score", 0.0), 0.0)
    obi = to_float(m.get("obi", 0.0), 0.0)
    category = m.get("topic_category", "other")
    bid_depth = to_float(m.get("bid_depth", 0.0), 0.0)
    ask_depth = to_float(m.get("ask_depth", 0.0), 0.0)

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
    if bid_depth < MIN_TOP_DEPTH:
        return "SKIP", "Bid depth too thin"
    if ask_depth < MIN_TOP_DEPTH:
        return "SKIP", "Ask depth too thin"
    if tiny_position_score < 0.45:
        return "SKIP", f"Small-size weak ({m.get('tiny_position_note', 'weak')})"
    if midpoint_zone <= 0:
        return "SKIP", "Midpoint outside tradable zone"

    yes_score, yes_edge = side_micro_score(
        "YES",
        live_prob,
        baseline_prob,
        spread,
        liquidity,
        tiny_position_score,
        midpoint_zone,
        time_score,
        obi,
        to_float(m.get("yes_fill_confidence", 0.0), 0.0),
        category,
    )
    no_score, no_edge = side_micro_score(
        "NO",
        live_prob,
        baseline_prob,
        spread,
        liquidity,
        tiny_position_score,
        midpoint_zone,
        time_score,
        obi,
        to_float(m.get("no_fill_confidence", 0.0), 0.0),
        category,
    )

    if yes_score >= no_score:
        side = "YES"
        composite = yes_score
        edge = yes_edge
        fill_conf = to_float(m.get("yes_fill_confidence", 0.0), 0.0)
        fill_note = m.get("yes_fill_note", "")
        entry_price = to_float(m.get("yes_entry_price", 0.0), 0.0)
        side_depth = ask_depth
    else:
        side = "NO"
        composite = no_score
        edge = no_edge
        fill_conf = to_float(m.get("no_fill_confidence", 0.0), 0.0)
        fill_note = m.get("no_fill_note", "")
        entry_price = to_float(m.get("no_entry_price", 0.0), 0.0)
        side_depth = bid_depth

    m["signal_side"] = side
    m["edge"] = edge
    m["composite_score"] = round(composite, 4)
    m["fill_confidence"] = fill_conf
    m["fill_note"] = fill_note
    m["entry_price"] = entry_price
    m["side_depth"] = side_depth
    m["yes_prob"] = live_prob
    m["no_prob"] = 1.0 - live_prob
    m["imbalance"] = live_prob - baseline_prob
    m["est_shares_min"] = estimate_shares(TARGET_MIN_TEST_POSITION_USD, entry_price)
    m["est_shares_max"] = estimate_shares(TARGET_MAX_TEST_POSITION_USD, entry_price)

    if side_depth < MIN_TOP_DEPTH:
        return "SKIP", "Chosen side depth too thin"
    if fill_conf < MIN_FILL_CONFIDENCE:
        return "SKIP", f"Fill confidence weak ({fill_note})"

    if composite >= MICRO_ALERT_SCORE and edge > 0:
        return "ALERT", f"{side} microstructure score={composite:.3f} edge={edge:.3f}"

    if composite >= MICRO_WATCH_SCORE:
        return "WATCH", f"{side} near-ready score={composite:.3f} edge={edge:.3f}"

    return "SKIP", "No qualifying signal"


# =========================================
# SCAN
# =========================================
def scan_markets() -> Dict[str, Any]:
    global last_skip_counts, last_pipeline_stats, last_preflight_reason_counts, last_near_passes, last_discovery_samples

    watchlist, pipeline_stats = fetch_watchlist()
    results: List[Dict[str, Any]] = []
    counts = {"total": 0, "alert": 0, "watch": 0, "skip": 0}
    skip_counter = Counter()
    resolve_failures = 0

    print(f"[{now_str()}] Scan start | watchlist_items={len(watchlist)} source={pipeline_stats.get('watchlist_source')}")

    for item in watchlist:
        counts["total"] += 1
        try:
            m = fetch_live_market_data(item, pipeline_stats=pipeline_stats)
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
                skip_counter[reason or "Unknown"] += 1

        except Exception as e:
            print(f"[{now_str()}] Market scan error for {item.get('slug')}: {e}")
            counts["skip"] += 1
            resolve_failures += 1
            skip_counter["Fetch/resolve error"] += 1

    pipeline_stats["scan_resolve_failures"] = resolve_failures
    pipeline_stats.setdefault("discover_preflight_reason_counts", {})
    pipeline_stats.setdefault("fallback_preflight_reason_counts", {})
    last_pipeline_stats = pipeline_stats
    last_preflight_reason_counts = {
        "discover": dict(pipeline_stats.get("discover_preflight_reason_counts", {})),
        "fallback": dict(pipeline_stats.get("fallback_preflight_reason_counts", {})),
    }
    last_near_passes = {
        "discover": list(pipeline_stats.get("discover_near_passes", []))[:5],
        "fallback": list(pipeline_stats.get("fallback_near_passes", []))[:5],
    }
    last_discovery_samples = {
        "accepted": list(pipeline_stats.get("discover_accept_samples", []))[:5],
        "hard_skipped": list(pipeline_stats.get("discover_skip_samples", []))[:5],
    }
    last_skip_counts = skip_counter

    results.sort(
        key=lambda x: (
            0 if x.get("category") == "ALERT" else 1 if x.get("category") == "WATCH" else 2,
            CATEGORY_PRIORITY.get(x.get("topic_category", "other"), 9),
            0 if not x.get("dead_book") else 1,
            -to_float(x.get("composite_score"), 0.0),
            -to_float(x.get("fill_confidence"), 0.0),
            -to_float(x.get("tiny_position_score"), 0.0),
            -to_float(x.get("time_score"), 0.0),
            -abs(to_float(x.get("obi"), 0.0)),
        )
    )

    print(
        f"[{now_str()}] Scan complete | "
        f"total={counts['total']} alert={counts['alert']} "
        f"watch={counts['watch']} skip={counts['skip']}"
    )
    return {"counts": counts, "results": results[:30], "pipeline": pipeline_stats}


def qualifying_results(scan: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        r for r in scan["results"]
        if r["category"] == "ALERT" or (r["category"] == "WATCH" and SEND_WATCH_ALERTS)
    ]


def summarize_skip_reasons(results: List[Dict[str, Any]]) -> str:
    reasons = [r.get("reason", "Unknown") for r in results if r.get("category") == "SKIP"]
    if not reasons:
        reasons = list(last_skip_counts.elements())
    if not reasons:
        return "No skip reasons available."
    top = Counter(reasons).most_common(6)
    return " | ".join(f"{k}: {v}" for k, v in top)


def near_miss_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    usable = [
        r for r in results
        if not r.get("dead_book")
        and r.get("reason") not in {
            "Bid too low",
            "Ask too high",
            "Spread too wide",
            "Liquidity too low",
            "Last trade only",
            "Bid depth too thin",
            "Ask depth too thin",
            "Chosen side depth too thin",
            "Fetch/resolve error",
        }
    ]
    usable.sort(
        key=lambda x: (
            -to_float(x.get("composite_score"), 0.0),
            -to_float(x.get("fill_confidence"), 0.0),
            -to_float(x.get("tiny_position_score"), 0.0),
            -to_float(x.get("time_score"), 0.0),
        )
    )
    return usable[:3]


def format_pipeline_stats(pipeline: Dict[str, Any]) -> str:
    discover_reasons = pipeline.get("discover_preflight_reason_counts", {}) or {}
    fallback_reasons = pipeline.get("fallback_preflight_reason_counts", {}) or {}

    def top_reason(reason_counts: Dict[str, Any]) -> str:
        if not reason_counts:
            return "none"
        top_key = max(reason_counts, key=lambda k: reason_counts.get(k, 0))
        return f"{top_key}:{reason_counts.get(top_key, 0)}"

    return (
        f"Pipeline: source={pipeline.get('watchlist_source', 'unknown')} | "
        f"discover_prelim={pipeline.get('discover_prelim', 0)} | "
        f"discover_hard_skipped={pipeline.get('discover_hard_skipped', 0)} | "
        f"discover_family_skips={pipeline.get('discover_family_skips', 0)} | "
        f"discover_bucket_skips={pipeline.get('discover_family_bucket_skips', 0)} | "
        f"discover_checked={pipeline.get('discover_checked', 0)} | "
        f"discover_kept={pipeline.get('discover_kept', 0)} | "
        f"fallback_items={pipeline.get('fallback_items', 0)} | "
        f"fallback_checked={pipeline.get('fallback_checked', 0)} | "
        f"fallback_kept={pipeline.get('fallback_kept', 0)} | "
        f"cache_used={pipeline.get('cache_used', 0)} | "
        f"cache_refetched={pipeline.get('cache_refetched', 0)} | "
        f"resolve_failures={pipeline.get('scan_resolve_failures', 0)} | "
        f"top_preflight_discover={top_reason(discover_reasons)} | "
        f"top_preflight_fallback={top_reason(fallback_reasons)}"
    )


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
        f"fill={r.get('fill_note', '')} fill_score={r.get('fill_confidence', 0):.3f} "
        f"tiny_size={r.get('tiny_position_note', '')} tiny_score={r.get('tiny_position_score', 0):.3f}\n"
        f"obi={r.get('obi', 0):.3f} "
        f"bid_depth={r.get('bid_depth', 0):.2f} "
        f"ask_depth={r.get('ask_depth', 0):.2f} "
        f"side_depth={r.get('side_depth', 0):.2f} "
        f"time_score={r.get('time_score', 0):.2f}\n"
        f"bid={r.get('best_bid', 0):.3f} "
        f"ask={r.get('best_ask', 0):.3f} "
        f"spread={r.get('spread', 0):.3f} "
        f"liq={r.get('liquidity', 0):.0f} "
        f"src={r.get('price_source', '')} "
        f"cache={r.get('cache_source', '')}"
    )




def format_preflight_near_passes(pipeline: Dict[str, Any]) -> List[str]:
    discover = list(pipeline.get("discover_near_passes", []) or [])
    fallback = list(pipeline.get("fallback_near_passes", []) or [])
    combined = []
    for item in discover:
        combined.append(("discover", item))
    for item in fallback:
        combined.append(("fallback", item))

    if not combined:
        return []

    combined.sort(key=lambda x: (to_float(x[1].get("distance_to_pass"), 999.0), to_float(x[1].get("spread"), 999.0), -max(to_float(x[1].get("bid_depth"), 0.0), to_float(x[1].get("ask_depth"), 0.0))))
    lines = ["Closest preflight near-passes:", ""]
    for source, item in combined[:5]:
        lines.append(
            f"{source.upper()} | {item.get('label', '')}\n"
            f"slug={item.get('slug', '')}\n"
            f"reason={item.get('reason', '')} | {item.get('distance_note', '') or ('distance=' + str(item.get('distance_to_pass', '')))}\n"
            f"bid={to_float(item.get('best_bid'), 0.0):.3f} ask={to_float(item.get('best_ask'), 0.0):.3f} spread={to_float(item.get('spread'), 0.0):.3f}\n"
            f"bid_depth={to_float(item.get('bid_depth'), 0.0):.2f} ask_depth={to_float(item.get('ask_depth'), 0.0):.2f} midpoint={to_float(item.get('midpoint'), 0.0):.3f} side_hint={item.get('side_hint', '')}"
        )
        lines.append("")
    return lines

def format_zero_summary(zero_count: int, seconds_in_window: int) -> str:
    minutes = max(1, round(seconds_in_window / 60))
    pipeline = last_pipeline_stats or {}
    discover_reasons = (last_preflight_reason_counts or {}).get("discover", {}) or {}
    top_reason = "none"
    if discover_reasons:
        key = max(discover_reasons, key=lambda k: discover_reasons.get(k, 0))
        top_reason = f"{key}:{discover_reasons.get(key, 0)}"
    near = (last_near_passes or {}).get("discover", []) or []
    near_text = "none"
    if near:
        first = near[0]
        near_text = f"{first.get('reason', 'unknown')} ({first.get('distance_note', '') or first.get('distance_to_pass', '')})"
    return (
        f"No qualifying markets in last {minutes} minutes.\n"
        f"Empty scans: {zero_count}\n"
        f"Top preflight block: {top_reason}\n"
        f"Hard-skipped in discovery: {pipeline.get('discover_hard_skipped', 0)} | family skips: {pipeline.get('discover_family_skips', 0)} | bucket skips: {pipeline.get('discover_family_bucket_skips', 0)}\n"
        f"Closest near-pass: {near_text}"
    )


def format_manual_scan(scan: Dict[str, Any]) -> Optional[str]:
    top = qualifying_results(scan)
    counts = scan.get("counts", {})
    results = scan.get("results", [])
    pipeline = scan.get("pipeline", {})

    if top:
        lines = [
            "Scan complete",
            "",
            f"Total: {counts.get('total', 0)}",
            f"Alerts: {counts.get('alert', 0)}",
            f"Watch: {counts.get('watch', 0)}",
            f"Skip: {counts.get('skip', 0)}",
            format_pipeline_stats(pipeline),
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
            format_pipeline_stats(pipeline),
            "",
            f"Near-miss summary: {summarize_skip_reasons(results)}",
        ]

        near = near_miss_results(results)
        if near:
            lines.extend(["", "Closest tradable near-misses:", ""])
            for r in near:
                lines.append(
                    f"{r.get('category', 'SKIP')} | {r.get('label', '')}\n"
                    f"slug={r.get('slug', '')}\n"
                    f"reason={r.get('reason', 'No qualifying signal')}\n"
                    f"topic={r.get('topic_category', 'other')} side={r.get('signal_side', '?')}\n"
                    f"ends={r.get('end_dt', '') or 'unknown'}\n"
                    f"entry_price={r.get('entry_price', 0):.3f} "
                    f"est_shares={r.get('est_shares_min', 0)}-{r.get('est_shares_max', 0)}\n"
                    f"fill={r.get('fill_note', '')} fill_score={r.get('fill_confidence', 0):.3f} "
                    f"tiny={r.get('tiny_position_note', '')} tiny_score={r.get('tiny_position_score', 0):.3f}\n"
                    f"obi={r.get('obi', 0):.3f} "
                    f"bid_depth={r.get('bid_depth', 0):.2f} "
                    f"ask_depth={r.get('ask_depth', 0):.2f} "
                    f"spread={r.get('spread', 0):.3f} liq={r.get('liquidity', 0):.0f}"
                )
                lines.append("")
        else:
            lines.extend([
                "",
                "No tradable near-misses found.",
                "Most candidates failed preflight, depth, fill confidence, or cached candidate resolution.",
                f"Preflight detail: discover={pipeline.get('discover_preflight_reason_counts', {})} fallback={pipeline.get('fallback_preflight_reason_counts', {})}",
            ])
            preflight_near = format_preflight_near_passes(pipeline)
            if preflight_near:
                lines.extend([""] + preflight_near)

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
        "script_version": SCRIPT_VERSION,
        "rolling_discovery_days": ROLLING_DISCOVERY_DAYS,
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
        "script_version": SCRIPT_VERSION,
        "rolling_discovery_days": ROLLING_DISCOVERY_DAYS,
        "alert_threshold": ALERT_THRESHOLD,
        "watch_threshold": WATCH_THRESHOLD,
        "send_watch_alerts": SEND_WATCH_ALERTS,
        "debug_scan_output": DEBUG_SCAN_OUTPUT,
        "scan_every_seconds": SCAN_EVERY_SECONDS,
        "short_term_only": SHORT_TERM_ONLY,
        "max_days_to_end": MAX_DAYS_TO_END,
        "discover_limit": DISCOVER_LIMIT,
        "discover_min_volume": DISCOVER_MIN_VOLUME,
        "discover_min_liquidity": DISCOVER_MIN_LIQUIDITY,
        "min_liquidity": MIN_LIQUIDITY,
        "max_spread": MAX_SPREAD,
        "min_bid": MIN_BID,
        "max_ask": MAX_ASK,
        "early_min_bid": EARLY_MIN_BID,
        "early_max_ask": EARLY_MAX_ASK,
        "early_max_spread": EARLY_MAX_SPREAD,
        "mid_price_min": MID_PRICE_MIN,
        "mid_price_max": MID_PRICE_MAX,
        "block_crypto_ladders": BLOCK_CRYPTO_LADDERS,
        "book_levels": BOOK_LEVELS,
        "min_top_depth": MIN_TOP_DEPTH,
        "min_fill_confidence": MIN_FILL_CONFIDENCE,
        "micro_alert_score": MICRO_ALERT_SCORE,
        "micro_watch_score": MICRO_WATCH_SCORE,
        "preflight_max_spread": PREFLIGHT_MAX_SPREAD,
        "preflight_min_bid": PREFLIGHT_MIN_BID,
        "preflight_max_ask": PREFLIGHT_MAX_ASK,
        "preflight_max_candidates": PREFLIGHT_MAX_CANDIDATES,
        "auto_discover": AUTO_DISCOVER,
        "enable_yes_no_only": ENABLE_YES_NO_ONLY,
        "manual_scan_in_progress": manual_scan_in_progress,
        "target_min_test_position_usd": TARGET_MIN_TEST_POSITION_USD,
        "target_max_test_position_usd": TARGET_MAX_TEST_POSITION_USD,
        "test_market_slug": TEST_MARKET_SLUG or "none",
        "last_pipeline_stats": last_pipeline_stats,
        "last_preflight_reason_counts": last_preflight_reason_counts,
        "last_near_passes": last_near_passes,
        "last_discovery_samples": last_discovery_samples,
        "candidate_cache_ttl_seconds": CANDIDATE_CACHE_TTL_SECONDS,
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
            f"min_bid={MIN_BID}\n"
            f"max_ask={MAX_ASK}\n"
            f"early_min_bid={EARLY_MIN_BID}\n"
            f"early_max_ask={EARLY_MAX_ASK}\n"
            f"early_max_spread={EARLY_MAX_SPREAD}\n"
            f"mid_price_min={MID_PRICE_MIN}\n"
            f"mid_price_max={MID_PRICE_MAX}\n"
            f"block_crypto_ladders={BLOCK_CRYPTO_LADDERS}\n"
            f"book_levels={BOOK_LEVELS}\n"
            f"min_top_depth={MIN_TOP_DEPTH}\n"
            f"min_fill_confidence={MIN_FILL_CONFIDENCE}\n"
            f"micro_alert_score={MICRO_ALERT_SCORE}\n"
            f"micro_watch_score={MICRO_WATCH_SCORE}\n"
            f"preflight_max_spread={PREFLIGHT_MAX_SPREAD}\n"
            f"preflight_min_bid={PREFLIGHT_MIN_BID}\n"
            f"preflight_max_ask={PREFLIGHT_MAX_ASK}\n"
            f"preflight_max_candidates={PREFLIGHT_MAX_CANDIDATES}\n"
            f"auto_discover={AUTO_DISCOVER}\n"
            f"enable_yes_no_only={ENABLE_YES_NO_ONLY}\n"
            f"manual_scan_in_progress={manual_scan_in_progress}\n"
            f"target_min_test_position_usd={TARGET_MIN_TEST_POSITION_USD}\n"
            f"target_max_test_position_usd={TARGET_MAX_TEST_POSITION_USD}\n"
            f"test_market_slug={TEST_MARKET_SLUG or 'none'}\n"
            f"candidate_cache_ttl_seconds={CANDIDATE_CACHE_TTL_SECONDS}\n"
            f"last_pipeline_stats={last_pipeline_stats}\n"
            f"last_preflight_reason_counts={last_preflight_reason_counts}\n"
            f"last_near_passes={last_near_passes}"
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
