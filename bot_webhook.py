import os
import re
import time
import math
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# =========================================================
# Version
# =========================================================
SCRIPT_VERSION = "v12-sports-highflow-discovery-fix"
ROLLING_DISCOVERY_DAYS = 30
UTC = timezone.utc

# =========================================================
# Environment
# =========================================================
GAMMA_BASE = os.getenv("GAMMA_BASE", "https://gamma-api.polymarket.com")
CLOB_BASE = os.getenv("CLOB_BASE", "https://clob.polymarket.com")
MARKETS_URL = os.getenv("MARKETS_URL", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
SCAN_EVERY_SECONDS = int(os.getenv("SCAN_EVERY_SECONDS", "7200"))
ZERO_SUMMARY_EVERY_SECONDS = int(os.getenv("ZERO_SUMMARY_EVERY_SECONDS", "21600"))
DEDUP_SECONDS = int(os.getenv("DEDUP_SECONDS", "3600"))
CANDIDATE_CACHE_TTL_SECONDS = int(os.getenv("CANDIDATE_CACHE_TTL_SECONDS", "90"))

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "-0.10"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "-0.05"))
SEND_WATCH_ALERTS = os.getenv("SEND_WATCH_ALERTS", "false").lower() == "true"
DEBUG_SCAN_OUTPUT = os.getenv("DEBUG_SCAN_OUTPUT", "true").lower() == "true"
SHORT_TERM_ONLY = os.getenv("SHORT_TERM_ONLY", "true").lower() == "true"
MAX_DAYS_TO_END = int(os.getenv("MAX_DAYS_TO_END", "14"))
AUTO_DISCOVER = os.getenv("AUTO_DISCOVER", "true").lower() == "true"
ENABLE_YES_NO_ONLY = os.getenv("ENABLE_YES_NO_ONLY", "true").lower() == "true"
BLOCK_CRYPTO_LADDERS = os.getenv("BLOCK_CRYPTO_LADDERS", "true").lower() == "true"

DISCOVER_LIMIT = int(os.getenv("DISCOVER_LIMIT", "250"))
DISCOVER_PAGE_SIZE = int(os.getenv("DISCOVER_PAGE_SIZE", "100"))
DISCOVER_MIN_VOLUME = float(os.getenv("DISCOVER_MIN_VOLUME", "25000"))
DISCOVER_MIN_LIQUIDITY = float(os.getenv("DISCOVER_MIN_LIQUIDITY", "10000"))
DISCOVER_KEYWORDS = os.getenv(
    "DISCOVER_KEYWORDS",
    "sports,game,match,final,win,series,playoff,tournament,championship,opening day,tonight,tomorrow,this week,hearing,ruling,vote,approval,cpi,ppi,fomc,fed,rates,tariff,shutdown,ftx,payout,sentencing"
)

MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "10000"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.05"))
MIN_BID = float(os.getenv("MIN_BID", "0.01"))
MAX_ASK = float(os.getenv("MAX_ASK", "0.90"))
EARLY_MIN_BID = float(os.getenv("EARLY_MIN_BID", "0.03"))
EARLY_MAX_ASK = float(os.getenv("EARLY_MAX_ASK", "0.97"))
EARLY_MAX_SPREAD = float(os.getenv("EARLY_MAX_SPREAD", "0.20"))
MID_PRICE_MIN = float(os.getenv("MID_PRICE_MIN", "0.05"))
MID_PRICE_MAX = float(os.getenv("MID_PRICE_MAX", "0.95"))
BOOK_LEVELS = int(os.getenv("BOOK_LEVELS", "3"))
MIN_TOP_DEPTH = float(os.getenv("MIN_TOP_DEPTH", "10"))
MIN_FILL_CONFIDENCE = float(os.getenv("MIN_FILL_CONFIDENCE", "0.45"))
MICRO_ALERT_SCORE = float(os.getenv("MICRO_ALERT_SCORE", "0.62"))
MICRO_WATCH_SCORE = float(os.getenv("MICRO_WATCH_SCORE", "0.52"))

PREFLIGHT_MAX_SPREAD = float(os.getenv("PREFLIGHT_MAX_SPREAD", "0.18"))
PREFLIGHT_MIN_BID = float(os.getenv("PREFLIGHT_MIN_BID", "0.005"))
PREFLIGHT_MAX_ASK = float(os.getenv("PREFLIGHT_MAX_ASK", "0.99"))
PREFLIGHT_MAX_CANDIDATES = int(os.getenv("PREFLIGHT_MAX_CANDIDATES", "120"))

TARGET_MIN_TEST_POSITION_USD = float(os.getenv("TARGET_MIN_TEST_POSITION_USD", "1"))
TARGET_MAX_TEST_POSITION_USD = float(os.getenv("TARGET_MAX_TEST_POSITION_USD", "5"))
TEST_MARKET_SLUG = os.getenv("TEST_MARKET_SLUG", "")
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))

# =========================================================
# Runtime state
# =========================================================
state_lock = threading.Lock()
manual_scan_in_progress = False
last_zero_summary_sent_at = 0.0
empty_scan_streak = 0
last_pipeline_stats: Dict[str, Any] = {}
last_preflight_reason_counts: Dict[str, Any] = {"discover": {}, "fallback": {}}
last_near_passes: Dict[str, Any] = {"discover": [], "fallback": []}
last_observation_results: List[Dict[str, Any]] = []
session_summary: Dict[str, Any] = {
    "scans": 0,
    "empty_prod_scans": 0,
    "observation_hits": 0,
    "observation_repeats": 0,
    "unique_forming": 0,
    "best_forming": {},
}
observation_seen: Dict[str, Dict[str, Any]] = {}
alert_dedupe: Dict[str, float] = {}
candidate_cache: Dict[str, Dict[str, Any]] = {}
background_started = False

# =========================================================
# Helpers
# =========================================================
def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def utc_ts() -> float:
    return time.time()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def title_slug_text(market: Dict[str, Any]) -> str:
    return compact_text(f"{market.get('question', '')} {market.get('slug', '')}").lower()


def hours_to_event(end_iso: str) -> Optional[float]:
    if not end_iso:
        return None
    try:
        dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        return (dt - now_utc()).total_seconds() / 3600.0
    except Exception:
        return None


def within_short_term(market: Dict[str, Any]) -> bool:
    if not SHORT_TERM_ONLY:
        return True
    h = hours_to_event(market.get("end_date_iso") or market.get("endDate") or market.get("end_date"))
    if h is None:
        return False
    return h >= 0 and h <= MAX_DAYS_TO_END * 24


def rolling_date_phrases() -> List[str]:
    start = now_utc().date()
    phrases = {"today", "tomorrow", "tonight", "this week"}
    for i in range(0, ROLLING_DISCOVERY_DAYS + 1):
        d = start + timedelta(days=i)
        phrases.add(f"by {d.strftime('%B').lower()} {d.day}")
        phrases.add(f"by {d.strftime('%b').lower()} {d.day}")
        phrases.add(d.strftime("%B %d").lower().replace(" 0", " "))
        phrases.add(d.strftime("%b %d").lower().replace(" 0", " "))
        phrases.add(d.strftime("%A").lower())
    month_end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    phrases.add(f"end of {start.strftime('%B').lower()}")
    if month_end <= start + timedelta(days=ROLLING_DISCOVERY_DAYS):
        phrases.add(f"end of {month_end.strftime('%B').lower()}")
    return sorted(phrases)


def is_yes_no_market(market: Dict[str, Any]) -> bool:
    outcomes = market.get("outcomes")
    if isinstance(outcomes, list):
        cleaned = [compact_text(str(x)).lower() for x in outcomes]
        return cleaned == ["yes", "no"] or cleaned == ["no", "yes"]
    tokens = market.get("tokens") or []
    if len(tokens) == 2:
        labels = [compact_text(str(t.get("outcome", ""))).lower() for t in tokens]
        return sorted(labels) == ["no", "yes"]
    return False


def market_volume(market: Dict[str, Any]) -> float:
    for k in ["volumeNum", "volume", "oneDayVolume", "liquidityClob", "volumeClob"]:
        if k in market:
            return safe_float(market.get(k), 0.0)
    return 0.0


def market_liquidity(market: Dict[str, Any]) -> float:
    for k in ["liquidityNum", "liquidity", "liquidityClob"]:
        if k in market:
            return safe_float(market.get(k), 0.0)
    return 0.0


def event_priority(market: Dict[str, Any]) -> float:
    txt = full_market_text(market)
    score = 1.0
    if is_sports_or_highflow_market(market):
        score += 0.22
    sports_terms = [
        "game", "match", "final", "series", "playoff", "tournament", "championship",
        "world series", "nba", "nfl", "mlb", "nhl", "ncaa", "ufc", "fight", "goal", "touchdown"
    ]
    catalyst_terms = [
        "vote", "hearing", "ruling", "approval", "decision", "debate", "primary", "sentencing",
        "payout", "tariff", "shutdown", "cpi", "ppi", "fomc", "fed", "rates", "jobs report", "bankruptcy"
    ]
    for t in sports_terms:
        if t in txt:
            score += 0.18
    for t in catalyst_terms:
        if t in txt:
            score += 0.14
    for p in rolling_date_phrases():
        if p in txt:
            score += 0.03
            break
    if "ftx" in txt:
        score += 0.06
    return score


def is_ranking_market(market: Dict[str, Any]) -> bool:
    txt = full_market_text(market)
    patterns = [
        "top ai model", "best ai model", "second-best", "second best", "leaderboard", "ranked",
        "have the top", "have the second-best", "have the second best", "best model"
    ]
    return any(p in txt for p in patterns)


def is_crypto_ladder(market: Dict[str, Any]) -> bool:
    txt = full_market_text(market)
    if not BLOCK_CRYPTO_LADDERS:
        return False
    crypto_terms = ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "doge"]
    ladder_terms = ["all time high", "hit $", "by march", "by april", "high)", "low)"]
    return any(c in txt for c in crypto_terms) and any(l in txt for l in ladder_terms)


def is_strike_or_shell(market: Dict[str, Any]) -> bool:
    txt = full_market_text(market)
    patterns = [
        r"hit \(high\)", r"hit \(low\)", r"all time high", r"hit \$\d+", r"price target",
        r"high\) \$", r"low\) \$"
    ]
    return any(re.search(p, txt) for p in patterns)


def is_non_curated(market: Dict[str, Any]) -> bool:
    if is_sports_or_highflow_market(market):
        return False
    if is_curated_catalyst_market(market):
        return False
    txt = full_market_text(market)
    soft_terms = ["today", "tonight", "tomorrow", "this week", "this month", "by ", "before ", "after "]
    return not any(t in txt for t in soft_terms)


def family_bucket(market: Dict[str, Any]) -> str:
    txt = full_market_text(market)
    if any(x in txt for x in ["trump", "hegseth", "zelenskyy", "president", "secretary of defense"]):
        return "politics-personnel"
    if any(x in txt for x in ["tariff", "shutdown", "fed", "fomc", "cpi", "ppi", "rates"]):
        return "macro-policy"
    if "ftx" in txt or "bankruptcy" in txt or "payout" in txt or "sentencing" in txt:
        return "legal-special"
    if any(x in txt for x in ["game", "match", "series", "playoff", "championship", "winner"]):
        return "sports"
    return "other"


def market_family_key(market: Dict[str, Any]) -> str:
    txt = full_market_text(market)
    txt = re.sub(r"\bby\s+[a-z]+\s+\d{1,2}\b", "", txt)
    txt = re.sub(r"\$\d+[kKmM]?", "$X", txt)
    txt = re.sub(r"\d{4}", "YEAR", txt)
    words = [w for w in re.split(r"[^a-z0-9$]+", txt) if w]
    return " ".join(words[:8])


def fetch_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_gamma_markets(limit: int) -> List[Dict[str, Any]]:
    urls = [
        (f"{GAMMA_BASE}/markets", {"limit": limit}),
        (f"{GAMMA_BASE}/markets", {"closed": "false", "limit": limit}),
    ]
    for url, params in urls:
        try:
            data = fetch_json(url, params=params)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                for k in ["markets", "data"]:
                    if isinstance(data.get(k), list):
                        return data[k]
        except Exception:
            continue
    return []


def fetch_manual_slugs() -> List[str]:
    if not MARKETS_URL:
        return []
    try:
        r = requests.get(MARKETS_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        slugs = []
        for line in r.text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                slugs.append(line)
        return slugs
    except Exception:
        return []


def find_market_by_slug(markets: List[Dict[str, Any]], slug: str) -> Optional[Dict[str, Any]]:
    slug = (slug or "").strip().lower()
    for m in markets:
        if (m.get("slug") or "").lower() == slug:
            return m
    return None


def extract_yes_no_tokens(market: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    tokens = market.get("tokens") or []
    yes_token = None
    no_token = None
    for t in tokens:
        outcome = compact_text(str(t.get("outcome", ""))).lower()
        token_id = t.get("token_id") or t.get("tokenId") or t.get("id")
        if outcome == "yes":
            yes_token = str(token_id)
        elif outcome == "no":
            no_token = str(token_id)
    return yes_token, no_token


def normalize_book(book: Dict[str, Any]) -> Dict[str, List[Dict[str, float]]]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    norm = {"bids": [], "asks": []}
    for side_name, raw in [("bids", bids), ("asks", asks)]:
        rows = []
        for row in raw:
            if isinstance(row, dict):
                price = safe_float(row.get("price"))
                size = safe_float(row.get("size") or row.get("quantity"))
            elif isinstance(row, list) and len(row) >= 2:
                price = safe_float(row[0])
                size = safe_float(row[1])
            else:
                continue
            rows.append({"price": price, "size": size})
        rows.sort(key=lambda x: x["price"], reverse=(side_name == "bids"))
        norm[side_name] = rows
    return norm


def fetch_book(token_id: str) -> Optional[Dict[str, Any]]:
    if not token_id:
        return None
    urls = [
        (f"{CLOB_BASE}/book", {"token_id": token_id}),
        (f"{CLOB_BASE}/book", {"tokenId": token_id}),
    ]
    for url, params in urls:
        try:
            data = fetch_json(url, params=params)
            return normalize_book(data if isinstance(data, dict) else {})
        except Exception:
            continue
    return None


def top_levels(book: Optional[Dict[str, Any]], levels: int = BOOK_LEVELS) -> Tuple[float, float, float, float]:
    if not book:
        return 0.0, 0.0, 0.0, 0.0
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    best_bid = safe_float(bids[0]["price"]) if bids else 0.0
    best_ask = safe_float(asks[0]["price"]) if asks else 0.0
    bid_depth = sum(safe_float(x["size"]) for x in bids[:levels])
    ask_depth = sum(safe_float(x["size"]) for x in asks[:levels])
    return best_bid, best_ask, bid_depth, ask_depth


def compute_fill_confidence(best_bid: float, best_ask: float, bid_depth: float, ask_depth: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0
    spread = max(0.0, best_ask - best_bid)
    depth_component = min(1.0, (bid_depth + ask_depth) / max(20.0, TARGET_MAX_TEST_POSITION_USD * 8.0))
    spread_penalty = max(0.0, 1.0 - (spread / max(PREFLIGHT_MAX_SPREAD, 0.0001)))
    return round(max(0.0, min(1.0, 0.6 * depth_component + 0.4 * spread_penalty)), 3)


def dead_extreme_book(best_bid: float, best_ask: float) -> bool:
    return best_bid <= 0.002 and best_ask >= 0.998


def preflight_check(market: Dict[str, Any], book: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    best_bid, best_ask, bid_depth, ask_depth = top_levels(book)
    spread = round(max(0.0, best_ask - best_bid), 6) if best_ask and best_bid else 0.0
    midpoint = round((best_bid + best_ask) / 2.0, 6) if best_bid and best_ask else 0.0
    fill_conf = compute_fill_confidence(best_bid, best_ask, bid_depth, ask_depth)
    metrics = {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "bid_depth": round(bid_depth, 2),
        "ask_depth": round(ask_depth, 2),
        "fill_confidence": fill_conf,
    }
    if best_bid <= 0 and best_ask <= 0:
        return False, "preflight_no_bid", metrics
    if best_bid <= 0:
        return False, "preflight_no_bid", metrics
    if best_ask <= 0:
        return False, "preflight_no_ask", metrics
    if dead_extreme_book(best_bid, best_ask):
        return False, "preflight_dead_extreme_book", metrics
    if best_bid < PREFLIGHT_MIN_BID:
        return False, "preflight_bid_too_low", metrics
    if best_ask > PREFLIGHT_MAX_ASK:
        return False, "preflight_ask_too_high", metrics
    if spread > PREFLIGHT_MAX_SPREAD:
        return False, "preflight_spread_too_wide", metrics
    return True, "ok", metrics


def observation_candidate(metrics: Dict[str, Any], market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bid = metrics["best_bid"]
    ask = metrics["best_ask"]
    spread = metrics["spread"]
    if dead_extreme_book(bid, ask):
        return None
    if bid <= 0 or ask <= 0:
        return None
    if bid < 0.003:
        return None
    if spread > 0.25:
        return None
    if metrics["bid_depth"] + metrics["ask_depth"] < 8:
        return None
    blockers = []
    if bid < MIN_BID:
        blockers.append("bid below production floor")
    if ask > MAX_ASK:
        blockers.append("ask above production cap")
    if spread > MAX_SPREAD:
        blockers.append("spread too wide for production")
    if metrics["fill_confidence"] < MIN_FILL_CONFIDENCE:
        blockers.append("fill confidence too weak")
    if not blockers:
        return None
    reason = blockers[0]
    txt = full_market_text(market)
    catalyst = "event clock"
    if any(x in txt for x in ["game", "match", "playoff", "series", "championship"]):
        catalyst = "sports timing"
    elif any(x in txt for x in ["vote", "hearing", "ruling", "approval", "debate", "primary"]):
        catalyst = "decision window"
    elif any(x in txt for x in ["ftx", "bankruptcy", "payout", "sentencing"]):
        catalyst = "legal milestone"
    score = round(0.45 * min(1.0, bid / max(MIN_BID, 0.0001)) + 0.30 * max(0.0, 1 - spread / 0.25) + 0.25 * metrics["fill_confidence"], 3)
    return {
        "slug": market.get("slug", ""),
        "question": market.get("question", market.get("title", "")),
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "bid_depth": metrics["bid_depth"],
        "ask_depth": metrics["ask_depth"],
        "reason": reason,
        "catalyst": catalyst,
        "score": score,
    }


def production_side_scores(metrics_yes: Dict[str, Any], metrics_no: Dict[str, Any]) -> Tuple[str, float, Dict[str, Any]]:
    def side_score(m: Dict[str, Any]) -> float:
        bid = m["best_bid"]
        ask = m["best_ask"]
        spread = m["spread"]
        depth = m["bid_depth"] + m["ask_depth"]
        fill = m["fill_confidence"]
        if bid < MIN_BID or ask > MAX_ASK or spread > MAX_SPREAD or fill < MIN_FILL_CONFIDENCE or m["midpoint"] < MID_PRICE_MIN or m["midpoint"] > MID_PRICE_MAX:
            return -1.0
        return round(0.35 * fill + 0.25 * min(1.0, depth / 25.0) + 0.20 * max(0.0, 1 - spread / max(MAX_SPREAD, 0.0001)) + 0.20 * min(1.0, bid / max(MIN_BID, 0.0001)), 4)
    yes_score = side_score(metrics_yes)
    no_score = side_score(metrics_no)
    if yes_score >= no_score:
        return "YES", yes_score, metrics_yes
    return "NO", no_score, metrics_no


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=REQUEST_TIMEOUT)
        return r.ok
    except Exception:
        return False


def dedupe_key(prefix: str, slug: str) -> str:
    return f"{prefix}:{slug}"


def allow_send(key: str) -> bool:
    now = utc_ts()
    last = alert_dedupe.get(key, 0.0)
    if now - last < DEDUP_SECONDS:
        return False
    alert_dedupe[key] = now
    return True

# =========================================================
# Discovery
# =========================================================
def discover_candidates() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    stats = {
        "discover_prelim": 0,
        "discover_hard_skipped": 0,
        "discover_hard_skip_reason_counts": {},
        "discover_non_curated_skips": 0,
        "discover_family_skips": 0,
        "discover_bucket_skips": 0,
        "discover_checked": 0,
        "discover_kept": 0,
        "discover_resolve_failures": 0,
        "discover_preflight_failures": 0,
        "discover_preflight_reason_counts": {},
        "discover_near_passes": [],
        "discover_accept_samples": [],
        "discover_skip_samples": [],
        "fallback_items": 0,
        "fallback_checked": 0,
        "fallback_kept": 0,
        "fallback_resolve_failures": 0,
        "fallback_preflight_failures": 0,
        "fallback_preflight_reason_counts": {},
        "fallback_used": False,
        "watchlist_source": "none",
        "scan_resolve_failures": 0,
        "cache_used": 0,
        "cache_refetched": 0,
    }
    markets = fetch_gamma_markets(DISCOVER_LIMIT) if AUTO_DISCOVER else []
    selected: List[Dict[str, Any]] = []
    family_seen = set()
    bucket_counts: Dict[str, int] = {}

    def add_hard_skip(reason: str, market: Dict[str, Any]):
        stats["discover_hard_skipped"] += 1
        stats["discover_hard_skip_reason_counts"][reason] = stats["discover_hard_skip_reason_counts"].get(reason, 0) + 1
        if len(stats["discover_skip_samples"]) < 8:
            stats["discover_skip_samples"].append({"slug": market.get("slug"), "reason": reason, "question": market.get("question")})

    bucket_caps = {"sports": 18, "macro-policy": 8, "legal-special": 8, "politics-personnel": 8, "other": 4}

    for m in markets:
        stats["discover_prelim"] += 1
        sports_like = is_sports_or_highflow_market(m)
        curated_like = is_curated_catalyst_market(m)
        if not within_short_term(m):
            add_hard_skip("hard_skip_outside_window", m)
            continue
        if ENABLE_YES_NO_ONLY and not is_yes_no_market(m):
            add_hard_skip("hard_skip_not_yes_no", m)
            continue
        min_liq = DISCOVER_MIN_LIQUIDITY * (0.5 if sports_like else 1.0)
        min_vol = DISCOVER_MIN_VOLUME * (0.5 if sports_like else 1.0)
        if market_liquidity(m) < min_liq and market_volume(m) < min_vol:
            add_hard_skip("hard_skip_low_liq_vol", m)
            continue
        if is_crypto_ladder(m):
            add_hard_skip("hard_skip_crypto_ladder", m)
            continue
        if is_ranking_market(m):
            add_hard_skip("hard_skip_ranking_market", m)
            continue
        if is_strike_or_shell(m):
            add_hard_skip("hard_skip_strike_ladder", m)
            continue
        if (not sports_like) and (not curated_like) and is_non_curated(m):
            stats["discover_non_curated_skips"] += 1
            if len(stats["discover_skip_samples"]) < 8:
                stats["discover_skip_samples"].append({"slug": m.get("slug"), "reason": "non_curated_skip", "question": m.get("question")})
            continue
        fam = market_family_key(m)
        if fam in family_seen:
            stats["discover_family_skips"] += 1
            continue
        bucket = family_bucket(m)
        cap = bucket_caps.get(bucket, 4)
        if bucket_counts.get(bucket, 0) >= cap:
            stats["discover_bucket_skips"] += 1
            continue
        family_seen.add(fam)
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        m["_priority"] = round(event_priority(m), 3)
        selected.append(m)

    selected.sort(key=lambda x: (x.get("_priority", 1.0), market_liquidity(x), market_volume(x)), reverse=True)
    final_list = selected[:PREFLIGHT_MAX_CANDIDATES]
    for m in final_list[:5]:
        stats["discover_accept_samples"].append({
            "slug": m.get("slug"),
            "question": m.get("question"),
            "priority": m.get("_priority", 1.0),
        })
    return final_list, stats

# =========================================================
# Scan engine
# =========================================================
def scan_once() -> Dict[str, Any]:
    global last_pipeline_stats, last_preflight_reason_counts, last_near_passes, last_observation_results
    candidates, stats = discover_candidates()
    observations: List[Dict[str, Any]] = []
    prod_results: List[Dict[str, Any]] = []
    near_passes: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {}

    for market in candidates:
        stats["discover_checked"] += 1
        slug = market.get("slug", "")
        yes_token, no_token = extract_yes_no_tokens(market)
        if not yes_token or not no_token:
            stats["discover_resolve_failures"] += 1
            continue

        cache_key = slug or market.get("question", "")
        cached = candidate_cache.get(cache_key)
        use_cache = cached and (utc_ts() - cached.get("ts", 0) <= CANDIDATE_CACHE_TTL_SECONDS)
        if use_cache:
            yes_book = cached.get("yes_book")
            no_book = cached.get("no_book")
            stats["cache_used"] += 1
        else:
            yes_book = fetch_book(yes_token)
            no_book = fetch_book(no_token)
            if cached:
                stats["cache_refetched"] += 1
            candidate_cache[cache_key] = {"ts": utc_ts(), "yes_book": yes_book, "no_book": no_book}
        if not yes_book or not no_book:
            stats["discover_resolve_failures"] += 1
            continue

        yes_ok, yes_reason, yes_metrics = preflight_check(market, yes_book)
        no_ok, no_reason, no_metrics = preflight_check(market, no_book)

        # Observation lane before production hard pass, but still clean.
        obs_yes = observation_candidate(yes_metrics, market)
        obs_no = observation_candidate(no_metrics, market)
        if obs_yes:
            obs_yes["side"] = "YES"
            observations.append(obs_yes)
        if obs_no:
            obs_no["side"] = "NO"
            observations.append(obs_no)

        if not yes_ok and not no_ok:
            stats["discover_preflight_failures"] += 1
            # use worst/common reason from yes side first, then no side
            final_reason = yes_reason if yes_reason != "ok" else no_reason
            reason_counts[final_reason] = reason_counts.get(final_reason, 0) + 1
            if len(near_passes) < 5 and final_reason not in ["preflight_dead_extreme_book", "preflight_no_bid", "preflight_no_ask"]:
                m = yes_metrics if yes_reason != "ok" else no_metrics
                near_passes.append({
                    "slug": slug,
                    "question": market.get("question"),
                    "reason": final_reason,
                    "bid": m["best_bid"],
                    "ask": m["best_ask"],
                    "spread": m["spread"],
                    "bid_depth": m["bid_depth"],
                    "ask_depth": m["ask_depth"],
                })
            continue

        side, score, chosen_metrics = production_side_scores(yes_metrics, no_metrics)
        if score < 0:
            # not ready for production; observation may still keep it.
            continue
        category = "ALERT" if score >= MICRO_ALERT_SCORE else ("WATCH" if score >= MICRO_WATCH_SCORE else "SKIP")
        if category == "SKIP":
            continue
        prod_results.append({
            "slug": slug,
            "question": market.get("question"),
            "side": side,
            "score": score,
            "bid": chosen_metrics["best_bid"],
            "ask": chosen_metrics["best_ask"],
            "spread": chosen_metrics["spread"],
            "fill_confidence": chosen_metrics["fill_confidence"],
            "category": category,
        })
        stats["discover_kept"] += 1

    prod_results.sort(key=lambda x: x["score"], reverse=True)
    observations.sort(key=lambda x: x["score"], reverse=True)

    # Deduplicate observation entries by slug, keeping best score.
    obs_map: Dict[str, Dict[str, Any]] = {}
    for o in observations:
        key = o["slug"] or o["question"]
        if key not in obs_map or o["score"] > obs_map[key]["score"]:
            prev = observation_seen.get(key)
            trend = "new"
            if prev:
                bid_diff = round(o["bid"] - prev.get("bid", 0.0), 4)
                spread_diff = round(prev.get("spread", 0.0) - o["spread"], 4)
                if bid_diff > 0 or spread_diff > 0:
                    trend = "improving"
                else:
                    trend = "flat"
                session_summary["observation_repeats"] += 1
            o["trend"] = trend
            obs_map[key] = o
            observation_seen[key] = {"bid": o["bid"], "spread": o["spread"], "ts": utc_ts()}

    final_observations = list(obs_map.values())[:5]
    if final_observations:
        session_summary["observation_hits"] += len(final_observations)
        session_summary["unique_forming"] = len(observation_seen)
        best = final_observations[0]
        if not session_summary["best_forming"] or best["score"] > session_summary["best_forming"].get("score", -1):
            session_summary["best_forming"] = best

    last_pipeline_stats = stats.copy()
    last_pipeline_stats["discover_preflight_reason_counts"] = reason_counts.copy()
    last_preflight_reason_counts = {"discover": reason_counts.copy(), "fallback": {}}
    last_near_passes = {"discover": near_passes[:5], "fallback": []}
    last_observation_results = final_observations

    return {
        "timestamp": now_utc().isoformat(),
        "pipeline": last_pipeline_stats,
        "alerts": [x for x in prod_results if x["category"] == "ALERT"][:MAX_ALERTS_PER_SCAN],
        "watches": [x for x in prod_results if x["category"] == "WATCH"][:MAX_ALERTS_PER_SCAN],
        "observations": final_observations,
        "near_passes": near_passes[:5],
        "reason_counts": reason_counts,
    }

# =========================================================
# Formatting
# =========================================================
def format_health_text() -> str:
    lines = [
        "Health check",
        f"script_version={SCRIPT_VERSION}",
        f"rolling_discovery_days={ROLLING_DISCOVERY_DAYS}",
        f"alert_threshold={ALERT_THRESHOLD}",
        f"watch_threshold={WATCH_THRESHOLD}",
        f"send_watch_alerts={SEND_WATCH_ALERTS}",
        f"debug_scan_output={DEBUG_SCAN_OUTPUT}",
        f"scan_every_seconds={SCAN_EVERY_SECONDS}",
        f"short_term_only={SHORT_TERM_ONLY}",
        f"max_days_to_end={MAX_DAYS_TO_END}",
        f"discover_limit={DISCOVER_LIMIT}",
        f"discover_min_volume={DISCOVER_MIN_VOLUME}",
        f"discover_min_liquidity={DISCOVER_MIN_LIQUIDITY}",
        f"min_liquidity={MIN_LIQUIDITY}",
        f"max_spread={MAX_SPREAD}",
        f"min_bid={MIN_BID}",
        f"max_ask={MAX_ASK}",
        f"early_min_bid={EARLY_MIN_BID}",
        f"early_max_ask={EARLY_MAX_ASK}",
        f"early_max_spread={EARLY_MAX_SPREAD}",
        f"mid_price_min={MID_PRICE_MIN}",
        f"mid_price_max={MID_PRICE_MAX}",
        f"block_crypto_ladders={BLOCK_CRYPTO_LADDERS}",
        f"book_levels={BOOK_LEVELS}",
        f"min_top_depth={MIN_TOP_DEPTH}",
        f"min_fill_confidence={MIN_FILL_CONFIDENCE}",
        f"micro_alert_score={MICRO_ALERT_SCORE}",
        f"micro_watch_score={MICRO_WATCH_SCORE}",
        f"preflight_max_spread={PREFLIGHT_MAX_SPREAD}",
        f"preflight_min_bid={PREFLIGHT_MIN_BID}",
        f"preflight_max_ask={PREFLIGHT_MAX_ASK}",
        f"preflight_max_candidates={PREFLIGHT_MAX_CANDIDATES}",
        f"auto_discover={AUTO_DISCOVER}",
        f"enable_yes_no_only={ENABLE_YES_NO_ONLY}",
        f"manual_scan_in_progress={manual_scan_in_progress}",
        f"target_min_test_position_usd={TARGET_MIN_TEST_POSITION_USD}",
        f"target_max_test_position_usd={TARGET_MAX_TEST_POSITION_USD}",
        f"test_market_slug={TEST_MARKET_SLUG or 'none'}",
        f"candidate_cache_ttl_seconds={CANDIDATE_CACHE_TTL_SECONDS}",
        f"last_pipeline_stats={last_pipeline_stats}",
        f"last_preflight_reason_counts={last_preflight_reason_counts}",
        f"last_near_passes={last_near_passes}",
        f"last_observation_results={last_observation_results}",
        f"session_summary={session_summary}",
    ]
    return "\n".join(lines)


def format_scan_text(scan: Dict[str, Any]) -> str:
    pipeline = scan["pipeline"]
    alerts = scan["alerts"]
    watches = scan["watches"]
    observations = scan["observations"]
    reason_counts = scan["reason_counts"]
    near_passes = scan["near_passes"]

    top_reason = "none"
    if reason_counts:
        k, v = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[0]
        top_reason = f"{k}:{v}"

    lines = [
        "Scan finished. No qualifying markets." if not alerts and not watches else "Scan finished.",
        "",
        f"Total: {len(alerts) + len(watches)}",
        f"Alerts: {len(alerts)}",
        f"Watch: {len(watches)}",
        "Skip: 0",
        (
            "Pipeline: source=none | "
            f"discover_prelim={pipeline.get('discover_prelim', 0)} | "
            f"discover_hard_skipped={pipeline.get('discover_hard_skipped', 0)} | "
            f"discover_non_curated_skips={pipeline.get('discover_non_curated_skips', 0)} | "
            f"discover_family_skips={pipeline.get('discover_family_skips', 0)} | "
            f"discover_bucket_skips={pipeline.get('discover_bucket_skips', 0)} | "
            f"discover_checked={pipeline.get('discover_checked', 0)} | "
            f"discover_kept={pipeline.get('discover_kept', 0)} | "
            f"fallback_items={pipeline.get('fallback_items', 0)} | "
            f"fallback_checked={pipeline.get('fallback_checked', 0)} | "
            f"fallback_kept={pipeline.get('fallback_kept', 0)} | "
            f"cache_used={pipeline.get('cache_used', 0)} | "
            f"cache_refetched={pipeline.get('cache_refetched', 0)} | "
            f"resolve_failures={pipeline.get('discover_resolve_failures', 0)} | "
            f"top_preflight_discover={top_reason} | top_preflight_fallback=none"
        ),
        "",
        "Near-miss summary: No skip reasons available." if not near_passes else "Near-miss summary:",
    ]
    if near_passes:
        for n in near_passes:
            lines.extend([
                f"{n['question']}",
                f"slug={n['slug']}",
                f"reason={n['reason']}",
                f"bid={n['bid']} ask={n['ask']} spread={n['spread']}",
            ])
    else:
        lines.extend([
            "",
            "No tradable near-misses found.",
            "Most candidates failed preflight, depth, fill confidence, or cached candidate resolution.",
            f"Preflight detail: discover={reason_counts} fallback={{}}",
        ])

    if alerts or watches:
        lines.append("")
        lines.append("Tradable now:")
        for row in alerts + watches:
            lines.extend([
                f"{row['category']} | {row['question']}",
                f"slug={row['slug']} side={row['side']} score={row['score']}",
                f"bid={row['bid']} ask={row['ask']} spread={row['spread']} fill={row['fill_confidence']}",
                "",
            ])

    lines.append("")
    if observations:
        lines.append("Forming markets:")
        for o in observations:
            lines.extend([
                f"FORMING | {o['question']}",
                f"slug={o['slug']} side={o['side']} score={o['score']} trend={o['trend']}",
                f"bid={o['bid']} ask={o['ask']} spread={o['spread']}",
                f"bid_depth={o['bid_depth']} ask_depth={o['ask_depth']}",
                f"interesting={o['catalyst']} | not_ready={o['reason']}",
                "",
            ])
    else:
        lines.append("Forming markets: none")

    lines.extend([
        "",
        "Session summary:",
        f"scans={session_summary.get('scans', 0)} | empty_prod_scans={session_summary.get('empty_prod_scans', 0)} | unique_forming={session_summary.get('unique_forming', 0)}",
        f"observation_hits={session_summary.get('observation_hits', 0)} | observation_repeats={session_summary.get('observation_repeats', 0)}",
        f"top_skip_reason={top_reason if top_reason else 'none'}",
        f"best_forming={session_summary.get('best_forming', {}) or 'none'}",
    ])
    return "\n".join(lines)


def format_zero_summary() -> str:
    reason_counts = last_preflight_reason_counts.get("discover", {}) or {}
    top_reason = "none"
    if reason_counts:
        k, v = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[0]
        top_reason = f"{k}:{v}"
    best_forming = session_summary.get("best_forming") or {}
    best_forming_label = best_forming.get("question") if isinstance(best_forming, dict) else None
    return "\n".join([
        f"No qualifying markets in last {int(ZERO_SUMMARY_EVERY_SECONDS/60) + 2} minutes.",
        f"Empty scans: {empty_scan_streak}",
        f"Top preflight block: {top_reason}",
        f"Hard-skipped in discovery: {last_pipeline_stats.get('discover_hard_skipped', 0)} | family skips: {last_pipeline_stats.get('discover_family_skips', 0)} | bucket skips: {last_pipeline_stats.get('discover_bucket_skips', 0)}",
        "Closest near-pass: none" if not last_near_passes.get("discover") else f"Closest near-pass: {last_near_passes['discover'][0].get('question')}",
        f"Top forming market: {best_forming_label or 'none'}",
    ])

# =========================================================
# Execution wrappers
# =========================================================
def run_scan_and_update() -> Dict[str, Any]:
    global empty_scan_streak
    scan = scan_once()
    session_summary["scans"] += 1
    if not scan["alerts"] and not scan["watches"]:
        empty_scan_streak += 1
        session_summary["empty_prod_scans"] += 1
    else:
        empty_scan_streak = 0
    return scan


def handle_command(text: str) -> str:
    global manual_scan_in_progress
    cmd = (text or "").strip().lower()
    if cmd.startswith("/health"):
        return format_health_text()
    if cmd.startswith("/scan"):
        with state_lock:
            manual_scan_in_progress = True
        try:
            scan = run_scan_and_update()
            return format_scan_text(scan)
        finally:
            with state_lock:
                manual_scan_in_progress = False
    return "Commands: /health, /scan"


def background_loop() -> None:
    global last_zero_summary_sent_at
    while True:
        time.sleep(max(30, SCAN_EVERY_SECONDS))
        try:
            scan = run_scan_and_update()
            if scan["alerts"]:
                text = format_scan_text(scan)
                send_telegram(text)
            elif SEND_WATCH_ALERTS and scan["watches"]:
                text = format_scan_text(scan)
                send_telegram(text)
            else:
                now = utc_ts()
                if now - last_zero_summary_sent_at >= ZERO_SUMMARY_EVERY_SECONDS:
                    send_telegram(format_zero_summary())
                    last_zero_summary_sent_at = now
        except Exception as exc:
            send_telegram(f"Scan error: {exc}")


def ensure_background_started() -> None:
    global background_started
    if background_started:
        return
    background_started = True
    threading.Thread(target=background_loop, daemon=True).start()

# =========================================================
# Flask routes
# =========================================================
@app.route("/health", methods=["GET"])
def health_route():
    return format_health_text(), 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/scan", methods=["GET"])
def scan_route():
    scan = run_scan_and_update()
    return format_scan_text(scan), 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/webhook", methods=["POST"])
def webhook_route():
    payload = request.get_json(silent=True) or {}
    msg = payload.get("message") or payload.get("edited_message") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = msg.get("text", "")
    reply = handle_command(text)
    if TELEGRAM_BOT_TOKEN and chat_id:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": reply},
                timeout=REQUEST_TIMEOUT,
            )
        except Exception:
            pass
    return jsonify({"ok": True})


@app.route("/", methods=["GET"])
def root_route():
    return jsonify({"ok": True, "script_version": SCRIPT_VERSION})


ensure_background_started()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
