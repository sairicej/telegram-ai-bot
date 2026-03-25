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
SCRIPT_VERSION = "v17.1-near-endtime-timing-pass"
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
unresolved_slug_failures: Dict[str, int] = {}
unresolved_family_failures: Dict[str, int] = {}
background_started = False

# =========================================================
# Helpers
# =========================================================
def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def market_slug(market: Dict[str, Any]) -> str:
    return str(market.get("slug") or "")


def market_question(market: Dict[str, Any]) -> str:
    return str(market.get("question") or market.get("title") or "")


def is_open_status_market(market: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Small discovery guard so obviously closed/resolved markets do not rely on time parsing.
    This keeps the pipeline focused on live, short-term, executable setups.
    """
    truthy_false = {"false", "0", "no", "closed", "inactive", "resolved", "ended", "finalized", "settled"}
    truthy_true = {"true", "1", "yes", "open", "active", "live"}

    def norm(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        if v is None:
            return ""
        return str(v).strip().lower()

    for key in ["closed", "isClosed", "archived", "isArchived", "resolved", "isResolved", "ended", "isEnded", "settled", "finalized"]:
        if norm(market.get(key)) in truthy_true:
            return False, f"status:{key}"

    active_seen = False
    for key in ["active", "isActive", "enableOrderBook", "orderBookEnabled"]:
        val = norm(market.get(key))
        if val:
            active_seen = True
            if val in truthy_false:
                return False, f"status:{key}"

    status_val = norm(market.get("status"))
    if status_val in truthy_false:
        return False, "status:status"
    if status_val and status_val not in truthy_true and "open" not in status_val and "active" not in status_val and "live" not in status_val:
        return False, f"status:{status_val}"

    market_status = norm(market.get("marketStatus"))
    if market_status in truthy_false:
        return False, "status:marketStatus"
    if market_status and market_status not in truthy_true and "open" not in market_status and "active" not in market_status and "live" not in market_status:
        return False, f"status:{market_status}"

    return True, "ok"


def get_market_end_dt(market: Dict[str, Any]) -> Tuple[Optional[datetime], str]:
    """
    Use only true end/close fields for discovery window checks.
    Do not treat start fields as expiry. That was causing active markets
    to be misclassified as already over.
    Returns (datetime_or_none, source_field_name).
    """
    candidates = [
        ("endDate", market.get("endDate")),
        ("end_date", market.get("end_date")),
        ("end_date_iso", market.get("end_date_iso")),
        ("endTime", market.get("endTime")),
        ("end_time", market.get("end_time")),
        ("closeTime", market.get("closeTime")),
        ("close_time", market.get("close_time")),
        ("expirationDate", market.get("expirationDate")),
        ("expiration_date", market.get("expiration_date")),
        ("expiry", market.get("expiry")),
        ("expiresAt", market.get("expiresAt")),
    ]

    event_obj = market.get("event") or {}
    if isinstance(event_obj, dict):
        candidates.extend([
            ("event.endDate", event_obj.get("endDate")),
            ("event.end_date", event_obj.get("end_date")),
            ("event.endTime", event_obj.get("endTime")),
            ("event.closeTime", event_obj.get("closeTime")),
            ("event.expirationDate", event_obj.get("expirationDate")),
        ])

    events = market.get("events") or []
    if isinstance(events, list):
        for idx, event_obj in enumerate(events[:3]):
            if isinstance(event_obj, dict):
                candidates.extend([
                    (f"events[{idx}].endDate", event_obj.get("endDate")),
                    (f"events[{idx}].end_date", event_obj.get("end_date")),
                    (f"events[{idx}].endTime", event_obj.get("endTime")),
                    (f"events[{idx}].closeTime", event_obj.get("closeTime")),
                    (f"events[{idx}].expirationDate", event_obj.get("expirationDate")),
                ])

    for key, value in candidates:
        dt = parse_iso_datetime(value)
        if dt is not None:
            return dt, key
    return None, "missing"


def classify_time_window(market: Dict[str, Any]) -> Tuple[bool, str, Optional[datetime], str]:
    end_dt, source = get_market_end_dt(market)

    # If we can't find time, DO NOT kill it
    if end_dt is None:
        return True, "soft_missing_time", None, source

    now_dt = datetime.now(timezone.utc)
    delta_days = (end_dt - now_dt).total_seconds() / 86400.0

    # Only reject if clearly past
    if delta_days < 0:
        return False, "hard_skip_past_event", end_dt, source

    # If outside window → allow but tag it
    if delta_days > MAX_DAYS_TO_END:
        return True, "soft_outside_window", end_dt, source

    return True, "ok", end_dt, source


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

def full_market_text(market: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in [
        "question", "slug", "description", "category", "subcategory", "groupItemTitle",
        "title", "eventTitle", "eventSlug", "seriesSlug", "resolutionSource", "rules"
    ]:
        v = market.get(key)
        if v:
            parts.append(str(v))
    tags = market.get("tags") or market.get("tag") or []
    if isinstance(tags, list):
        for t in tags:
            if isinstance(t, dict):
                parts.extend(str(t.get(k, "")) for k in ["label", "name", "slug"] if t.get(k))
            elif t:
                parts.append(str(t))
    events = market.get("events") or []
    if isinstance(events, list):
        for e in events:
            if isinstance(e, dict):
                parts.extend(str(e.get(k, "")) for k in ["title", "slug", "category"] if e.get(k))
    return compact_text(" ".join(parts)).lower()


def is_sports_or_highflow_market(market: Dict[str, Any]) -> bool:
    txt = full_market_text(market)
    sports_terms = [
        "sports", "game", "match", "vs", " v ", "final", "series", "playoff", "tournament",
        "championship", "world cup", "world series", "opening day", "mlb", "nba", "nfl", "nhl",
        "ncaa", "soccer", "football", "baseball", "basketball", "hockey", "tennis", "golf",
        "ufc", "mma", "boxing", "race", "grand prix", "wimbledon", "masters"
    ]
    return any(term in txt for term in sports_terms)


def is_curated_catalyst_market(market: Dict[str, Any]) -> bool:
    txt = full_market_text(market)
    catalyst_terms = [
        "vote", "voting", "hearing", "ruling", "approval", "decision", "debate", "primary",
        "caucus", "election", "tariff", "shutdown", "fed", "fomc", "cpi", "ppi", "rates",
        "jobs report", "payrolls", "bankruptcy", "ftx", "payout", "sentencing", "settlement",
        "delist", "delisted", "trial", "verdict", "sec", "etf", "earnings"
    ]
    if any(term in txt for term in catalyst_terms):
        return True
    return any(p in txt for p in rolling_date_phrases())


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


def has_strong_time_signal(market: Dict[str, Any]) -> bool:
    txt = full_market_text(market)
    strong_terms = ["today", "tonight", "tomorrow", "this week", "by friday", "by saturday", "by sunday"]
    if any(t in txt for t in strong_terms):
        return True
    for p in rolling_date_phrases():
        if p in txt:
            return True
    return False


def catalyst_signal_score(market: Dict[str, Any]) -> float:
    txt = full_market_text(market)
    strong = [
        "cpi", "ppi", "fomc", "fed", "rates", "vote", "hearing", "ruling",
        "approval", "decision", "deadline", "primary", "debate", "etf",
        "ban", "legal", "sentencing", "settlement", "shutdown", "tariff"
    ]
    medium = [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "jobs report",
        "payrolls", "treasury", "yield", "opec"
    ]
    weak_penalty = [
        "championship", "nba finals", "stanley cup", "world series",
        "super bowl", "will the ", "gta vi", "novelty", "winner"
    ]
    score = 0.0
    for t in strong:
        if t in txt:
            score += 1.0
    for t in medium:
        if t in txt:
            score += 0.45
    for t in weak_penalty:
        if t in txt:
            score -= 0.8
    if has_strong_time_signal(market):
        score += 1.2
    return round(score, 3)


def is_bad_microstructure_family(market: Dict[str, Any]) -> Tuple[bool, str]:
    txt = full_market_text(market)
    if any(x in txt for x in ["nba finals", "stanley cup", "world series", "super bowl", "championship"]) and "will the " in txt:
        return True, "sports_futures"
    if any(x in txt for x in ["gta vi", "novelty", "before gta", "celebrity death", "ufo", "alien"]):
        return True, "novelty_special"
    return False, "ok"


def event_proximity_priority(market: Dict[str, Any]) -> float:
    end_dt, _ = get_market_end_dt(market)
    if end_dt is None:
        return 0.0
    delta_hours = (end_dt - now_utc()).total_seconds() / 3600.0
    if delta_hours < 0:
        return -5.0
    if delta_hours <= 24:
        return 1.25
    if delta_hours <= 72:
        return 1.0
    if delta_hours <= 7 * 24:
        return 0.65
    if delta_hours <= 14 * 24:
        return 0.25
    return -0.5


def has_near_end_time(market: Dict[str, Any]) -> bool:
    end_dt, _ = get_market_end_dt(market)
    if end_dt is None:
        return False
    delta_hours = (end_dt - now_utc()).total_seconds() / 3600.0
    if delta_hours < 0:
        return False
    if delta_hours <= 72:
        return True
    if delta_hours <= 7 * 24 and catalyst_signal_score(market) >= 1.5:
        return True
    return False


def discovery_intake_score(market: Dict[str, Any]) -> float:
    liq = market_liquidity(market)
    vol = market_volume(market)
    base = event_priority(market)
    catalyst = catalyst_signal_score(market)
    proximity = event_proximity_priority(market)
    flow = min(1.5, (liq / max(DISCOVER_MIN_LIQUIDITY, 1.0)) * 0.35 + (vol / max(DISCOVER_MIN_VOLUME, 1.0)) * 0.25)
    return round(base + catalyst + proximity + flow, 3)


def is_yes_no_market(market: Dict[str, Any]) -> bool:
    """
    Keep YES/NO only, but be less brittle about how Gamma formats it.
    Accept:
    - outcomes as a real list
    - outcomes as a JSON/string list like '["Yes","No"]'
    - two tokens where labels appear in outcome/name/title/label
    """
    def clean_label(v: Any) -> str:
        return compact_text(str(v or "")).lower()

    def is_yes_no_pair(labels: List[str]) -> bool:
        cleaned = [clean_label(x) for x in labels if clean_label(x)]
        if len(cleaned) != 2:
            return False
        return sorted(cleaned) == ["no", "yes"]

    outcomes = market.get("outcomes")

    if isinstance(outcomes, list):
        if is_yes_no_pair(outcomes):
            return True

    if isinstance(outcomes, str):
        s = outcomes.strip()
        if s:
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list) and is_yes_no_pair(parsed):
                    return True
            except Exception:
                pass

            # fallback for loose comma-separated strings like "Yes,No"
            parts = [p.strip().strip('"\'') for p in s.strip("[]").split(",") if p.strip()]
            if is_yes_no_pair(parts):
                return True

    tokens = market.get("tokens") or []
    if isinstance(tokens, list) and len(tokens) == 2:
        labels = []
        for t in tokens:
            if isinstance(t, dict):
                for key in ["outcome", "name", "title", "label"]:
                    if t.get(key):
                        labels.append(t.get(key))
                        break
            elif t:
                labels.append(t)
        if is_yes_no_pair(labels):
            return True

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
        (f"{GAMMA_BASE}/markets", {"active": "true", "closed": "false", "archived": "false", "limit": limit}),
        (f"{GAMMA_BASE}/markets", {"closed": "false", "archived": "false", "limit": limit}),
        (f"{GAMMA_BASE}/markets", {"limit": limit}),
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


def market_family_hint(market: Dict[str, Any]) -> str:
    txt = full_market_text(market)
    if any(x in txt for x in ["nba finals", "nfl", "mlb", "nhl", "world series", "championship", "will the "]):
        return "sports-futures"
    if any(x in txt for x in ["gta vi", "novelty", "special", "before "]):
        return "novelty-special"
    return family_bucket(market)


def should_cooldown_slug(market: Dict[str, Any]) -> Tuple[bool, str]:
    slug = str(market.get("slug") or "")
    if not slug:
        return False, "no_slug"
    slug_failures = unresolved_slug_failures.get(slug, 0)
    fam = market_family_hint(market)
    fam_failures = unresolved_family_failures.get(fam, 0)
    if slug_failures >= 3:
        return True, "slug_repeat_unresolved"
    if fam_failures >= 10 and "sports-futures" in fam:
        return True, "family_repeat_unresolved"
    return False, "ok"


def record_unresolved_market_failure(market: Dict[str, Any]) -> None:
    slug = str(market.get("slug") or "")
    if slug:
        unresolved_slug_failures[slug] = unresolved_slug_failures.get(slug, 0) + 1
    fam = market_family_hint(market)
    unresolved_family_failures[fam] = unresolved_family_failures.get(fam, 0) + 1


def record_market_resolved_success(market: Dict[str, Any]) -> None:
    slug = str(market.get("slug") or "")
    if slug in unresolved_slug_failures:
        unresolved_slug_failures[slug] = max(0, unresolved_slug_failures.get(slug, 0) - 1)
        if unresolved_slug_failures[slug] == 0:
            unresolved_slug_failures.pop(slug, None)


def deep_find_yes_no_tokens(obj: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Look through messy nested payloads for YES/NO labels paired with token ids.
    This stays inside current architecture. It just hardens resolution.
    """
    yes_token = None
    no_token = None

    def clean(v: Any) -> str:
        return compact_text(str(v or "")).lower()

    def extract_token_id_from_dict(d: Dict[str, Any]) -> Optional[str]:
        for key in [
            "token_id", "tokenId", "id", "asset_id", "assetId",
            "clobTokenId", "clob_token_id", "outcomeTokenId", "outcome_token_id",
            "token", "asset"
        ]:
            val = d.get(key)
            if isinstance(val, dict):
                nested = extract_token_id_from_dict(val)
                if nested:
                    return nested
            elif val not in (None, "", [], {}):
                return str(val)
        return None

    def extract_label_from_dict(d: Dict[str, Any]) -> str:
        for key in ["outcome", "name", "title", "label", "side"]:
            val = d.get(key)
            if val not in (None, "", [], {}):
                return clean(val)
        return ""

    def walk(node: Any):
        nonlocal yes_token, no_token
        if yes_token and no_token:
            return
        if isinstance(node, dict):
            label = extract_label_from_dict(node)
            token_id = extract_token_id_from_dict(node)
            if label == "yes" and token_id and not yes_token:
                yes_token = token_id
            elif label == "no" and token_id and not no_token:
                no_token = token_id
            for v in node.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    walk(item)

    walk(obj)
    if not yes_token or not no_token:
        deep_yes, deep_no = deep_find_yes_no_tokens(market)
        yes_token = yes_token or deep_yes
        no_token = no_token or deep_no

    return yes_token, no_token


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


def merge_market_records(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base or {})
    for k, v in (extra or {}).items():
        if v is None or v == "":
            continue
        if k == "tokens" and isinstance(v, list) and v:
            merged[k] = v
            continue
        if k in ["outcomes", "clobTokenIds", "clob_token_ids", "tokenIds", "token_ids"] and v:
            merged[k] = v
            continue
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            inner = dict(merged.get(k) or {})
            inner.update(v)
            merged[k] = inner
            continue
        if isinstance(v, list) and isinstance(merged.get(k), list) and not merged.get(k):
            merged[k] = v
            continue
        if k not in merged or merged.get(k) in (None, "", [], {}):
            merged[k] = v
    return merged


def fetch_market_detail(market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    slug = str(market.get("slug") or "").strip()
    market_id = market.get("id") or market.get("market_id") or market.get("marketId")
    urls = []

    if slug:
        urls.extend([
            (f"{GAMMA_BASE}/markets", {"slug": slug}),
            (f"{GAMMA_BASE}/markets", {"slug": slug, "limit": 1}),
            (f"{GAMMA_BASE}/markets/{slug}", None),
            (f"{GAMMA_BASE}/markets/slug/{slug}", None),
            (f"{GAMMA_BASE}/events", {"slug": slug}),
        ])
    if market_id not in (None, ""):
        market_id = str(market_id)
        urls.extend([
            (f"{GAMMA_BASE}/markets/{market_id}", None),
            (f"{GAMMA_BASE}/markets", {"id": market_id}),
            (f"{GAMMA_BASE}/markets", {"market_id": market_id}),
        ])

    for url, params in urls:
        try:
            data = fetch_json(url, params=params)
            if isinstance(data, list) and data:
                first = data[0]
                if isinstance(first, dict):
                    return first
            if isinstance(data, dict):
                for key in ["market", "data"]:
                    if isinstance(data.get(key), dict):
                        return data[key]
                for key in ["markets", "events"]:
                    val = data.get(key)
                    if isinstance(val, list) and val and isinstance(val[0], dict):
                        return val[0]
                return data
        except Exception:
            continue
    return None


def hydrate_market_for_tokens(market: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Discovery list payloads can be thin. Only skip hydration if inline extraction
    already resolves BOTH YES and NO token ids. Otherwise force detail fetch.
    """
    inline_yes, inline_no = extract_yes_no_tokens(market)
    if inline_yes and inline_no:
        return market, "inline"

    detail = fetch_market_detail(market)
    if not detail:
        return market, "detail_missing"

    merged = merge_market_records(market, detail)
    merged["_hydrated"] = True
    return merged, "detail_ok"

def find_market_by_slug(markets: List[Dict[str, Any]], slug: str) -> Optional[Dict[str, Any]]:
    slug = (slug or "").strip().lower()
    for m in markets:
        if (m.get("slug") or "").lower() == slug:
            return m
    return None


def extract_yes_no_tokens(market: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve YES/NO token ids from messy Gamma payloads without widening beyond YES/NO markets.
    Tries multiple label fields and multiple token-id fields.
    """
    def clean_label(v: Any) -> str:
        return compact_text(str(v or "")).lower()

    def extract_token_id_from_dict(d: Dict[str, Any]) -> Optional[str]:
        for key in [
            "token_id", "tokenId", "id", "asset_id", "assetId",
            "clobTokenId", "clob_token_id", "outcomeTokenId", "outcome_token_id"
        ]:
            val = d.get(key)
            if val not in (None, ""):
                return str(val)
        # Some payloads keep ids in nested token-ish objects
        for nested_key in ["token", "asset", "clobToken", "clob_token"]:
            nested = d.get(nested_key)
            if isinstance(nested, dict):
                nested_id = extract_token_id_from_dict(nested)
                if nested_id:
                    return nested_id
        return None

    def extract_label_from_dict(d: Dict[str, Any]) -> str:
        for key in ["outcome", "name", "title", "label", "side"]:
            val = d.get(key)
            if val not in (None, ""):
                return clean_label(val)
        return ""

    yes_token = None
    no_token = None

    tokens = market.get("tokens") or []
    if isinstance(tokens, list):
        for t in tokens:
            if isinstance(t, dict):
                label = extract_label_from_dict(t)
                token_id = extract_token_id_from_dict(t)
                if label == "yes" and token_id:
                    yes_token = token_id
                elif label == "no" and token_id:
                    no_token = token_id

    # Fallback: some markets expose parallel clob token id arrays
    if (not yes_token or not no_token):
        outcomes = market.get("outcomes")
        clob_ids = (
            market.get("clobTokenIds")
            or market.get("clob_token_ids")
            or market.get("tokenIds")
            or market.get("token_ids")
        )
        if isinstance(outcomes, str):
            try:
                import json
                parsed = json.loads(outcomes)
                if isinstance(parsed, list):
                    outcomes = parsed
            except Exception:
                outcomes = [p.strip().strip('"\'') for p in outcomes.strip("[]").split(",") if p.strip()]
        if isinstance(outcomes, list) and isinstance(clob_ids, list) and len(outcomes) == 2 and len(clob_ids) == 2:
            labels = [clean_label(x) for x in outcomes]
            ids = [str(x) for x in clob_ids]
            for label, token_id in zip(labels, ids):
                if label == "yes":
                    yes_token = yes_token or token_id
                elif label == "no":
                    no_token = no_token or token_id

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


def compute_book_imbalance(bid_depth: float, ask_depth: float) -> float:
    total = max(0.0001, bid_depth + ask_depth)
    return round((bid_depth - ask_depth) / total, 4)


def compute_pressure_score(best_bid: float, best_ask: float, bid_depth: float, ask_depth: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0
    spread = max(0.0001, best_ask - best_bid)
    imbalance = compute_book_imbalance(bid_depth, ask_depth)
    tightness = max(0.0, 1.0 - (spread / max(PREFLIGHT_MAX_SPREAD, 0.0001)))
    raw = 0.55 * max(0.0, imbalance) + 0.45 * tightness
    return round(max(0.0, min(1.0, raw)), 4)


def classify_trade_failure(metrics: Dict[str, Any]) -> str:
    if metrics["best_bid"] < MIN_BID:
        return "bid below production floor"
    if metrics["best_ask"] > MAX_ASK:
        return "ask above production cap"
    if metrics["spread"] > MAX_SPREAD:
        return "spread too wide for production"
    if metrics["fill_confidence"] < MIN_FILL_CONFIDENCE:
        return "fill confidence too weak"
    if metrics["midpoint"] < MID_PRICE_MIN or metrics["midpoint"] > MID_PRICE_MAX:
        return "midpoint outside production band"
    return "not ready yet"


def is_near_pass_metrics(metrics: Dict[str, Any]) -> bool:
    if metrics["best_bid"] <= 0 or metrics["best_ask"] <= 0:
        return False
    if dead_extreme_book(metrics["best_bid"], metrics["best_ask"]):
        return False
    if metrics["spread"] > max(0.25, MAX_SPREAD * 3.5):
        return False
    spread_close = metrics["spread"] <= max(MAX_SPREAD * 1.5, 0.08)
    fill_close = metrics["fill_confidence"] >= max(0.30, MIN_FILL_CONFIDENCE - 0.15)
    bid_close = metrics["best_bid"] >= max(0.005, MIN_BID * 0.65)
    return spread_close and fill_close and bid_close


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
        "imbalance": compute_book_imbalance(bid_depth, ask_depth),
        "pressure": compute_pressure_score(best_bid, best_ask, bid_depth, ask_depth),
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


def observation_candidate(metrics: Dict[str, Any], market: Dict[str, Any], lane: str = "forming") -> Optional[Dict[str, Any]]:
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
    reason = classify_trade_failure(metrics)
    txt = full_market_text(market)
    catalyst = "event clock"
    if any(x in txt for x in ["game", "match", "playoff", "series", "championship"]):
        catalyst = "sports timing"
    elif any(x in txt for x in ["vote", "hearing", "ruling", "approval", "debate", "primary"]):
        catalyst = "decision window"
    elif any(x in txt for x in ["ftx", "bankruptcy", "payout", "sentencing"]):
        catalyst = "legal milestone"
    score = round(
        0.28 * min(1.0, bid / max(MIN_BID, 0.0001)) +
        0.22 * max(0.0, 1 - spread / 0.25) +
        0.20 * metrics["fill_confidence"] +
        0.15 * max(0.0, metrics.get("pressure", 0.0)) +
        0.15 * max(0.0, metrics.get("imbalance", 0.0)),
        3
    )
    return {
        "slug": market.get("slug", ""),
        "question": market.get("question", market.get("title", "")),
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "bid_depth": metrics["bid_depth"],
        "ask_depth": metrics["ask_depth"],
        "imbalance": metrics.get("imbalance", 0.0),
        "pressure": metrics.get("pressure", 0.0),
        "reason": reason,
        "catalyst": catalyst,
        "lane": lane,
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
        return round(
            0.28 * fill +
            0.22 * min(1.0, depth / 25.0) +
            0.18 * max(0.0, 1 - spread / max(MAX_SPREAD, 0.0001)) +
            0.12 * min(1.0, bid / max(MIN_BID, 0.0001)) +
            0.10 * max(0.0, m.get("pressure", 0.0)) +
            0.10 * max(0.0, m.get("imbalance", 0.0)),
            4
        )
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
        "discover_relaxed_admits": 0,
        "discover_low_flow_candidates": 0,
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
        "cooldown_skips": 0,
        "bad_family_skips": 0,
        "weak_timing_skips": 0,
        "low_catalyst_skips": 0,
    }
    markets = fetch_gamma_markets(DISCOVER_LIMIT) if AUTO_DISCOVER else []
    primary_pool: List[Dict[str, Any]] = []
    relaxed_pool: List[Dict[str, Any]] = []
    failopen_pool: List[Dict[str, Any]] = []

    def add_hard_skip(reason: str, market: Dict[str, Any]):
        stats["discover_hard_skipped"] += 1
        stats["discover_hard_skip_reason_counts"][reason] = stats["discover_hard_skip_reason_counts"].get(reason, 0) + 1
        if len(stats["discover_skip_samples"]) < 8:
            stats["discover_skip_samples"].append({"slug": market.get("slug"), "reason": reason, "question": market.get("question")})

    for m in markets:
        stats["discover_prelim"] += 1
        sports_like = is_sports_or_highflow_market(m)
        curated_like = is_curated_catalyst_market(m)
        event_like = sports_like or curated_like

        open_ok, open_reason = is_open_status_market(m)
        if not open_ok:
            add_hard_skip(f"hard_skip_{open_reason}", m)
            if len(stats.get("discover_reject_samples", [])) < 5:
                stats.setdefault("discover_reject_samples", []).append({
                    "slug": market_slug(m),
                    "reason": f"hard_skip_{open_reason}",
                    "time_source": "status",
                    "time_value": "status-gate",
                    "question": market_question(m),
                })
            continue

        cooldown_skip, cooldown_reason = should_cooldown_slug(m)
        if cooldown_skip:
            stats["cooldown_skips"] += 1
            if len(stats["discover_skip_samples"]) < 8:
                stats["discover_skip_samples"].append({
                    "slug": m.get("slug"),
                    "reason": cooldown_reason,
                    "question": m.get("question"),
                })
            continue

        bad_family, bad_reason = is_bad_microstructure_family(m)
        if bad_family:
            stats["bad_family_skips"] += 1
            if len(stats["discover_skip_samples"]) < 8:
                stats["discover_skip_samples"].append({
                    "slug": m.get("slug"),
                    "reason": bad_reason,
                    "question": m.get("question"),
                })
            continue

        strong_time = has_strong_time_signal(m)
        near_end_time = has_near_end_time(m)
        catalyst_score = catalyst_signal_score(m)
        if not strong_time and not near_end_time:
            stats["weak_timing_skips"] += 1
            if len(stats["discover_skip_samples"]) < 8:
                stats["discover_skip_samples"].append({
                    "slug": m.get("slug"),
                    "reason": "weak_time_signal",
                    "question": m.get("question"),
                })
            continue
        if catalyst_score < 0.6:
            stats["low_catalyst_skips"] += 1
            if len(stats["discover_skip_samples"]) < 8:
                stats["discover_skip_samples"].append({
                    "slug": m.get("slug"),
                    "reason": "low_catalyst_score",
                    "question": m.get("question"),
                })
            continue

        allowed_time, time_reason, time_dt, time_source = classify_time_window(m)
        if not allowed_time:
            add_hard_skip(time_reason, m)
            if len(stats.get("discover_reject_samples", [])) < 5:
                stats.setdefault("discover_reject_samples", []).append({
                    "slug": market_slug(m),
                    "reason": time_reason,
                    "time_source": time_source,
                    "time_value": time_dt.isoformat() if time_dt else "missing",
                    "question": market_question(m),
                })
            continue
        if ENABLE_YES_NO_ONLY and not is_yes_no_market(m):
            add_hard_skip("hard_skip_not_yes_no", m)
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

        # fail-open candidate pool for discovery debugging:
        # if a market is short-term, yes/no, and not obvious junk, let it be considered later
        failopen_pool.append(m)
        if len(stats.get("discover_time_debug", [])) < 5:
            _dt, _src = get_market_end_dt(m)
            stats.setdefault("discover_time_debug", []).append({
                "slug": market_slug(m),
                "question": market_question(m),
                "time_source": _src,
                "time_value": _dt.isoformat() if _dt else "missing",
            })

        liq = market_liquidity(m)
        vol = market_volume(m)
        min_liq = DISCOVER_MIN_LIQUIDITY * (0.35 if sports_like else 0.5 if curated_like else 1.0)
        min_vol = DISCOVER_MIN_VOLUME * (0.35 if sports_like else 0.5 if curated_like else 1.0)
        low_flow = (liq < min_liq and vol < min_vol)

        # Only skip weakly-themed markets. Let sports/catalyst names reach preflight even if low-flow.
        if (not event_like) and is_non_curated(m):
            stats["discover_non_curated_skips"] += 1
            if len(stats["discover_skip_samples"]) < 8:
                stats["discover_skip_samples"].append({"slug": m.get("slug"), "reason": "non_curated_skip", "question": m.get("question")})
            continue

        m["_priority"] = round(event_priority(m), 3)
        m["_low_flow"] = low_flow
        m["_sports_like"] = sports_like
        m["_curated_like"] = curated_like
        if low_flow:
            stats["discover_low_flow_candidates"] += 1
            relaxed_pool.append(m)
        else:
            primary_pool.append(m)

    primary_pool.sort(key=lambda x: (x.get("_priority", 1.0), market_liquidity(x), market_volume(x)), reverse=True)
    relaxed_pool.sort(key=lambda x: (x.get("_priority", 1.0), market_liquidity(x), market_volume(x)), reverse=True)

    selected: List[Dict[str, Any]] = []
    family_seen = set()
    bucket_counts: Dict[str, int] = {}
    adaptive_level = 0
    bucket_multiplier = 1.0
    target_bonus = 0
    if empty_scan_streak >= 10:
        adaptive_level = 1
        bucket_multiplier = 1.35
        target_bonus = 12
    if empty_scan_streak >= 20:
        adaptive_level = 2
        bucket_multiplier = 1.75
        target_bonus = 24
    if empty_scan_streak >= 35:
        adaptive_level = 3
        bucket_multiplier = 2.4
        target_bonus = 36
    stats["adaptive_intake_level"] = adaptive_level
    base_bucket_caps = {"sports": 36, "macro-policy": 18, "legal-special": 18, "politics-personnel": 18, "other": 10}
    bucket_caps = {k: max(v, int(round(v * bucket_multiplier))) for k, v in base_bucket_caps.items()}

    def try_add(m: Dict[str, Any], relaxed: bool = False):
        fam = market_family_key(m)
        if fam in family_seen:
            stats["discover_family_skips"] += 1
            return False
        bucket = family_bucket(m)
        cap = bucket_caps.get(bucket, 10)
        if bucket_counts.get(bucket, 0) >= cap:
            stats["discover_bucket_skips"] += 1
            return False
        family_seen.add(fam)
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        if relaxed:
            stats["discover_relaxed_admits"] += 1
        selected.append(m)
        return True

    target = min(DISCOVER_LIMIT, PREFLIGHT_MAX_CANDIDATES + target_bonus)
    for m in primary_pool:
        if len(selected) >= target:
            break
        try_add(m, relaxed=False)

    # If the front door is still too narrow, let some low-flow event markets reach preflight.
    if len(selected) < min(target, 24):
        for m in relaxed_pool:
            if len(selected) >= target:
                break
            try_add(m, relaxed=True)

    # Final fail-open path: if discovery still admitted nobody, let a capped number of
    # short-term yes/no non-junk markets reach preflight so the real engine can judge them.
    if not selected and failopen_pool:
        failopen_pool.sort(key=lambda x: (market_liquidity(x), market_volume(x), event_priority(x)), reverse=True)
        for m in failopen_pool[:min(target, 24)]:
            if try_add(m, relaxed=True):
                stats["discover_failopen_admits"] += 1

    final_list = selected[:target]
    for m in final_list[:5]:
        stats["discover_accept_samples"].append({
            "slug": m.get("slug"),
            "question": m.get("question"),
            "priority": m.get("_priority", 1.0),
            "relaxed": bool(m.get("_low_flow", False)),
        })
    return final_list, stats

# =========================================================
# Scan engine
# =========================================================
def scan_once() -> Dict[str, Any]:
    global last_pipeline_stats, last_preflight_reason_counts, last_near_passes, last_observation_results
    candidates, stats = discover_candidates()
    observations: List[Dict[str, Any]] = []
    structural_observations: List[Dict[str, Any]] = []
    prod_results: List[Dict[str, Any]] = []
    near_passes: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {}
    structural_reason_counts: Dict[str, int] = {}

    for market in candidates:
        stats["discover_checked"] += 1
        slug = market.get("slug", "")
        hydrated_market, hydration_source = hydrate_market_for_tokens(market)
        if hydration_source == "detail_ok":
            stats["hydrated_detail_hits"] = stats.get("hydrated_detail_hits", 0) + 1
        elif hydration_source == "detail_missing":
            stats["hydrated_detail_misses"] = stats.get("hydrated_detail_misses", 0) + 1

        yes_token, no_token = extract_yes_no_tokens(hydrated_market)
        if not yes_token or not no_token:
            stats["discover_resolve_failures"] += 1
            structural_reason_counts["missing_token_ids"] = structural_reason_counts.get("missing_token_ids", 0) + 1
            record_unresolved_market_failure(hydrated_market)
            structural_observations.append({
                "slug": hydrated_market.get("slug", ""),
                "question": hydrated_market.get("question", hydrated_market.get("title", "")),
                "lane": "structural",
                "reason": "missing token ids",
                "catalyst": "market plumbing",
                "score": 0.2,
            })
            if len(stats.setdefault("resolve_failure_samples", [])) < 5:
                raw_tokens = hydrated_market.get("tokens") or []
                token_sample = raw_tokens[:2] if isinstance(raw_tokens, list) else raw_tokens
                stats["resolve_failure_samples"].append({
                    "slug": hydrated_market.get("slug", ""),
                    "question": hydrated_market.get("question", hydrated_market.get("title", "")),
                    "hydration_source": hydration_source,
                    "yes_token": yes_token,
                    "no_token": no_token,
                    "outcomes": hydrated_market.get("outcomes"),
                    "token_sample": token_sample,
                })
            continue

        market = hydrated_market
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
            structural_reason_counts["book_lookup_failed"] = structural_reason_counts.get("book_lookup_failed", 0) + 1
            record_unresolved_market_failure(market)
            structural_observations.append({
                "slug": market.get("slug", ""),
                "question": market.get("question", market.get("title", "")),
                "lane": "structural",
                "reason": "book lookup failed",
                "catalyst": "market plumbing",
                "score": 0.25,
            })
            if len(stats.setdefault("book_failure_samples", [])) < 5:
                stats["book_failure_samples"].append({
                    "slug": market.get("slug", ""),
                    "question": market.get("question", market.get("title", "")),
                    "yes_token": yes_token,
                    "no_token": no_token,
                    "yes_book_ok": bool(yes_book),
                    "no_book_ok": bool(no_book),
                })
            continue

        record_market_resolved_success(market)

        yes_ok, yes_reason, yes_metrics = preflight_check(market, yes_book)
        no_ok, no_reason, no_metrics = preflight_check(market, no_book)

        # Observation lane before production hard pass, but still clean.
        obs_yes = observation_candidate(yes_metrics, market, lane="forming")
        obs_no = observation_candidate(no_metrics, market, lane="forming")
        if obs_yes:
            obs_yes["side"] = "YES"
            observations.append(obs_yes)
        if obs_no:
            obs_no["side"] = "NO"
            observations.append(obs_no)

        if not yes_ok and not no_ok:
            stats["discover_preflight_failures"] += 1
            final_reason = yes_reason if yes_reason != "ok" else no_reason
            reason_counts[final_reason] = reason_counts.get(final_reason, 0) + 1
            candidate_metrics = []
            if is_near_pass_metrics(yes_metrics):
                candidate_metrics.append(("YES", yes_metrics, yes_reason))
            if is_near_pass_metrics(no_metrics):
                candidate_metrics.append(("NO", no_metrics, no_reason))
            for side_name, m, side_reason in candidate_metrics[:2]:
                if len(near_passes) < 5:
                    near_passes.append({
                        "slug": slug,
                        "question": market.get("question"),
                        "side": side_name,
                        "reason": classify_trade_failure(m) if side_reason == "ok" else side_reason,
                        "bid": m["best_bid"],
                        "ask": m["best_ask"],
                        "spread": m["spread"],
                        "bid_depth": m["bid_depth"],
                        "ask_depth": m["ask_depth"],
                        "imbalance": m.get("imbalance", 0.0),
                        "pressure": m.get("pressure", 0.0),
                    })
                obs = observation_candidate(m, market, lane="near_pass")
                if obs:
                    obs["side"] = side_name
                    observations.append(obs)
            continue

        side, score, chosen_metrics = production_side_scores(yes_metrics, no_metrics)
        if score < 0:
            if is_near_pass_metrics(chosen_metrics) and len(near_passes) < 5:
                near_passes.append({
                    "slug": slug,
                    "question": market.get("question"),
                    "side": side,
                    "reason": classify_trade_failure(chosen_metrics),
                    "bid": chosen_metrics["best_bid"],
                    "ask": chosen_metrics["best_ask"],
                    "spread": chosen_metrics["spread"],
                    "bid_depth": chosen_metrics["bid_depth"],
                    "ask_depth": chosen_metrics["ask_depth"],
                    "imbalance": chosen_metrics.get("imbalance", 0.0),
                    "pressure": chosen_metrics.get("pressure", 0.0),
                })
            obs = observation_candidate(chosen_metrics, market, lane="near_pass")
            if obs:
                obs["side"] = side
                observations.append(obs)
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
            "imbalance": chosen_metrics.get("imbalance", 0.0),
            "pressure": chosen_metrics.get("pressure", 0.0),
            "category": category,
        })
        stats["discover_kept"] += 1

    prod_results.sort(key=lambda x: x["score"], reverse=True)
    observations.extend(structural_observations)
    observations.sort(key=lambda x: x["score"], reverse=True)

    # Deduplicate observation entries by slug, keeping best score.
    obs_map: Dict[str, Dict[str, Any]] = {}
    for o in observations:
        key = o["slug"] or o["question"]
        if key not in obs_map or o["score"] > obs_map[key]["score"]:
            prev = observation_seen.get(key)
            trend = "new"
            if prev:
                bid_diff = round(o.get("bid", 0.0) - prev.get("bid", 0.0), 4)
                spread_diff = round(prev.get("spread", 0.0) - o.get("spread", 0.0), 4)
                if bid_diff > 0 or spread_diff > 0:
                    trend = "improving"
                else:
                    trend = "flat"
                session_summary["observation_repeats"] += 1
            o["trend"] = trend
            obs_map[key] = o
            observation_seen[key] = {"bid": o.get("bid", 0.0), "spread": o.get("spread", 0.0), "ts": utc_ts()}

    final_observations = list(obs_map.values())[:5]
    if final_observations:
        session_summary["observation_hits"] += len(final_observations)
        session_summary["unique_forming"] = len(observation_seen)
        best = final_observations[0]
        if not session_summary["best_forming"] or best["score"] > session_summary["best_forming"].get("score", -1):
            session_summary["best_forming"] = best

    last_pipeline_stats = stats.copy()
    last_pipeline_stats["discover_preflight_reason_counts"] = reason_counts.copy()
    last_pipeline_stats["structural_reason_counts"] = structural_reason_counts.copy()
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
        "structural_reason_counts": structural_reason_counts,
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
        f"last_pipeline_summary={len(last_pipeline_stats)} keys",
        f"preflight_reasons={list(last_preflight_reason_counts.get('discover', {}).keys())[:3]}",
        f"near_passes_count={len(last_near_passes.get('discover', []))}",
        f"observation_count={len(last_observation_results)}",
        f"resolve_failure_samples={len((last_pipeline_stats or {}).get('resolve_failure_samples', []))}",
        f"book_failure_samples={len((last_pipeline_stats or {}).get('book_failure_samples', []))}",
        f"hydrated_detail_hits={(last_pipeline_stats or {}).get('hydrated_detail_hits', 0)}",
        f"hydrated_detail_misses={(last_pipeline_stats or {}).get('hydrated_detail_misses', 0)}",
        f"adaptive_intake_level={(last_pipeline_stats or {}).get('adaptive_intake_level', 0)}",
        f"structural_reasons={list((last_pipeline_stats or {}).get('structural_reason_counts', {}).keys())[:3]}",
        f"cooldown_skips={(last_pipeline_stats or {}).get('cooldown_skips', 0)}",
        f"bad_family_skips={(last_pipeline_stats or {}).get('bad_family_skips', 0)}",
        f"weak_timing_skips={(last_pipeline_stats or {}).get('weak_timing_skips', 0)}",
        f"low_catalyst_skips={(last_pipeline_stats or {}).get('low_catalyst_skips', 0)}",
        f"unresolved_slug_count={len(unresolved_slug_failures)}",
        f"top_unresolved_families={sorted(unresolved_family_failures.items(), key=lambda x: x[1], reverse=True)[:3]}",
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
    structural_reason_counts = scan.get("structural_reason_counts", {}) or {}

    top_reason = "none"
    if reason_counts:
        k, v = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[0]
        top_reason = f"{k}:{v}"
    hard_skip_reason = "none"
    hard_skip_counts = pipeline.get("discover_hard_skip_reason_counts", {}) or {}
    if hard_skip_counts:
        hk, hv = sorted(hard_skip_counts.items(), key=lambda x: x[1], reverse=True)[0]
        hard_skip_reason = f"{hk}:{hv}"

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
            f"discover_relaxed_admits={pipeline.get('discover_relaxed_admits', 0)} | "
            f"discover_failopen_admits={pipeline.get('discover_failopen_admits', 0)} | "
            f"discover_family_skips={pipeline.get('discover_family_skips', 0)} | "
            f"discover_bucket_skips={pipeline.get('discover_bucket_skips', 0)} | "
            f"cooldown_skips={pipeline.get('cooldown_skips', 0)} | "
            f"bad_family_skips={pipeline.get('bad_family_skips', 0)} | "
            f"weak_timing_skips={pipeline.get('weak_timing_skips', 0)} | "
            f"low_catalyst_skips={pipeline.get('low_catalyst_skips', 0)} | "
            f"adaptive_intake_level={pipeline.get('adaptive_intake_level', 0)} | "
            f"discover_checked={pipeline.get('discover_checked', 0)} | "
            f"discover_kept={pipeline.get('discover_kept', 0)} | "
            f"fallback_items={pipeline.get('fallback_items', 0)} | "
            f"fallback_checked={pipeline.get('fallback_checked', 0)} | "
            f"fallback_kept={pipeline.get('fallback_kept', 0)} | "
            f"cache_used={pipeline.get('cache_used', 0)} | "
            f"cache_refetched={pipeline.get('cache_refetched', 0)} | "
            f"resolve_failures={pipeline.get('discover_resolve_failures', 0)} | "
            f"structural_top={sorted(structural_reason_counts.items(), key=lambda x: x[1], reverse=True)[0][0] + ':' + str(sorted(structural_reason_counts.items(), key=lambda x: x[1], reverse=True)[0][1]) if structural_reason_counts else 'none'} | "
            f"resolve_samples={len(pipeline.get('resolve_failure_samples', []) or [])} | "f"book_samples={len(pipeline.get('book_failure_samples', []) or [])} | "f"hydrated_hits={pipeline.get('hydrated_detail_hits', 0)} | "f"hydrated_misses={pipeline.get('hydrated_detail_misses', 0)} | "
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
                f"side={n.get('side', '?')} reason={n['reason']}",
                f"bid={n['bid']} ask={n['ask']} spread={n['spread']} imbalance={n.get('imbalance', 0.0)} pressure={n.get('pressure', 0.0)}",
            ])
    else:
        lines.extend([
            "",
            "No tradable near-misses found.",
            "Most candidates failed preflight, depth, fill confidence, or cached candidate resolution.",
            f"Preflight detail: discover={reason_counts} fallback={{}}",
        ])
        resolve_samples = pipeline.get("resolve_failure_samples", []) or []
        book_samples = pipeline.get("book_failure_samples", []) or []
        if resolve_samples:
            lines.append("")
            lines.append("Resolve failure samples:")
            for s in resolve_samples[:3]:
                lines.extend([
                    f"{s.get('question') or 'unknown'}",
                    f"slug={s.get('slug', '')}",
                    f"hydration_source={s.get('hydration_source', 'unknown')}",
                    f"yes_token={s.get('yes_token')} no_token={s.get('no_token')}",
                    f"outcomes={s.get('outcomes')}",
                ])
        if book_samples:
            lines.append("")
            lines.append("Book failure samples:")
            for s in book_samples[:3]:
                lines.extend([
                    f"{s.get('question') or 'unknown'}",
                    f"slug={s.get('slug', '')}",
                    f"yes_token={s.get('yes_token')} no_token={s.get('no_token')}",
                    f"yes_book_ok={s.get('yes_book_ok')} no_book_ok={s.get('no_book_ok')}",
                ])

    if alerts or watches:
        lines.append("")
        lines.append("Tradable now:")
        for row in alerts + watches:
            lines.extend([
                f"{row['category']} | {row['question']}",
                f"slug={row['slug']} side={row['side']} score={row['score']}",
                f"bid={row['bid']} ask={row['ask']} spread={row['spread']} fill={row['fill_confidence']} imbalance={row.get('imbalance', 0.0)} pressure={row.get('pressure', 0.0)}",
                "",
            ])

    lines.append("")
    if observations:
        lines.append("Forming markets:")
        for o in observations:
            lines.extend([
                f"FORMING | {o['question']}",
                f"slug={o['slug']} side={o.get('side', '?')} lane={o.get('lane', 'forming')} score={o['score']} trend={o['trend']}",
                f"bid={o.get('bid', 0.0)} ask={o.get('ask', 0.0)} spread={o.get('spread', 0.0)} imbalance={o.get('imbalance', 0.0)} pressure={o.get('pressure', 0.0)}",
                f"bid_depth={o.get('bid_depth', 0.0)} ask_depth={o.get('ask_depth', 0.0)}",
                f"interesting={o['catalyst']} | not_ready={o['reason']}",
                "",
            ])
    else:
        lines.append("Forming markets: none")

    repeated_unresolved = sorted(unresolved_slug_failures.items(), key=lambda x: x[1], reverse=True)[:3]
    top_fams = sorted(unresolved_family_failures.items(), key=lambda x: x[1], reverse=True)[:3]
    lines.append("")
    lines.append(f"Recurring unresolved slugs: {repeated_unresolved if repeated_unresolved else 'none'}")
    lines.append(f"Recurring unresolved families: {top_fams if top_fams else 'none'}")

    lines.extend([
        "",
        "Session summary:",
        f"scans={session_summary.get('scans', 0)} | empty_prod_scans={session_summary.get('empty_prod_scans', 0)} | unique_forming={session_summary.get('unique_forming', 0)}",
        f"observation_hits={session_summary.get('observation_hits', 0)} | observation_repeats={session_summary.get('observation_repeats', 0)}",
        f"top_skip_reason={top_reason if top_reason else 'none'} | top_hard_skip={hard_skip_reason}",
        f"best_forming={session_summary.get('best_forming', {}) or 'none'}",
    ])
    return "\n".join(lines)


def format_zero_summary() -> str:
    reason_counts = last_preflight_reason_counts.get("discover", {}) or {}
    top_reason = "none"
    if reason_counts:
        k, v = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[0]
        top_reason = f"{k}:{v}"
    hard_skip_counts = last_pipeline_stats.get("discover_hard_skip_reason_counts", {}) or {}
    hard_skip_reason = "none"
    if hard_skip_counts:
        hk, hv = sorted(hard_skip_counts.items(), key=lambda x: x[1], reverse=True)[0]
        hard_skip_reason = f"{hk}:{hv}"
    best_forming = session_summary.get("best_forming") or {}
    best_forming_label = best_forming.get("question") if isinstance(best_forming, dict) else None
    return "\n".join([
        f"No qualifying markets in last {int(ZERO_SUMMARY_EVERY_SECONDS/60) + 2} minutes.",
        f"Empty scans: {empty_scan_streak}",
        f"Top preflight block: {top_reason}",
        f"Top hard-skip block: {hard_skip_reason}",
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
    cmd = (text or "").strip().lower()
    if cmd.startswith("/health"):
        return format_health_text()
    if cmd.startswith("/scan"):
        return "__RUN_SCAN_ASYNC__"
    return "Commands: /health, /scan"


def run_manual_scan_async(chat_id: str) -> None:
    global manual_scan_in_progress
    try:
        scan = run_scan_and_update()
        reply = format_scan_text(scan)
        if TELEGRAM_BOT_TOKEN and chat_id:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": reply},
                timeout=REQUEST_TIMEOUT,
            )
    except Exception as exc:
        if TELEGRAM_BOT_TOKEN and chat_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": chat_id, "text": f"Scan error: {exc}"},
                    timeout=REQUEST_TIMEOUT,
                )
            except Exception:
                pass
    finally:
        with state_lock:
            manual_scan_in_progress = False


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
    global manual_scan_in_progress
    payload = request.get_json(silent=True) or {}
    msg = payload.get("message") or payload.get("edited_message") or {}
    chat_id = str(msg.get("chat", {}).get("id") or "").strip()
    text = msg.get("text", "")
    reply = handle_command(text)

    if reply == "__RUN_SCAN_ASYNC__":
        with state_lock:
            if manual_scan_in_progress:
                if TELEGRAM_BOT_TOKEN and chat_id:
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": "Scan already running. Wait for result."},
                            timeout=REQUEST_TIMEOUT,
                        )
                    except Exception:
                        pass
                return jsonify({"ok": True})
            manual_scan_in_progress = True

        if TELEGRAM_BOT_TOKEN and chat_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": chat_id, "text": "Running scan..."},
                    timeout=REQUEST_TIMEOUT,
                )
            except Exception:
                pass
        threading.Thread(target=run_manual_scan_async, args=(chat_id,), daemon=True).start()
        return jsonify({"ok": True})

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
