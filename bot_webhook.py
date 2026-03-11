import os
import re
import math
import requests
from flask import Flask, request, jsonify

# =====================
# SETTINGS
# =====================
EDGE_THRESHOLD = 0.10
STRONG_EDGE = 0.20
VOL_BUMP = 0.05
MIN_LIQUIDITY = 5000.0
GOOD_LIQUIDITY = 20000.0
MAX_SPREAD = 0.03
GOOD_SPREAD = 0.015
IGNORE_NEAR_CERTAIN = True

# Alert-ready settings
ALERT_MIN_EDGE = 0.20
ALERT_MIN_LIQUIDITY = 20000.0
ALERT_REQUIRE_ROBUST = True

SAMPLE_MARKETS_FILE = "sample_markets.txt"
CANDIDATE_MARKETS_FILE = "candidate_markets.txt"
INCOMING_MARKETS_FILE = "incoming_markets.txt"
SHORTLIST_MARKETS_FILE = "shortlist_markets.txt"
LIVE_FEED_MARKETS_FILE = "live_feed_markets.txt"

# =====================
# ENV / TOKEN
# =====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

app = Flask(__name__)

# =====================
# HELPERS
# =====================
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def parse_float(patterns, text: str):
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                pass
    return None

def parse_text(patterns, text: str):
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return None

def split_blocks(text: str):
    parts = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    return parts if parts else [text.strip()]

def extract_inputs(block: str):
    market = parse_text([r"\bmarket\b\s*[:=]?\s*(.+)"], block) or "Unnamed market"
    mode_text = parse_text([r"\bmode\b\s*[:=]?\s*(.+)"], block)

    spot = parse_float([r"\bspot\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)
    strike = parse_float([r"\bstrike\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)
    vol = parse_float([r"\bvol\b(?:atility)?\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)
    days = parse_float([
        r"\bexpiry\b\s*[:=]?\s*([0-9]*\.?[0-9]+)\s*days",
        r"\bexpiry\b\s*[:=]?\s*([0-9]*\.?[0-9]+)",
        r"\bdays\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"
    ], block)

    yes_ask = parse_float([r"\byes\s*ask\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)
    no_ask = parse_float([r"\bno\s*ask\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)
    liquidity = parse_float([r"\bliquidity\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)
    spread = parse_float([r"\bspread\b\s*[:=]?\s*([0-9]*\.?[0-9]+)"], block)

    if vol is not None and vol > 1.5:
        vol = vol / 100.0
    if yes_ask is not None and yes_ask > 1.5:
        yes_ask = yes_ask / 100.0
    if no_ask is not None and no_ask > 1.5:
        no_ask = no_ask / 100.0

    if mode_text:
        is_touch = mode_text.lower().strip() in ["touch", "hit", "high", "reach"]
    else:
        is_touch = bool(re.search(r"\b(touch|hit|high|reach|reaches|hitting)\b", block, re.IGNORECASE))

    return {
        "market": market,
        "spot": spot,
        "strike": strike,
        "vol": vol,
        "days": days,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "liquidity": liquidity,
        "spread": spread,
        "is_touch": is_touch,
    }

# =====================
# MODELS
# =====================
def fair_yes_settle_above(spot: float, strike: float, vol: float, T_years: float):
    if spot <= 0 or strike <= 0 or vol <= 0 or T_years <= 0:
        return None
    d2 = (math.log(spot / strike) - 0.5 * vol * vol * T_years) / (vol * math.sqrt(T_years))
    return clamp01(norm_cdf(d2))

def fair_yes_touch_above(spot: float, barrier: float, vol: float, T_years: float):
    if spot <= 0 or barrier <= 0 or vol <= 0 or T_years <= 0:
        return None
    if barrier <= spot:
        return 1.0

    x = math.log(spot)
    b = math.log(barrier)
    a = b - x
    sigma = vol
    mu = -0.5 * sigma * sigma
    denom = sigma * math.sqrt(T_years)

    z1 = (-a + mu * T_years) / denom
    z2 = (-a - mu * T_years) / denom
    adjust = math.exp((2.0 * mu * a) / (sigma * sigma))

    p_hit = norm_cdf(z1) + adjust * norm_cdf(z2)
    return clamp01(p_hit)

# =====================
# CORE CALC
# =====================
def build_side_result(side_name, fair_prob, ask_price, edge_low, edge_mid, edge_high):
    robust = edge_low > 0 and edge_mid > 0 and edge_high > 0

    verdict = "FAIR"
    if edge_mid > 0.01:
        verdict = f"CHEAP ({side_name})"
    elif edge_mid < -0.01:
        verdict = f"EXPENSIVE ({side_name})"

    return {
        "side": side_name,
        "fair_prob": fair_prob,
        "ask_price": ask_price,
        "edge_mid": edge_mid,
        "edge_low": edge_low,
        "edge_high": edge_high,
        "robust": robust,
        "verdict": verdict,
    }

def decide_action(best, liquidity, spread, warnings):
    if warnings:
        if any(w in warnings for w in [
            "Near-certain price",
            "Low liquidity",
            "Wide spread",
            "Below edge threshold",
            "Fragile across vol band",
        ]):
            if best["edge_mid"] >= EDGE_THRESHOLD and "Fragile across vol band" in warnings:
                return "WATCH"
            return "SKIP"

    if best["edge_mid"] >= STRONG_EDGE and best["robust"]:
        if (liquidity is None or liquidity >= GOOD_LIQUIDITY) and (spread is None or spread <= GOOD_SPREAD):
            return "SMALL TEST"
        return "WATCH"

    if best["edge_mid"] >= EDGE_THRESHOLD:
        return "WATCH"

    return "SKIP"

def compute_one(m):
    spot = m["spot"]
    strike = m["strike"]
    vol = m["vol"]
    days = m["days"]
    yes_ask = m["yes_ask"]
    no_ask = m["no_ask"]
    is_touch = m["is_touch"]

    if None in [spot, strike, vol, days]:
        return {"ok": False, "reason": "Missing Spot / Strike / Vol / Expiry"}

    if yes_ask is None and no_ask is None:
        return {"ok": False, "reason": "Need at least Yes ask or No ask"}

    T = days / 365.0
    if T <= 0:
        return {"ok": False, "reason": "Expiry must be > 0"}

    if is_touch:
        model_name = "TOUCH/HIT"
        f_yes_mid = fair_yes_touch_above(spot, strike, vol, T)
        f_yes_low = fair_yes_touch_above(spot, strike, max(0.01, vol - VOL_BUMP), T)
        f_yes_high = fair_yes_touch_above(spot, strike, vol + VOL_BUMP, T)
    else:
        model_name = "SETTLE"
        f_yes_mid = fair_yes_settle_above(spot, strike, vol, T)
        f_yes_low = fair_yes_settle_above(spot, strike, max(0.01, vol - VOL_BUMP), T)
        f_yes_high = fair_yes_settle_above(spot, strike, vol + VOL_BUMP, T)

    if None in [f_yes_mid, f_yes_low, f_yes_high]:
        return {"ok": False, "reason": "Computation error"}

    f_no_mid = 1.0 - f_yes_mid
    f_no_low = 1.0 - f_yes_low
    f_no_high = 1.0 - f_yes_high

    yes_side = None
    no_side = None

    if yes_ask is not None:
        yes_side = build_side_result(
            "YES",
            f_yes_mid,
            yes_ask,
            f_yes_low - yes_ask,
            f_yes_mid - yes_ask,
            f_yes_high - yes_ask,
        )

    if no_ask is not None:
        no_side = build_side_result(
            "NO",
            f_no_mid,
            no_ask,
            f_no_low - no_ask,
            f_no_mid - no_ask,
            f_no_high - no_ask,
        )

    candidates = [x for x in [yes_side, no_side] if x is not None]
    best = max(candidates, key=lambda x: x["edge_mid"])

    warnings = []

    if m["liquidity"] is not None and m["liquidity"] < MIN_LIQUIDITY:
        warnings.append("Low liquidity")

    if m["spread"] is not None and m["spread"] > MAX_SPREAD:
        warnings.append("Wide spread")

    if IGNORE_NEAR_CERTAIN:
        near_certain_hit = False
        for p in [yes_ask, no_ask]:
            if p is not None and (p < 0.05 or p > 0.95):
                near_certain_hit = True
        if near_certain_hit:
            warnings.append("Near-certain price")

    if abs(best["edge_mid"]) < EDGE_THRESHOLD:
        warnings.append("Below edge threshold")

    if not best["robust"]:
        warnings.append("Fragile across vol band")

    action = decide_action(best, m["liquidity"], m["spread"], warnings)

    return {
        "ok": True,
        "market": m["market"],
        "model": model_name,
        "spot": spot,
        "strike": strike,
        "vol": vol,
        "days": days,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "liquidity": m["liquidity"],
        "spread": m["spread"],
        "fair_yes": f_yes_mid,
        "fair_no": f_no_mid,
        "yes_side": yes_side,
        "no_side": no_side,
        "best": best,
        "warnings": warnings,
        "action": action,
    }

# =====================
# FORMATTERS
# =====================
def fmt_pct(x):
    return f"{x:.2%}"

def format_side(side):
    if side is None:
        return "n/a"
    return (
        f"{side['side']}: fair {fmt_pct(side['fair_prob'])} | "
        f"ask {fmt_pct(side['ask_price'])} | "
        f"edge {fmt_pct(side['edge_mid'])} | "
        f"{side['verdict']}"
    )

def format_one(r):
    if not r["ok"]:
        return f"❌ {r['reason']}"

    hdr = f"Market: {r['market']}\nModel: {r['model']}\nAction: {r['action']}\n"
    core = (
        f"- Fair YES: {fmt_pct(r['fair_yes'])}\n"
        f"- Fair NO: {fmt_pct(r['fair_no'])}\n"
        f"- Best side: {r['best']['side']}\n"
        f"- Best edge: {fmt_pct(r['best']['edge_mid'])}\n"
        f"- Best verdict: {r['best']['verdict']}\n"
    )

    asks = []
    if r["yes_ask"] is not None:
        asks.append(f"Yes ask {fmt_pct(r['yes_ask'])}")
    if r["no_ask"] is not None:
        asks.append(f"No ask {fmt_pct(r['no_ask'])}")

    ask_line = ""
    if asks:
        ask_line = "- " + " | ".join(asks) + "\n"

    side_lines = f"- {format_side(r['yes_side'])}\n- {format_side(r['no_side'])}\n"

    extras = []
    if r["liquidity"] is not None:
        extras.append(f"Liquidity: {r['liquidity']:.0f}")
    if r["spread"] is not None:
        extras.append(f"Spread: {r['spread']:.2%}")

    extra_line = ""
    if extras:
        extra_line = "\n" + " | ".join(extras)

    best = r["best"]
    sens = (
        f"\nBest-side vol sensitivity:\n"
        f"- low: {fmt_pct(best['edge_low'])}\n"
        f"- mid: {fmt_pct(best['edge_mid'])}\n"
        f"- high: {fmt_pct(best['edge_high'])}\n"
        f"- Robust: {'YES' if best['robust'] else 'NO'}"
    )

    warn = ""
    if r["warnings"]:
        warn = "\nWarnings: " + ", ".join(r["warnings"])

    checklist = ""
    if r["action"] == "SMALL TEST":
        checklist = (
            "\nChecklist:\n"
            "- Confirm event type matches model\n"
            "- Confirm ask is still live\n"
            "- Use small size only\n"
            "- Be willing to lose full premium\n"
        )
    elif r["action"] == "WATCH":
        checklist = (
            "\nChecklist:\n"
            "- Recheck later\n"
            "- Watch liquidity and spread\n"
            "- See if edge improves\n"
        )

    return hdr + core + ask_line + side_lines + extra_line + sens + warn + checklist

def format_ranked(results):
    good = [r for r in results if r["ok"]]
    if not good:
        return "No valid markets parsed."

    ranked = sorted(good, key=lambda x: x["best"]["edge_mid"], reverse=True)

    lines = ["Top screens:\n"]
    for i, r in enumerate(ranked[:10], start=1):
        badge = {"SMALL TEST": "✅", "WATCH": "👀", "SKIP": "⛔"}.get(r["action"], "•")
        best = r["best"]
        robust = "robust" if best["robust"] else "fragile"
        lines.append(
            f"{i}. {badge} {r['market']} | {r['action']} | best {best['side']} | edge {fmt_pct(best['edge_mid'])} | {robust}"
        )

    lines.append("\nDetails:\n")
    for r in ranked[:5]:
        lines.append(format_one(r))
        lines.append("\n---\n")

    return "\n".join(lines)

def format_counts(results, label):
    ok_results = [r for r in results if r.get("ok")]
    total = len(ok_results)
    smalltest = sum(1 for r in ok_results if r["action"] == "SMALL TEST")
    watch = sum(1 for r in ok_results if r["action"] == "WATCH")
    skip = sum(1 for r in ok_results if r["action"] == "SKIP")

    return (
        f"{label}\n"
        f"- Total valid markets: {total}\n"
        f"- SMALL TEST: {smalltest}\n"
        f"- WATCH: {watch}\n"
        f"- SKIP: {skip}"
    )

# =====================
# FILE LOADERS
# =====================
def load_text_blocks_from_file(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    if not raw:
        return []
    return split_blocks(raw)

def load_sample_markets():
    return load_text_blocks_from_file(SAMPLE_MARKETS_FILE)

def load_candidate_markets():
    return load_text_blocks_from_file(CANDIDATE_MARKETS_FILE)

def load_incoming_markets():
    return load_text_blocks_from_file(INCOMING_MARKETS_FILE)

def load_shortlist_markets():
    return load_text_blocks_from_file(SHORTLIST_MARKETS_FILE)

def load_live_feed_markets():
    return load_text_blocks_from_file(LIVE_FEED_MARKETS_FILE)

# =====================
# FILTER HELPERS
# =====================
def score_blocks(blocks):
    results = []
    for block in blocks:
        m = extract_inputs(block)
        results.append(compute_one(m))
    return results

def filter_results_by_action(results, actions):
    filtered = [
        r for r in results
        if r.get("ok") and r.get("action") in actions
    ]
    return sorted(filtered, key=lambda x: x["best"]["edge_mid"], reverse=True)

def filter_alert_candidates(results):
    filtered = []
    for r in results:
        if not r.get("ok"):
            continue
        if r["best"]["edge_mid"] < ALERT_MIN_EDGE:
            continue
        if ALERT_REQUIRE_ROBUST and not r["best"]["robust"]:
            continue
        if r.get("liquidity") is not None and r["liquidity"] < ALERT_MIN_LIQUIDITY:
            continue
        filtered.append(r)
    return sorted(filtered, key=lambda x: x["best"]["edge_mid"], reverse=True)

def quick_top_text(results, title, limit=5):
    ok_results = [r for r in results if r.get("ok")]
    if not ok_results:
        return f"No valid markets found for {title.lower()}."

    ranked = sorted(ok_results, key=lambda x: x["best"]["edge_mid"], reverse=True)

    lines = [f"{title}\n"]
    for i, r in enumerate(ranked[:limit], start=1):
        badge = {"SMALL TEST": "✅", "WATCH": "👀", "SKIP": "⛔"}.get(r["action"], "•")
        best = r["best"]
        lines.append(
            f"{i}. {badge} {r['market']} | {r['action']} | best {best['side']} | edge {fmt_pct(best['edge_mid'])}"
        )
    return "\n".join(lines)

# =====================
# COMMAND TEXT
# =====================
def help_text():
    return (
        "Commands:\n"
        "/help - show commands\n"
        "/format - show the market format\n"
        "/scan - score sample_markets.txt\n\n"
        "Candidate commands:\n"
        "/top - top candidates\n"
        "/watch - WATCH and SMALL TEST candidates\n"
        "/smalltest - only SMALL TEST candidates\n"
        "/skip - only SKIP candidates\n"
        "/counts - candidate counts\n"
        "/refresh - reload candidate file count\n"
        "/alertcheck - candidate alert-ready markets\n\n"
        "Inbox commands:\n"
        "/inbox - full inbox scoring\n"
        "/inboxtop - top inbox ideas\n"
        "/inboxwatch - inbox WATCH and SMALL TEST\n"
        "/inboxcounts - inbox counts\n"
        "/inboxalert - inbox alert-ready markets\n\n"
        "Shortlist commands:\n"
        "/shortlist - full shortlist scoring\n"
        "/shortlisttop - top shortlist ideas\n"
        "/shortlistcounts - shortlist counts\n\n"
        "Live-feed commands:\n"
        "/livefeed - full live-feed scoring\n"
        "/livefeedtop - top live-feed ideas\n"
        "/livefeedwatch - live-feed WATCH and SMALL TEST\n"
        "/livefeedcounts - live-feed counts\n"
        "/livefeedalert - live-feed alert-ready markets\n\n"
        "You can also paste one or more market blocks directly."
    )

def format_text():
    return (
        "Use this format:\n\n"
        "Market Oil hit 90 by Mar 31\n"
        "Mode touch\n"
        "Spot 81.43\n"
        "Strike 90\n"
        "Vol 0.35\n"
        "Expiry 26 days\n"
        "Yes ask 0.71\n"
        "No ask 0.30\n"
        "Liquidity 24760\n"
        "Spread 0.01"
    )

# =====================
# COMMAND RUNNERS
# =====================
def run_scan():
    blocks = load_sample_markets()
    if not blocks:
        return "No sample markets found in sample_markets.txt"
    return format_ranked(score_blocks(blocks))

def run_top():
    blocks = load_candidate_markets()
    if not blocks:
        return "No candidate markets found in candidate_markets.txt"
    return quick_top_text(score_blocks(blocks), "Top candidate markets:")

def run_watch():
    blocks = load_candidate_markets()
    if not blocks:
        return "No candidate markets found in candidate_markets.txt"
    filtered = filter_results_by_action(score_blocks(blocks), ["SMALL TEST", "WATCH"])
    if not filtered:
        return "No WATCH or SMALL TEST markets found."
    return format_ranked(filtered)

def run_smalltest():
    blocks = load_candidate_markets()
    if not blocks:
        return "No candidate markets found in candidate_markets.txt"
    filtered = filter_results_by_action(score_blocks(blocks), ["SMALL TEST"])
    if not filtered:
        return "No SMALL TEST markets found."
    return format_ranked(filtered)

def run_skip():
    blocks = load_candidate_markets()
    if not blocks:
        return "No candidate markets found in candidate_markets.txt"
    filtered = filter_results_by_action(score_blocks(blocks), ["SKIP"])
    if not filtered:
        return "No SKIP markets found."
    return format_ranked(filtered)

def run_counts():
    blocks = load_candidate_markets()
    if not blocks:
        return "No candidate markets found in candidate_markets.txt"
    return format_counts(score_blocks(blocks), "Candidate market counts:")

def run_refresh():
    blocks = load_candidate_markets()
    count = len(blocks)
    if count == 0:
        return "Refresh done. No candidate markets found in candidate_markets.txt"
    return f"Refresh done. Loaded {count} candidate market blocks from candidate_markets.txt"

def run_alertcheck():
    blocks = load_candidate_markets()
    if not blocks:
        return "No candidate markets found in candidate_markets.txt"
    filtered = filter_alert_candidates(score_blocks(blocks))
    if not filtered:
        return "No alert-ready candidate markets found."
    return format_ranked(filtered)

def run_inbox():
    blocks = load_incoming_markets()
    if not blocks:
        return "No incoming markets found in incoming_markets.txt"
    return format_ranked(score_blocks(blocks))

def run_inboxtop():
    blocks = load_incoming_markets()
    if not blocks:
        return "No incoming markets found in incoming_markets.txt"
    return quick_top_text(score_blocks(blocks), "Top inbox markets:")

def run_inboxwatch():
    blocks = load_incoming_markets()
    if not blocks:
        return "No incoming markets found in incoming_markets.txt"
    filtered = filter_results_by_action(score_blocks(blocks), ["SMALL TEST", "WATCH"])
    if not filtered:
        return "No WATCH or SMALL TEST inbox markets found."
    return format_ranked(filtered)

def run_inboxcounts():
    blocks = load_incoming_markets()
    if not blocks:
        return "No incoming markets found in incoming_markets.txt"
    return format_counts(score_blocks(blocks), "Inbox market counts:")

def run_inboxalert():
    blocks = load_incoming_markets()
    if not blocks:
        return "No incoming markets found in incoming_markets.txt"
    filtered = filter_alert_candidates(score_blocks(blocks))
    if not filtered:
        return "No alert-ready inbox markets found."
    return format_ranked(filtered)

def run_shortlist():
    blocks = load_shortlist_markets()
    if not blocks:
        return "No shortlist markets found in shortlist_markets.txt"
    return format_ranked(score_blocks(blocks))

def run_shortlisttop():
    blocks = load_shortlist_markets()
    if not blocks:
        return "No shortlist markets found in shortlist_markets.txt"
    return quick_top_text(score_blocks(blocks), "Top shortlist markets:")

def run_shortlistcounts():
    blocks = load_shortlist_markets()
    if not blocks:
        return "No shortlist markets found in shortlist_markets.txt"
    return format_counts(score_blocks(blocks), "Shortlist market counts:")

def run_livefeed():
    blocks = load_live_feed_markets()
    if not blocks:
        return "No live-feed markets found in live_feed_markets.txt"
    return format_ranked(score_blocks(blocks))

def run_livefeedtop():
    blocks = load_live_feed_markets()
    if not blocks:
        return "No live-feed markets found in live_feed_markets.txt"
    return quick_top_text(score_blocks(blocks), "Top live-feed markets:")

def run_livefeedwatch():
    blocks = load_live_feed_markets()
    if not blocks:
        return "No live-feed markets found in live_feed_markets.txt"
    filtered = filter_results_by_action(score_blocks(blocks), ["SMALL TEST", "WATCH"])
    if not filtered:
        return "No WATCH or SMALL TEST live-feed markets found."
    return format_ranked(filtered)

def run_livefeedcounts():
    blocks = load_live_feed_markets()
    if not blocks:
        return "No live-feed markets found in live_feed_markets.txt"
    return format_counts(score_blocks(blocks), "Live-feed market counts:")

def run_livefeedalert():
    blocks = load_live_feed_markets()
    if not blocks:
        return "No live-feed markets found in live_feed_markets.txt"
    filtered = filter_alert_candidates(score_blocks(blocks))
    if not filtered:
        return "No alert-ready live-feed markets found."
    return format_ranked(filtered)

# =====================
# TELEGRAM SEND
# =====================
def split_message_for_telegram(text, max_len=3500):
    if len(text) <= max_len:
        return [text]

    chunks = []
    remaining = text

    while len(remaining) > max_len:
        cut = remaining.rfind("\n---\n", 0, max_len)
        if cut == -1:
            cut = remaining.rfind("\n\n", 0, max_len)
        if cut == -1:
            cut = max_len

        chunk = remaining[:cut].strip()
        if chunk:
            chunks.append(chunk)

        remaining = remaining[cut:].strip()

    if remaining:
        chunks.append(remaining)

    return chunks

def send_telegram_message(chat_id: int, text: str):
    chunks = split_message_for_telegram(text)

    for chunk in chunks:
        r = requests.post(
            f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": chat_id, "text": chunk},
            timeout=30,
        )
        print("TELEGRAM SEND:", r.status_code, flush=True)
        r.raise_for_status()

# =====================
# FLASK ROUTES
# =====================
@app.route("/", methods=["GET"])
def home():
    return "Market screener bot is running", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True) or {}
        print("INCOMING UPDATE:", update, flush=True)

        message = update.get("message", {})
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()

        if not chat_id or not text:
            return jsonify({"ok": True}), 200

        lower = text.lower()

        if lower == "/help":
            reply = help_text()
        elif lower == "/format":
            reply = format_text()
        elif lower == "/scan":
            reply = run_scan()
        elif lower == "/top":
            reply = run_top()
        elif lower == "/watch":
            reply = run_watch()
        elif lower == "/smalltest":
            reply = run_smalltest()
        elif lower == "/skip":
            reply = run_skip()
        elif lower == "/counts":
            reply = run_counts()
        elif lower == "/refresh":
            reply = run_refresh()
        elif lower == "/alertcheck":
            reply = run_alertcheck()
        elif lower == "/inbox":
            reply = run_inbox()
        elif lower == "/inboxtop":
            reply = run_inboxtop()
        elif lower == "/inboxwatch":
            reply = run_inboxwatch()
        elif lower == "/inboxcounts":
            reply = run_inboxcounts()
        elif lower == "/inboxalert":
            reply = run_inboxalert()
        elif lower == "/shortlist":
            reply = run_shortlist()
        elif lower == "/shortlisttop":
            reply = run_shortlisttop()
        elif lower == "/shortlistcounts":
            reply = run_shortlistcounts()
        elif lower == "/livefeed":
            reply = run_livefeed()
        elif lower == "/livefeedtop":
            reply = run_livefeedtop()
        elif lower == "/livefeedwatch":
            reply = run_livefeedwatch()
        elif lower == "/livefeedcounts":
            reply = run_livefeedcounts()
        elif lower == "/livefeedalert":
            reply = run_livefeedalert()
        else:
            reply = format_ranked(score_blocks(split_blocks(text)))

        send_telegram_message(chat_id, reply)
        return jsonify({"ok": True}), 200

    except Exception as e:
        print("WEBHOOK ERROR:", str(e), flush=True)
        return jsonify({"ok": True}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
