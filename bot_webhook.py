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
LIVE_FEED_SOURCE_URL = os.environ.get("LIVE_FEED_SOURCE_URL", "").strip()
SCHEDULER_SECRET = os.environ.get("SCHEDULER_SECRET", "").strip()
TELEGRAM_ALERT_CHAT_ID = os.environ.get("TELEGRAM_ALERT_CHAT_ID", "").strip()

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

    return hdr + core + ask_line + sens + warn

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

# =====================
# LIVE FEED HELPERS
# =====================
def load_text_blocks_from_file(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    if not raw:
        return []
    return split_blocks(raw)

def load_live_feed_markets_local():
    return load_text_blocks_from_file(LIVE_FEED_MARKETS_FILE)

def fetch_live_feed_blocks():
    if LIVE_FEED_SOURCE_URL:
        try:
            r = requests.get(LIVE_FEED_SOURCE_URL, timeout=30)
            r.raise_for_status()
            raw = (r.text or "").strip()
            if raw:
                blocks = split_blocks(raw)
                return blocks, f"Fetched from LIVE_FEED_SOURCE_URL ({len(blocks)} blocks)"
            return [], "LIVE_FEED_SOURCE_URL returned empty text"
        except Exception as e:
            fallback = load_live_feed_markets_local()
            return fallback, f"Fetch failed, used local fallback: {str(e)}"
    fallback = load_live_feed_markets_local()
    return fallback, "Used local live_feed_markets.txt"

def score_blocks(blocks):
    return [compute_one(extract_inputs(block)) for block in blocks]

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
        r.raise_for_status()

def send_alert_message(text: str):
    if not TELEGRAM_ALERT_CHAT_ID:
        return "TELEGRAM_ALERT_CHAT_ID not set"
    send_telegram_message(int(TELEGRAM_ALERT_CHAT_ID), text)
    return f"Alert sent to chat {TELEGRAM_ALERT_CHAT_ID}"

# =====================
# COMMAND RUNNERS
# =====================
def run_chatid(chat_id: int):
    return f"Your chat ID is: {chat_id}"

def run_fetchlive():
    blocks, source_note = fetch_live_feed_blocks()
    return f"{source_note}\nLoaded {len(blocks)} live-feed market blocks."

def run_livefeedcounts():
    blocks, source_note = fetch_live_feed_blocks()
    results = score_blocks(blocks)
    ok_results = [r for r in results if r.get('ok')]
    smalltest = sum(1 for r in ok_results if r["action"] == "SMALL TEST")
    watch = sum(1 for r in ok_results if r["action"] == "WATCH")
    skip = sum(1 for r in ok_results if r["action"] == "SKIP")
    return (
        f"{source_note}\n"
        f"Live-feed market counts:\n"
        f"- Total valid markets: {len(ok_results)}\n"
        f"- SMALL TEST: {smalltest}\n"
        f"- WATCH: {watch}\n"
        f"- SKIP: {skip}"
    )

def run_livefeedalert():
    blocks, source_note = fetch_live_feed_blocks()
    alerts = filter_alert_candidates(score_blocks(blocks))
    if not alerts:
        return f"{source_note}\nNo alert-ready live-feed markets found."
    return f"{source_note}\n\n" + format_ranked(alerts)

def run_alert_scan_and_send():
    blocks, source_note = fetch_live_feed_blocks()
    if not blocks:
        return f"{source_note}\nNo live-feed markets found."

    alerts = filter_alert_candidates(score_blocks(blocks))
    if not alerts:
        return f"{source_note}\nNo alert-ready live-feed markets found."

    alert_text = "LIVE ALERT SCAN\n\n" + format_ranked(alerts)
    send_result = send_alert_message(alert_text)
    return f"{source_note}\nFound {len(alerts)} alert-ready market(s).\n{send_result}"

# =====================
# FLASK ROUTES
# =====================
@app.route("/", methods=["GET"])
def home():
    return "Market screener bot is running", 200

@app.route("/cron/<secret>", methods=["GET", "POST"])
def cron_scan(secret):
    if not SCHEDULER_SECRET or secret != SCHEDULER_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    result = run_alert_scan_and_send()
    return jsonify({"ok": True, "message": result}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True) or {}
        message = update.get("message", {})
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()

        if not chat_id or not text:
            return jsonify({"ok": True}), 200

        lower = text.lower()

        if lower == "/chatid":
            reply = run_chatid(chat_id)
        elif lower == "/fetchlive":
            reply = run_fetchlive()
        elif lower == "/livefeedcounts":
            reply = run_livefeedcounts()
        elif lower == "/livefeedalert":
            reply = run_livefeedalert()
        elif lower == "/runalertscan":
            reply = run_alert_scan_and_send()
        else:
            reply = "Use /chatid, /fetchlive, /livefeedcounts, /livefeedalert, or /runalertscan"

        send_telegram_message(chat_id, reply)
        return jsonify({"ok": True}), 200

    except Exception as e:
        print("WEBHOOK ERROR:", str(e), flush=True)
        return jsonify({"ok": True}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
