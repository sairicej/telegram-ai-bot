import os
import requests
from flask import Flask, request, jsonify

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

app = Flask(__name__)

def send_telegram_message(chat_id: int, text: str):
    r = requests.post(
        f"{TELEGRAM_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=30,
    )
    r.raise_for_status()

@app.route("/", methods=["GET"])
def home():
    return "Bot is running", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True) or {}
        message = update.get("message", {})
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()

        if not chat_id:
            return jsonify({"ok": True}), 200

        if text.lower() == "/chatid":
            reply = f"Your chat ID is: {chat_id}"
        else:
            reply = f"You said: {text}" if text else "No text received."

        send_telegram_message(chat_id, reply)
        return jsonify({"ok": True}), 200

    except Exception as e:
        print("WEBHOOK ERROR:", str(e), flush=True)
        return jsonify({"ok": True}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
