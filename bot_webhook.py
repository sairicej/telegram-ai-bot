import os
import requests
from flask import Flask, request

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

app = Flask(__name__)

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_message(chat_id, text):
    url = f"{TELEGRAM_API}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    r = requests.post(url, json=payload, timeout=30)
    print("TELEGRAM SEND STATUS:", r.status_code)
    print("TELEGRAM SEND RESPONSE:", r.text)
    r.raise_for_status()

def ask_openai(prompt):
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": "gpt-4.1-mini",
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=data,
        timeout=60
    )

    print("OPENAI STATUS:", r.status_code)
    print("OPENAI RESPONSE:", r.text)

    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

@app.route("/", methods=["GET"])
def home():
    return "Bot running", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        print("INCOMING UPDATE:", data)

        message = data.get("message", {})
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = message.get("text", "")

        print("CHAT ID:", chat_id)
        print("TEXT:", text)

        if chat_id and text:
            reply = ask_openai(text)
            send_message(chat_id, reply)
        else:
            print("No usable text message found.")

        return "ok", 200

    except Exception as e:
        print("WEBHOOK ERROR:", str(e))
        return "error", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
