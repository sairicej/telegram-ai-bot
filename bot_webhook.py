import os
import requests
from flask import Flask, request

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

app = Flask(__name__)

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_message(chat_id, text):
    requests.post(
        f"{TELEGRAM_API}/sendMessage",
        json={"chat_id": chat_id, "text": text}
    )

def ask_openai(prompt):

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": "gpt-4.1-mini",
        "messages":[
            {"role":"user","content":prompt}
        ]
    }

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=data
    )

    return r.json()["choices"][0]["message"]["content"]

@app.route("/", methods=["GET"])
def home():
    return "Bot running"

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.json
    message = data.get("message", {})

    chat_id = message["chat"]["id"]
    text = message.get("text","")

    if text:
        reply = ask_openai(text)
        send_message(chat_id, reply)

    return "ok"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
