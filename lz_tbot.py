import os
import sys
import time
import json
import threading
import requests
from flask import Flask, jsonify

# --- CONFIG ---
APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
# Support BOTH names: DERIV_PAT and DERIV_TOKEN (you use DERIV_TOKEN)
PAT = os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or ""
API_BASE = "https://api.derivws.com/trading/v1"
WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

print(f"[BOOT] APP_ID={APP_ID}")
print(f"[BOOT] PAT found: {len(PAT) > 20} (len={len(PAT)}) - starts_with_pat={PAT.startswith('pat_')}")

def get_dot_account():
    """Get DOT account via REST - the new Deriv v1 API"""
    if not PAT or not PAT.startswith("pat_"):
        print("[ERROR] PAT invalid! Must start with pat_")
        return None

    url = f"{API_BASE}/options/accounts"
    headers = {
        "Authorization": f"Bearer {PAT}",
        "x-deriv-app-id": APP_ID,
        "Content-Type": "application/json"
    }
    print(f"[TRY] GET {url} with Bearer PAT + x-deriv-app-id={APP_ID[:8]}...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        print(f"[REST] {resp.status_code} {resp.text[:500]}")
        if resp.status_code == 200:
            data = resp.json()
            accounts = data.get("data", [])
            # Find DOT or USD account for options
            for acc in accounts:
                print(f"[ACC] {acc}")
                # Use first trading account
                if acc.get("id"):
                    print(f"[REST OK] Selected account: {acc['id']}")
                    return acc
            if accounts:
                return accounts[0]
        elif resp.status_code == 401:
            print("[FATAL] 401 Invalid token format or expired - generate NEW PAT at https://app.deriv.com/account/personal-access-tokens")
        else:
            print(f"[FATAL] REST failed {resp.status_code}")
    except Exception as e:
        print(f"[EXC] REST error: {e}")
    return None

# Flask keep-alive for Render Web Service (if you use web) + health check
app = Flask(__name__)
status_info = {"status": "starting", "account": None, "last_trade": None}

@app.route("/")
def home():
    return jsonify(status_info)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

def trading_loop():
    while True:
        try:
            acc = get_dot_account()
            if not acc:
                print("[RETRY] No account, retry in 30s...")
                status_info["status"] = "waiting_for_valid_pat"
                time.sleep(30)
                continue

            status_info["status"] = "connected"
            status_info["account"] = acc.get("id")

            # --- Your Deriv WS trading logic here ---
            # For now, just keep alive and log
            print(f"[WS LIVE] Bot authorized for account {acc.get('id')}! Ready to trade.")
            status_info["status"] = "live_authorized"

            # Simulate live loop - replace with your actual websocket trading
            while True:
                time.sleep(60)
                print(f"[HEARTBEAT] Live - Account {acc.get('id')} - {time.strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"[LOOP EXC] {e}")
            time.sleep(10)

if __name__ == "__main__":
    # Start trading in background thread so Flask can serve
    threading.Thread(target=trading_loop, daemon=True).start()
    # Flask for Render
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
