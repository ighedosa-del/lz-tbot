import os, time, json, threading, requests
from flask import Flask, jsonify

APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
PAT = os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or ""
API_BASE = "https://api.derivws.com/trading/v1"

print(f"[BOOT] APP_ID={APP_ID} PAT_len={len(PAT)} pat_start={PAT[:10]}")

def get_dot_account():
    if not PAT.startswith("pat_"):
        print("[ERROR] PAT must start with pat_")
        return None
    url = f"{API_BASE}/options/accounts"
    # FIX: Deriv now requires EXACT header "Deriv-App-ID" (case sensitive message)
    headers = {
        "Authorization": f"Bearer {PAT}",
        "Deriv-App-ID": APP_ID,
        "X-App-ID": APP_ID,
        "x-deriv-app-id": APP_ID,
        "Content-Type": "application/json"
    }
    print(f"[TRY] GET {url} with Deriv-App-ID={APP_ID}")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        print(f"[REST] {resp.status_code} {resp.text[:800]}")
        if resp.status_code == 200:
            data = resp.json()
            accounts = data.get("data", [])
            for acc in accounts:
                print(f"[ACC] {acc}")
            if accounts:
                print(f"[REST OK] Found {len(accounts)} accounts, using {accounts[0]['id']}")
                return accounts[0]
        elif resp.status_code == 401:
            print(f"[FATAL] 401 still - {resp.text[:500]}")
    except Exception as e:
        print(f"[EXC] {e}")
    return None

app = Flask(__name__)
status_info = {"status": "starting"}

@app.route("/")
def home():
    return jsonify(status_info)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

def trading_loop():
    while True:
        acc = get_dot_account()
        if not acc:
            status_info["status"] = "retry_no_account"
            print("[RETRY] No account, retry in 15s...")
            time.sleep(15)
            continue
        status_info["status"] = "live"
        status_info["account"] = acc.get("id")
        print(f"[WS LIVE] Bot authorized! Account {acc.get('id')}")
        while True:
            time.sleep(60)
            print(f"[HEARTBEAT] Live {acc.get('id')} {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    threading.Thread(target=trading_loop, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
