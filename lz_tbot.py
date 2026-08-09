import os, time, threading, requests
from flask import Flask, jsonify

APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
PAT = (os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or "").strip()
API_BASE = "https://api.derivws.com/trading/v1"

print(f"[BOOT] APP_ID={APP_ID}")
print(f"[BOOT] PAT len={len(PAT)} starts_pat={PAT.startswith('pat_')} preview={PAT[:12]}...{PAT[-6:] if len(PAT)>10 else ''}")

def get_dot_account():
    if not PAT or not PAT.startswith("pat_"):
        print(f"[ERROR] PAT missing or bad format! Must start with pat_ got: {PAT[:20]}")
        return None

    url = f"{API_BASE}/options/accounts"
    headers = {
        "Authorization": f"Bearer {PAT}",
        "Deriv-App-ID": APP_ID,  # REQUIRED by Deriv for PAT
        "X-App-ID": APP_ID,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Masked header log for debug
    print(f"[TRY] GET {url} with Deriv-App-ID={APP_ID} PAT={PAT[:10]}...{PAT[-4:]}")

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        
        # 1. Log actual status + body + content-type (your debug idea)
        print(f"[RESP] status={resp.status_code} content-type={resp.headers.get('content-type','')}")
        print(f"[RESP] body={resp.text[:800]}")

        # 2. Handle 401 specifically
        if resp.status_code == 401:
            print(f"[FATAL] 401 Invalid or expired token - Generate NEW PAT at https://app.deriv.com/account/personal-access-tokens")
            print(f"[FATAL] Check: PAT expired? Revoked? Scopes missing? Need Trading + Admin scopes!")
            return None

        # 3. Check content-type before JSON
        if 'application/json' not in resp.headers.get('content-type',''):
            print(f"[ERR] Not JSON response: {resp.text[:500]}")
            return None

        if resp.status_code == 200:
            data = resp.json()
            accounts = data.get("data", [])
            print(f"[REST OK] Got {len(accounts)} accounts")
            for acc in accounts:
                print(f"[ACC] {acc}")
            if accounts:
                return accounts[0]
        else:
            print(f"[ERR] Unexpected status {resp.status_code}")

    except Exception as e:
        print(f"[EXC] Request failed: {e}")
    
    return None

app = Flask(__name__)
status_info = {"status": "starting", "account": None}

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
                status_info["status"] = "waiting_valid_pat"
                print("[RETRY] No account, retry in 15s - Generate NEW PAT if 401 keeps happening")
                time.sleep(15)
                continue

            status_info["status"] = "live"
            status_info["account"] = acc.get("id")
            print(f"[WS LIVE] Bot authorized! Account {acc.get('id')} - READY TO TRADE!")

            while True:
                time.sleep(60)
                print(f"[HEARTBEAT] Live {acc.get('id')} {time.strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"[LOOP EXC] {e}")
            time.sleep(10)

if __name__ == "__main__":
    threading.Thread(target=trading_loop, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
