import os, sys, time, threading, json, requests
from flask import Flask, jsonify

# Force unbuffered logs for Render
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
PAT = (os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or "").strip()

print(f"[BOOT] APP_ID={APP_ID}", flush=True)
print(f"[BOOT] PAT len={len(PAT)} preview={PAT[:12]}...{PAT[-6:] if len(PAT)>6 else ''} is_pat={PAT.startswith('pat_')}", flush=True)
print(f"[BOOT] NEW FLOW: REST accounts -> REST OTP -> Authenticated WS", flush=True)

API_BASE = "https://api.derivws.com/trading/v1"

def get_authenticated_ws_url():
    """NEW DERIV PAT FLOW - Your discovery!"""
    headers = {
        "Authorization": f"Bearer {PAT}",
        "Deriv-App-ID": APP_ID,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # STEP 1: GET ACCOUNTS
    print(f"\n[STEP1] GET {API_BASE}/options/accounts", flush=True)
    print(f"[STEP1] Header Deriv-App-ID={APP_ID} Bearer={PAT[:10]}...{PAT[-4:]}", flush=True)
    
    try:
        r = requests.get(f"{API_BASE}/options/accounts", headers=headers, timeout=20)
        print(f"[STEP1] status={r.status_code}", flush=True)
        print(f"[STEP1] body={r.text[:1000]}", flush=True)

        if r.status_code == 401:
            print(f"[FATAL] 401 Invalid PAT - regenerate token", flush=True)
            return None, None
        if r.status_code == 500:
            print(f"[FATAL] 500 Deriv server error - check App ID", flush=True)
            return None, None
        if r.status_code != 200:
            print(f"[ERR] Unexpected {r.status_code}", flush=True)
            return None, None

        data = r.json()
        accounts = data.get("data", [])
        print(f"[STEP1 OK] Found {len(accounts)} accounts: {accounts}", flush=True)
        
        if not accounts:
            print(f"[FATAL] No accounts found - check token scopes", flush=True)
            return None, None

        # Use first account (or prefer demo)
        account = accounts[0]
        for acc in accounts:
            if "demo" in str(acc).lower() or "DOT" in str(acc.get("id","")) or "DOT" in str(acc.get("account_id","")):
                account = acc
                break
        
        account_id = account.get("account_id") or account.get("id")
        print(f"[STEP1] Selected account_id={account_id}", flush=True)

        # STEP 2: GET OTP WS URL
        print(f"\n[STEP2] POST {API_BASE}/options/accounts/{account_id}/otp", flush=True)
        r2 = requests.post(f"{API_BASE}/options/accounts/{account_id}/otp", headers=headers, timeout=20)
        print(f"[STEP2] status={r2.status_code}", flush=True)
        print(f"[STEP2] body={r2.text[:1000]}", flush=True)

        if r2.status_code != 200:
            print(f"[FATAL] OTP failed: {r2.text[:500]}", flush=True)
            return None, None

        j2 = r2.json()
        ws_url = j2.get("data", {}).get("url") or j2.get("data", {}).get("ws_url") or j2.get("url")
        
        if not ws_url:
            print(f"[FATAL] No ws url in response", flush=True)
            return None, None

        print(f"[STEP2 OK] ws_url={ws_url[:120]}...", flush=True)
        return ws_url, account_id

    except Exception as e:
        print(f"[STEP EXC] {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None, None

def ws_trading_loop(ws_url, account_id):
    """STEP 3: Connect to OTP-authenticated WS"""
    import websocket
    try:
        print(f"\n[STEP3] Connecting to authenticated WS...", flush=True)
        print(f"[STEP3] URL={ws_url[:100]}...", flush=True)
        
        ws = websocket.create_connection(ws_url, timeout=20)
        print(f"[LIVE] ✅ CONNECTED! Pre-authenticated! Account {account_id}", flush=True)
        print(f"[LIVE] No authorize message needed - OTP already auth'd!", flush=True)

        # Test - subscribe to ticks
        ws.send(json.dumps({"ticks": "R_100", "subscribe": 1}))
        print(f"[LIVE] Subscribed to ticks", flush=True)

        # Your trading strategy here
        while True:
            raw = ws.recv()
            msg = json.loads(raw)
            
            if "tick" in msg:
                tick = msg["tick"]
                print(f"[TICK] {tick.get('symbol')} {tick.get('quote')} {tick.get('epoch')}", flush=True)
            
            if "error" in msg:
                print(f"[WS ERR] {msg['error']}", flush=True)

    except Exception as e:
        print(f"[WS EXC] {e}", flush=True)
        raise

app = Flask(__name__)
status_info = {"status": "starting", "account": None, "flow": "REST->OTP->WS"}

@app.route("/")
def home():
    return jsonify(status_info)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

def main_loop():
    while True:
        try:
            ws_url, account_id = get_authenticated_ws_url()
            if not ws_url:
                status_info["status"] = "retrying_auth"
                print(f"[RETRY] Auth failed, retry 15s", flush=True)
                time.sleep(15)
                continue

            status_info["status"] = "live"
            status_info["account"] = account_id
            ws_trading_loop(ws_url, account_id)

        except Exception as e:
            print(f"[MAIN LOOP EXC] {e}", flush=True)
            status_info["status"] = "reconnecting"
            time.sleep(10)

if __name__ == "__main__":
    # Add PYTHONUNBUFFERED env in Render for instant logs
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    print(f"[FLASK] Starting on 0.0.0.0:{port}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
