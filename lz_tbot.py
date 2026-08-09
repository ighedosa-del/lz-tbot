import os, sys, json, time, threading, requests, websocket
from flask import Flask

print("=== LZ TBOT v11 FINAL - CORRECT HOST api.derivws.com ===")
PAT = (os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or "").strip()
APP_ID = (os.getenv("DERIV_APP_IDS","").split(",")[0].strip() or os.getenv("DERIV_APP_ID","").strip() or "341aJK71v75g15Vud3q6w")
print(f"PAT {PAT[:8]}... len={len(PAT)} APP_ID={APP_ID}")

app = Flask(__name__)
@app.route("/")
def home(): return "Bot v11 api.derivws.com - Live"
def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",10000)))
threading.Thread(target=run_flask, daemon=True).start()

BASE = "https://api.derivws.com/trading/v1/options"
HEADERS = {
    "Authorization": f"Bearer {PAT}",
    "x-deriv-app-id": APP_ID,
    "App-Id": APP_ID,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def rest_accounts():
    url = f"{BASE}/accounts"
    print(f"[TRY] GET {url} with Bearer PAT + x-deriv-app-id={APP_ID}")
    r = requests.get(url, headers=HEADERS, timeout=15)
    print(f"[REST] {r.status_code} {r.text[:1000]}")
    if r.status_code==200:
        try:
            j = r.json()
            print(f"[REST OK] {json.dumps(j)[:2000]}")
            return j
        except:
            print("[REST] Not JSON, but status 200")
            return None
    else:
        print(f"[REST FAIL] {r.status_code} {r.text[:500]}")
        if r.status_code==401:
            print("[HINT] PAT expired! Regenerate new PAT in Deriv account -> API tokens")
        return None

def run():
    data = rest_accounts()
    if not data:
        print("[ERR] accounts failed, retry 10s")
        time.sleep(10)
        return
    # data contains accounts and maybe trading urls
    # Try to find DOT account
    accounts = data.get("data") or data.get("accounts") or []
    dot = None
    for acc in accounts:
        print(f"[ACCOUNT] {acc}")
        if "DOT" in str(acc).upper() or "derived" in str(acc).lower():
            dot = acc
    if not dot and accounts:
        dot = accounts[0]
    print(f"[SELECTED] {dot}")
    # If trading URL present, connect WS
    auth_url = None
    if isinstance(dot, dict):
        auth_url = dot.get("authenticated_url") or dot.get("login_url") or dot.get("url")
    if auth_url:
        print(f"[WS OTP] Connecting {auth_url[:120]}...")
        ws = websocket.create_connection(auth_url, timeout=15)
        print("[WS] OTP Connected!")
        while True:
            msg = ws.recv()
            print(f"[WS MSG] {msg[:500]}")
    else:
        # No OTP URL, use direct WS authorize as fallback (works for many PATs)
        print("[WS] No OTP URL, trying direct WS authorize")
        ws = websocket.create_connection(f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}", timeout=15)
        ws.send(json.dumps({"authorize": PAT}))
        resp = json.loads(ws.recv())
        print(f"[WS AUTH] {resp}")
        if "error" in resp:
            print(f"[WS AUTH ERR] {resp['error']['message']}")
            time.sleep(5)
            return
        print("[WS LIVE] Bot authorized! Subscribing balance...")
        ws.send(json.dumps({"balance":1,"subscribe":1}))
        while True:
            print(ws.recv())

while True:
    try:
        run()
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[LOOP ERR] {e} retry 5s")
        time.sleep(5)
