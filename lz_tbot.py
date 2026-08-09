import os, sys, json, time, threading, requests, traceback
from flask import Flask
import websocket

print("=== LZ TBOT v9 FIX - PAT + CORRECT ENDPOINT ===")

# --- ENV ---
PAT = os.getenv("DERIV_PAT") or os.getenv("DERIV_TOKEN") or ""
APP_IDS = [s.strip() for s in os.getenv("DERIV_APP_IDS","").split(",") if s.strip()]
if not APP_IDS:
    APP_IDS = [os.getenv("DERIV_APP_ID","").strip()]

if not PAT:
    print("[FATAL] No DERIV_PAT set in Render env!")
    sys.exit(1)
if not APP_IDS or not APP_IDS[0]:
    print("[FATAL] No APP_ID set!")
    sys.exit(1)

print(f"PAT: {PAT[:6]}...{PAT[-4:]} len={len(PAT)}")
print(f"APP_IDS: {APP_IDS}")

# Keep Render alive
app = Flask(__name__)
@app.route("/")
def home(): return "Bot v9 Running - PAT Fixed Endpoint"
def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",10000)))
threading.Thread(target=run_flask, daemon=True).start()

# Correct REST endpoints to try (Deriv PAT API)
ENDPOINTS = [
    "https://api.deriv.com/trading/v1/options/accounts",
    "https://api.deriv.com/api/trading/v1/options/accounts",
    "https://api.deriv.com/trading/v1/accounts",
]

HEADERS_BASE = {
    "Authorization": f"Bearer {PAT}",
    "Accept": "application/json",
    "User-Agent": "LZ-TBOT/9.0",
}

def try_rest():
    for base_url in ENDPOINTS:
        for app_id in APP_IDS:
            url = base_url
            # Some endpoints need app_id as query
            headers = {**HEADERS_BASE, "App-Id": app_id, "X-App-Id": app_id}
            params = {"app_id": app_id}
            try:
                print(f"[TRY] GET {url} AppID={app_id}")
                r = requests.get(url, headers=headers, params=params, timeout=15)
                print(f"[REST] {r.status_code} {r.text[:200]}")
                if r.status_code==200 and r.text.strip().startswith("{"):
                    data = r.json()
                    print(f"[REST OK] {json.dumps(data)[:500]}")
                    # Find DOT account
                    accounts = data.get("data") or data.get("accounts") or []
                    if accounts:
                        for acc in accounts:
                            if "DOT" in str(acc).upper() or "derived" in str(acc).lower():
                                print(f"[FOUND DOT] {acc}")
                                return acc
                        return accounts[0]
                    # Also handle OTP url flow
                    if "authenticated_url" in str(data).lower():
                        return data
                elif "<!DOCTYPE" in r.text:
                    print("[ERR] Got HTML not JSON - endpoint is website fallback, trying next...")
                    continue
            except Exception as e:
                print(f"[REST ERR] {e}")
                traceback.print_exc()
    return None

def ws_trading(authenticated_ws_url=None):
    # Fallback to websocket trading if we got OTP url
    # Deriv PAT flow: REST returns trading websocket URL with OTP
    # Use that URL to connect
    if not authenticated_ws_url:
        print("[WS] No OTP url, trying direct WS with PAT")
        # Try direct WS authorize
        ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_IDS[0]}"
        try:
            ws = websocket.create_connection(ws_url, timeout=10)
            ws.send(json.dumps({"authorize": PAT}))
            resp = json.loads(ws.recv())
            print(f"[WS AUTH] {resp}")
            if "error" in resp:
                print(f"[WS AUTH FAIL] {resp['error']}")
                return False
            print("[WS] Authorized OK - bot would trade here")
            # Keep alive loop
            while True:
                time.sleep(30)
                ws.send(json.dumps({"ping":1}))
                print("[WS] ping", ws.recv())
        except Exception as e:
            print(f"[WS ERR] {e}")
            traceback.print_exc()
            return False
    else:
        print(f"[WS] Connecting to OTP URL: {authenticated_ws_url[:80]}...")
        try:
            ws = websocket.create_connection(authenticated_ws_url, timeout=15)
            print("[WS] Connected to trading WS!")
            while True:
                time.sleep(30)
                ws.send(json.dumps({"ping":1}))
                print(ws.recv())
        except Exception as e:
            print(f"[WS OTP ERR] {e}")
            return False

# MAIN LOOP
while True:
    try:
        acc = try_rest()
        if acc:
            print(f"[MAIN] Got account data: {acc}")
            # Extract OTP url if present
            otp_url = None
            if isinstance(acc, dict):
                otp_url = acc.get("authenticated_url") or acc.get("trading_url") or acc.get("url")
                if not otp_url:
                    # nested
                    for v in acc.values():
                        if isinstance(v,str) and "wss://" in v:
                            otp_url=v
            ws_trading(otp_url)
        else:
            print("[ERR] All REST endpoints failed - retry in 10s")
            time.sleep(10)
    except Exception as e:
        print(f"[LOOP ERR] {e}")
        time.sleep(5)
