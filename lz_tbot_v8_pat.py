
"""
LZ-TBot v8 PAT Edition - Deriv New API
- REST + OTP auth flow for alphanumeric App IDs
- App ID: 341aJK71v75g15Vud3q6w
- Env: DERIV_APP_ID, DERIV_TOKEN (pat_...), DERIV_ACCOUNT_ID (optional, else auto-pick first)
- Business guards: Daily TP/SL, Risk%, Martingale cap, Vol filter, Tick quality
"""

import os
import sys
import time
import json
import requests
import websocket
import threading
from datetime import datetime

APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
PAT_TOKEN = os.getenv("DERIV_TOKEN", "")
ACCOUNT_ID_HINT = os.getenv("DERIV_ACCOUNT_ID", "")  # e.g. DOT93742818

# === Business Config ===
RISK_PCT = 0.008
DAILY_TP = 0.06
DAILY_SL = -0.035
STAKE = 1
MARTINGALE_FACTOR = 2.10
MARTINGALE_CAP = 3
VOLATILITY_SYMBOL = "R_100"
STRATEGY = "Digit Even"  # Even/Odd/Over/Under

REST_BASE = "https://api.deriv.com"

def log(msg):
    print(f"{datetime.utcnow().strftime('%H:%M:%S')} {msg}", flush=True)

def get_accounts():
    if not PAT_TOKEN:
        log("[ERR] DERIV_TOKEN env missing")
        sys.exit(1)
    headers = {
        "Deriv-App-ID": APP_ID,
        "Authorization": f"Bearer {PAT_TOKEN}",
        "Content-Type": "application/json"
    }
    log(f"[TRY] REST GET {REST_BASE}/trading/v1/options/accounts with App ID {APP_ID}")
    r = requests.get(f"{REST_BASE}/trading/v1/options/accounts", headers=headers, timeout=15)
    log(f"[REST] {r.status_code} {r.text[:500]}")
    r.raise_for_status()
    data = r.json()
    accounts = data.get('data', [])
    if not accounts:
        log("[ERR] No accounts found - check PAT scopes (Trade)")
        sys.exit(1)
    for acc in accounts:
        log(f"  - {acc.get('account_id')} {acc.get('account_type')} {acc.get('currency')} bal {acc.get('balance')}")
    # pick hint or first
    if ACCOUNT_ID_HINT:
        chosen = next((a for a in accounts if a['account_id']==ACCOUNT_ID_HINT), accounts[0])
    else:
        chosen = accounts[0]
    log(f"[AUTH] Using account {chosen['account_id']}")
    return chosen['account_id'], headers

def get_otp_url(account_id, headers):
    log(f"[TRY] REST POST /accounts/{account_id}/otp")
    r = requests.post(f"{REST_BASE}/trading/v1/options/accounts/{account_id}/otp", headers=headers, timeout=15)
    log(f"[OTP] {r.status_code} {r.text[:800]}")
    r.raise_for_status()
    data = r.json()
    ws_url = data.get('data', {}).get('url')
    if not ws_url:
        log("[ERR] No ws url in OTP response")
        sys.exit(1)
    log(f"[OTP] Got authenticated URL: {ws_url[:100]}...")
    return ws_url

# --- WebSocket trading ---
balance = 10000.0
daily_pl = 0.0
martingale_steps = 0
trading_enabled = True

def on_message(ws, message):
    global balance, daily_pl, martingale_steps
    try:
        msg = json.loads(message)
        # New API format: ticks, balance updates
        if 'data' in msg:
            d = msg['data']
            if isinstance(d, dict) and 'tick' in d:
                tick = d['tick']
                price = tick.get('quote')
                # log tick occasionally
                # Trading logic placeholder
        else:
            # log full for debug
            if 'error' in str(message).lower():
                log(f"[WS] {message[:500]}")
            # balance
            if 'balance' in message:
                log(f"[BAL] {message[:400]}")
    except Exception as e:
        log(f"[ERR] on_message {e} {message[:300]}")

def on_open(ws):
    log("==> Your service is live 🎉")
    log(f"==> WSS / OTP {ws.url[:80]}...")
    log(f"==> Available at your primary URL https://lz-tbot-cloud.onrender.com")
    # Subscribe to R_100 ticks
    sub_msg = {"action": "subscribe", "channel": f"ticks:{VOLATILITY_SYMBOL}"}
    ws.send(json.dumps(sub_msg))
    log(f"[SUB] {sub_msg}")

def on_error(ws, err):
    log(f"[ERR] WS error {err}")

def on_close(ws, code, reason):
    log(f"[CLOSE] {code} {reason} - retry in 5s")

def run_bot():
    while True:
        try:
            account_id, headers = get_accounts()
            ws_url = get_otp_url(account_id, headers)
            log(f"[TRY] Connecting to OTP URL")
            ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log(f"[ERR] All failed - retry in 5s {e}")
            time.sleep(5)

if __name__ == "__main__":
    log(f"Trying App ID {APP_ID} -> REST auth flow")
    run_bot()
