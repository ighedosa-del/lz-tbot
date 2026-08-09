import os, asyncio, json, time, threading, collections, datetime
import aiohttp
import websockets
from flask import Flask, jsonify, render_template_string

# === CONFIG FROM ENV ===
DERIV_TOKEN = os.getenv("DERIV_TOKEN", "")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
DERIV_ACCOUNT_ID = os.getenv("DERIV_ACCOUNT_ID", "DOT93742818")
SYMBOL = "R_75"
STAKE = 0.35
PORT = int(os.getenv("PORT", 10000))

# === SHARED STATE FOR DASHBOARD ===
state = {
    "status": "Starting...",
    "connected": False,
    "balance": 0,
    "currency": "USD",
    "account_id": DERIV_ACCOUNT_ID,
    "symbol": SYMBOL,
    "stake": STAKE,
    "ticks": collections.deque(maxlen=200),
    "last_price": 0,
    "collecting": "0/200",
    "last_signal": "None",
    "trades": collections.deque(maxlen=50),
    "logs": collections.deque(maxlen=100),
    "uptime": "",
    "start_time": time.time()
}

def log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    state["logs"].appendleft(line)

# === REST HELPERS (v5 Fixed) ===
async def rest_get_accounts(session):
    urls = [
        f"https://api.derivws.com/trading/v1/accounts",
        f"https://api.deriv.com/trading/v1/accounts",
        f"https://api.derivws.com/trading/v1/options/accounts"
    ]
    for url in urls:
        try:
            log(f"[REST] TRY GET {url}")
            async with session.get(url, headers={"Authorization": f"Bearer {DERIV_TOKEN}", "App-Id": DERIV_APP_ID}, timeout=10) as r:
                text = await r.text()
                if "<!DOCTYPE" in text or "<html" in text:
                    log(f"[REST] {url} returned HTML, skipping")
                    continue
                data = json.loads(text)
                accounts = data.get("data") or data.get("accounts") or []
                if accounts:
                    log(f"[REST] ✓ Found accounts: {[a.get('account_id') for a in accounts]}")
                    return accounts
        except Exception as e:
            log(f"[REST] {url} failed {e}")
    return []

async def rest_get_otp(session, account_id):
    urls = [
        f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp",
        f"https://api.derivws.com/trading/v1/accounts/{account_id}/otp",
        f"https://api.deriv.com/trading/v1/options/accounts/{account_id}/otp",
    ]
    for url in urls:
        try:
            log(f"[REST] POST OTP {url}")
            async with session.post(url, headers={"Authorization": f"Bearer {DERIV_TOKEN}", "App-Id": DERIV_APP_ID}, timeout=10) as r:
                text = await r.text()
                if "<!DOCTYPE" in text:
                    continue
                j = json.loads(text)
                log(f"[REST] OTP 200: {text[:200]}")
                otp_url = j.get("data", {}).get("url") or j.get("url")
                if otp_url:
                    log(f"[REST] ✓ OTP URL {otp_url[:80]}...")
                    return otp_url
        except Exception as e:
            log(f"[REST] OTP {url} err {e}")
    return None

# === TRADING LOOP ===
async def trading_loop():
    log(f"=== LZ-TBot v5.1 Dashboard - App {DERIV_APP_ID} ===")
    state["status"] = "Connecting to Deriv..."
    async with aiohttp.ClientSession() as session:
        # 1. Get accounts
        accounts = await rest_get_accounts(session)
        if not accounts:
            state["status"] = "Failed to get accounts - check token"
            log("[MAIN] No accounts found")
            return
        # Pick requested account
        chosen = None
        for a in accounts:
            if a.get("account_id") == DERIV_ACCOUNT_ID:
                chosen = a
                break
        if not chosen:
            chosen = accounts[0]
        account_id = chosen.get("account_id")
        state["account_id"] = account_id
        state["balance"] = float(chosen.get("balance", 0))
        state["currency"] = chosen.get("currency", "USD")
        log(f"[MAIN] Using {account_id}")
        
        # 2. Get OTP WS URL
        ws_url = await rest_get_otp(session, account_id)
        if not ws_url:
            state["status"] = "Failed OTP"
            log("[MAIN] OTP failed")
            return
        
        # 3. Connect WS
        log("[WS] Connecting...")
        try:
            async with websockets.connect(ws_url, ping_interval=20) as ws:
                state["connected"] = True
                state["status"] = f"LIVE Listening {SYMBOL} Stake {STAKE}"
                log("✓ CONNECTED - No 1006!")
                log(f"✓ LIVE Listening {SYMBOL} Stake {STAKE}")
                log(f"Balance: {chosen}")

                # Subscribe to ticks
                await ws.send(json.dumps({"ticks": SYMBOL}))
                
                tick_count = 0
                prices = collections.deque(maxlen=200)
                
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        # Tick
                        if "tick" in data:
                            price = float(data["tick"]["quote"])
                            prices.append(price)
                            state["last_price"] = price
                            state["ticks"].appendleft({"time": time.time(), "price": price})
                            tick_count += 1
                            if tick_count % 10 == 0:
                                log(f"Tick {tick_count} {price}")
                            state["collecting"] = f"{len(prices)}/200"
                            
                            if len(prices) >= 20 and tick_count % 10 == 0:
                                # Simple EMA crossover demo signal (replace with your strategy)
                                ema_short = sum(list(prices)[-10:]) / 10
                                ema_long = sum(list(prices)[-20:]) / 20
                                signal = "CALL" if ema_short > ema_long else "PUT"
                                state["last_signal"] = f"{signal} | EMA10 {ema_short:.2f} > EMA20 {ema_long:.2f}" if signal=="CALL" else f"{signal} | EMA10 {ema_short:.2f} < EMA20 {ema_long:.2f}"
                                if tick_count % 20 == 0:
                                    log(f"[05:29:42] Collecting {len(prices)}/200 - {state['last_signal']}")

                        # Balance / proposal
                        if "balance" in data:
                            state["balance"] = float(data["balance"]["balance"])
                        
                        # Fake trade log for demo - replace with real buy logic
                        # When you have 200 ticks, here you would call proposal/buy
                        
                    except Exception as e:
                        log(f"[WS] msg err {e} {msg[:100]}")

        except Exception as e:
            state["connected"] = False
            state["status"] = f"Disconnected: {e}"
            log(f"[WS] Disconnected {e}")
            await asyncio.sleep(5)
            # Auto reconnect loop
            asyncio.create_task(trading_loop())

def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(trading_loop())

# Start bot in background
threading.Thread(target=start_bot_thread, daemon=True).start()

# === FLASK DASHBOARD ===
app = Flask(__name__)

DASH_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LZ-TBot Dashboard</title>
<style>
body{background:#0b0e14;color:#e6e6e6;font-family:Inter,system-ui;padding:16px}
.card{background:#151a25;border-radius:16px;padding:16px;margin-bottom:12px;border:1px solid #222}
.badge{padding:4px 10px;border-radius:20px;font-size:12px;font-weight:700}
.live{background:#00d18f;color:#001} .dead{background:#ff4560;color:#fff}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
h1{font-size:22px;margin:0 0 8px}
.price{font-size:32px;font-weight:800}
small{color:#8a8fa3}
.log{font-family:monospace;font-size:12px;background:#0e121b;padding:8px;border-radius:8px;max-height:300px;overflow:auto;white-space:pre-wrap}
</style>
<script>
setInterval(async()=>{
  let r=await fetch('/api/status'); let s=await r.json();
  document.getElementById('status').innerText=s.status;
  document.getElementById('bal').innerText=s.balance+' '+s.currency;
  document.getElementById('price').innerText=s.last_price;
  document.getElementById('acc').innerText=s.account_id;
  document.getElementById('collect').innerText=s.collecting;
  document.getElementById('signal').innerText=s.last_signal;
  document.getElementById('conn').className='badge '+(s.connected?'live':'dead');
  document.getElementById('conn').innerText=s.connected?'● CONNECTED - No 1006!':'○ DISCONNECTED';
  document.getElementById('logs').innerText=s.logs.join('\\n');
},1000)
</script>
</head>
<body>
<h1>LZ-TBot v5.1 <span id="conn" class="badge dead">Connecting...</span></h1>
<div class="grid">
  <div class="card"><small>Balance</small><div id="bal" class="price">--</div><small id="acc"></small></div>
  <div class="card"><small>{{symbol}} Price</small><div id="price" class="price">--</div><small id="collect"></small></div>
</div>
<div class="card"><small>Status</small><div id="status" style="font-weight:700;margin-top:6px">Starting...</div><div style="margin-top:8px"><small>Last Signal</small><div id="signal">None</div></div></div>
<div class="card"><small>Live Logs</small><div id="logs" class="log">Loading...</div></div>
<div class="card"><small>App</small><div>App ID {{app_id}} | Stake ${{stake}} | Auto Reconnect ON</div></div>
</body>
</html>
"""

@app.route("/")
def dash():
    return render_template_string(DASH_HTML, symbol=SYMBOL, app_id=DERIV_APP_ID, stake=STAKE)

@app.route("/api/status")
def api():
    # convert deques to list
    out = dict(state)
    out["ticks"] = list(out["ticks"])[:10]
    out["trades"] = list(out["trades"])
    out["logs"] = list(out["logs"])
    out["uptime"] = str(datetime.timedelta(seconds=int(time.time() - out["start_time"])))
    return jsonify(out)

@app.route("/health")
def health():
    return "ok", 200

if __name__ == "__main__":
    log(f"[WEB] Starting dashboard on :{PORT}")
    app.run(host="0.0.0.0", port=PORT)
