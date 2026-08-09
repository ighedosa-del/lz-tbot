
import os, asyncio, json, time
import websockets
import numpy as np
import aiohttp

print("=== LZ-TBot v3 FINAL - PAT_ + Alphanumeric App ID ===", flush=True)

TOKEN = os.environ.get("DERIV_TOKEN","").strip()
APP_ID = os.environ.get("DERIV_APP_ID","341aJK71v75g15Vud3q6w").strip()  # Your correct ID
ACCOUNT_ID = os.environ.get("DERIV_ACCOUNT_ID","").strip()
SYMBOL = os.environ.get("SYMBOL","R_75")
STAKE = float(os.environ.get("STAKE","0.35"))

print(f"App ID: {APP_ID}", flush=True)
print(f"Token: {'PAT_'+TOKEN[4:12]+'...' if TOKEN.startswith('PAT_') else TOKEN[:6]}", flush=True)
print(f"Account ID: {ACCOUNT_ID or 'AUTO-DETECT'}", flush=True)

if not TOKEN:
    print("[FATAL] Set DERIV_TOKEN in Render Environment!", flush=True)
    while True: time.sleep(60)

async def get_account_id(session):
    """Try to auto-detect account ID"""
    if ACCOUNT_ID:
        return ACCOUNT_ID
    print("[PAT] Auto-detecting Account ID...", flush=True)
    # Try trading API
    endpoints = [
        "https://api.deriv.com/trading/v1/accounts",
        "https://api.derivws.com/trading/v1/accounts",
        "https://api.deriv.com/trading/v1/options/accounts",
    ]
    for url in endpoints:
        try:
            async with session.get(url, headers={"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {TOKEN}"}) as r:
                txt = await r.text()
                print(f"[PAT] GET {url} -> {r.status} {txt[:300]}", flush=True)
                if r.status==200:
                    data = json.loads(txt)
                    # parse
                    candidates = []
                    if isinstance(data, dict):
                        if "data" in data:
                            d = data["data"]
                            if isinstance(d, list): candidates = d
                            elif isinstance(d, dict) and "accounts" in d: candidates = d["accounts"]
                        elif "accounts" in data: candidates = data["accounts"]
                    if candidates:
                        first = candidates[0]
                        acc_id = first.get("id") or first.get("accountId") or first.get("account_id")
                        print(f"[PAT] Found account: {first} -> ID {acc_id}", flush=True)
                        if acc_id:
                            return acc_id
        except Exception as e:
            print(f"[PAT] Error {url}: {e}", flush=True)
    return None

async def get_ws_url():
    async with aiohttp.ClientSession() as session:
        acc_id = await get_account_id(session)
        if not acc_id:
            print("\n[ERROR] Could not auto-detect DERIV_ACCOUNT_ID", flush=True)
            print("Go to Render -> Environment -> Add DERIV_ACCOUNT_ID", flush=True)
            print("How to find it: In Deriv dashboard, URL is app.deriv.com or check API docs", flush=True)
            print("Or try using your loginid like CR1234567", flush=True)
            # Let user set manually, keep retrying
            raise ValueError("Need DERIV_ACCOUNT_ID")
        
        print(f"[PAT] Requesting OTP for {acc_id}...", flush=True)
        otp_url = f"https://api.derivws.com/trading/v1/options/accounts/{acc_id}/otp"
        headers = {"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
        async with session.post(otp_url, headers=headers, json={}) as r:
            txt = await r.text()
            print(f"[PAT] OTP Response {r.status}: {txt}", flush=True)
            if r.status!=200:
                raise RuntimeError(f"OTP failed: {txt}")
            data = json.loads(txt)
            # Expected: {"data": {"url": "wss://...?otp=...", "otp": "..."}} or similar
            ws_url = None
            if "data" in data:
                d = data["data"]
                ws_url = d.get("url") or d.get("websocket_url") or d.get("ws_url")
                if not ws_url and "otp" in d:
                    # construct? But server should give full url
                    otp = d["otp"]
                    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}&otp={otp}"
            ws_url = ws_url or data.get("url")
            if not ws_url:
                raise RuntimeError(f"No WS URL in {data}")
            print(f"[PAT] ✓ Got WS URL: {ws_url[:100]}...", flush=True)
            return ws_url

# Trading brain
ticks=[]
def calc_signal(prices):
    if len(prices)<200:
        return None, f"Collecting {len(prices)}/200"
    arr=np.array(prices)
    ema20=np.mean(arr[-20:]); ema50=np.mean(arr[-50:]); ema200=np.mean(arr[-200:])
    deltas=np.diff(arr[-15:])
    gains=deltas[deltas>0].sum(); losses=-deltas[deltas<0].sum()
    rsi=100-(100/(1+gains/(losses+0.001)))
    up=ema20>ema50>ema200; down=ema20<ema50<ema200
    if up and 40<rsi<58:
        return "CALL", f"UP EMA{ema20:.1f}>{ema50:.1f}>{ema200:.1f} RSI{rsi:.1f}"
    if down and 42<rsi<62:
        return "PUT", f"DOWN EMA{ema20:.1f}<{ema50:.1f}<{ema200:.1f} RSI{rsi:.1f}"
    return None, f"Wait EMA20{ema20:.1f} EMA50{ema50:.1f} RSI{rsi:.1f}"

async def run():
    while True:
        try:
            ws_url = await get_ws_url()
            print(f"Connecting WS...", flush=True)
            async with websockets.connect(ws_url) as ws:
                print(f"✓ CONNECTED! No 1006! Auth via OTP successful", flush=True)
                # Subscribe balance to confirm
                await ws.send(json.dumps({"balance":1}))
                bal = json.loads(await ws.recv())
                print(f"Balance: {bal}", flush=True)
                
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe":1}))
                print(f"✓ Listening {SYMBOL} Stake {STAKE}", flush=True)
                while True:
                    msg=json.loads(await ws.recv())
                    if "tick" in msg:
                        p=float(msg["tick"]["quote"])
                        ticks.append(p)
                        if len(ticks)>300: ticks.pop(0)
                        if len(ticks)%10==0:
                            print(f"Tick {len(ticks)} {p}", flush=True)
                        sig, reason = calc_signal(ticks)
                        if sig:
                            print(f"[SIGNAL] {sig} | {reason}", flush=True)
                            prop={"proposal":1,"amount":STAKE,"basis":"stake","contract_type":sig,"currency":"USD","duration":1,"duration_unit":"m","symbol":SYMBOL}
                            await ws.send(json.dumps(prop))
                            pr=json.loads(await ws.recv())
                            if "proposal" in pr:
                                await ws.send(json.dumps({"buy": pr["proposal"]["id"], "price": STAKE}))
                                buy=json.loads(await ws.recv())
                                print(f"→ TRADE {buy}", flush=True)
                        else:
                            if len(ticks)%20==0:
                                print(f"[{time.strftime('%H:%M:%S')}] {reason}", flush=True)
        except Exception as e:
            import traceback
            print(f"[RETRY] {e}", flush=True)
            traceback.print_exc()
            await asyncio.sleep(10)

asyncio.run(run())
