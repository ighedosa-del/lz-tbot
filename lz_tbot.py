
import os, asyncio, json, time
import websockets, aiohttp, numpy as np

print("=== LZ-TBot v5 - Fixed Endpoints - App 341aJK71v75g15Vud3q6w ===", flush=True)

TOKEN = os.environ.get("DERIV_TOKEN","").strip()
APP_ID = "341aJK71v75g15Vud3q6w"
ACCOUNT_ID = os.environ.get("DERIV_ACCOUNT_ID","").strip() or "DOT93742818"
SYMBOL = os.environ.get("SYMBOL","R_75")
STAKE = float(os.environ.get("STAKE","0.35"))

print(f"App ID: {APP_ID} | Account: {ACCOUNT_ID}", flush=True)

if not TOKEN:
    print("FATAL: DERIV_TOKEN missing")
    while True: time.sleep(60)

# Try endpoints in order until JSON with account_id
ACCOUNT_ENDPOINTS = [
    "https://api.derivws.com/trading/v1/accounts",
    "https://api.deriv.com/trading/v1/options/accounts",
    "https://api.deriv.com/trading/v1/accounts",
    "https://api.derivws.com/trading/v1/options/accounts",
]

async def get_accounts(session):
    if ACCOUNT_ID and ACCOUNT_ID.startswith(("CR","VR","DO","RO")):
        # If user gave valid looking ID, trust it if we want to skip detection, but still try to validate
        print(f"[REST] Using configured ACCOUNT_ID {ACCOUNT_ID} directly (skip auto-detect if needed)", flush=True)
        # We will still try to fetch to validate, but if all endpoints fail HTML, return this ID
        pass

    for url in ACCOUNT_ENDPOINTS:
        try:
            print(f"[REST] TRY GET {url}", flush=True)
            async with session.get(url, headers={"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {TOKEN}"}) as r:
                txt = await r.text()
                # Skip HTML
                if txt.strip().startswith("<!DOCTYPE") or txt.strip().startswith("<html"):
                    print(f"[REST] {url} -> HTML (skip)", flush=True)
                    continue
                print(f"[REST] {url} -> {r.status} {txt[:800]}", flush=True)
                if r.status != 200:
                    continue
                data = json.loads(txt)
                # Parse possible formats
                accounts = []
                if isinstance(data, dict):
                    if "data" in data:
                        d = data["data"]
                        if isinstance(d, list): accounts = d
                        elif isinstance(d, dict) and "accounts" in d: accounts = d["accounts"]
                        else: accounts = [d] if isinstance(d, dict) else []
                    elif "accounts" in data:
                        accounts = data["accounts"]
                    elif "account_id" in data or "id" in data:
                        accounts = [data]
                elif isinstance(data, list):
                    accounts = data
                
                # Filter valid
                valid = []
                for a in accounts:
                    if not isinstance(a, dict): continue
                    aid = a.get("account_id") or a.get("id") or a.get("accountId")
                    if aid:
                        valid.append({"account_id": aid, "raw": a})
                if valid:
                    print(f"[REST] ✓ Found accounts: {[x['account_id'] for x in valid]}", flush=True)
                    return valid
        except Exception as e:
            print(f"[REST] {url} error: {e}", flush=True)
            continue
    
    # Fallback: if all failed, use configured ACCOUNT_ID if present
    if ACCOUNT_ID:
        print(f"[REST] All endpoints HTML/failed, fallback to configured {ACCOUNT_ID}", flush=True)
        return [{"account_id": ACCOUNT_ID, "raw": {}}]
    
    raise RuntimeError("No accounts found - all endpoints returned HTML")

async def get_otp_url(session, acc_id):
    endpoints = [
        f"https://api.derivws.com/trading/v1/options/accounts/{acc_id}/otp",
        f"https://api.deriv.com/trading/v1/options/accounts/{acc_id}/otp",
    ]
    for url in endpoints:
        try:
            print(f"[REST] POST OTP {url}", flush=True)
            async with session.post(url, headers={"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}, json={}) as r:
                txt = await r.text()
                if txt.strip().startswith("<!DOCTYPE"):
                    print(f"[REST] OTP {url} -> HTML skip", flush=True)
                    continue
                print(f"[REST] OTP {r.status}: {txt[:1000]}", flush=True)
                if r.status != 200:
                    continue
                data = json.loads(txt)
                ws_url = None
                if "data" in data:
                    ws_url = data["data"].get("url") or data["data"].get("ws_url")
                ws_url = ws_url or data.get("url")
                if ws_url:
                    print(f"[REST] ✓ OTP URL {ws_url[:100]}...", flush=True)
                    return ws_url
        except Exception as e:
            print(f"[REST] OTP {url} error {e}", flush=True)
            continue
    raise RuntimeError(f"OTP failed for {acc_id}")

ticks=[]
def calc_signal(prices):
    if len(prices)<200: return None, f"Collecting {len(prices)}/200"
    arr=np.array(prices)
    ema20=np.mean(arr[-20:]); ema50=np.mean(arr[-50:]); ema200=np.mean(arr[-200:])
    d=np.diff(arr[-15:]); g=d[d>0].sum(); l=-d[d<0].sum()
    rsi=100-(100/(1+g/(l+0.001)))
    up=ema20>ema50>ema200; down=ema20<ema50<ema200
    if up and 40<rsi<58: return "CALL", f"UP EMA{ema20:.1f}>{ema50:.1f}>{ema200:.1f} RSI{rsi:.1f}"
    if down and 42<rsi<62: return "PUT", f"DOWN EMA{ema20:.1f}<{ema50:.1f}<{ema200:.1f} RSI{rsi:.1f}"
    return None, f"WAIT EMA20{ema20:.1f} EMA50{ema50:.1f} RSI{rsi:.1f}"

async def trading_loop(ws):
    await ws.send(json.dumps({"ticks": SYMBOL, "subscribe":1}))
    await ws.send(json.dumps({"balance":1, "subscribe":1}))
    print(f"✓ LIVE Listening {SYMBOL} Stake {STAKE}", flush=True)
    while True:
        msg=json.loads(await ws.recv())
        if "tick" in msg:
            p=float(msg["tick"]["quote"]); ticks.append(p)
            if len(ticks)>300: ticks.pop(0)
            if len(ticks)%10==0: print(f"Tick {len(ticks)} {p}", flush=True)
            sig, reason = calc_signal(ticks)
            if sig:
                print(f"[SIGNAL] {sig} | {reason}", flush=True)
                prop={"proposal":1,"amount":STAKE,"basis":"stake","contract_type":sig,"currency":"USD","duration":1,"duration_unit":"m","symbol":SYMBOL}
                await ws.send(json.dumps(prop))
                pr=json.loads(await ws.recv()); print(f"Proposal: {pr}", flush=True)
                if "proposal" in pr:
                    await ws.send(json.dumps({"buy": pr["proposal"]["id"], "price": STAKE}))
                    buy=json.loads(await ws.recv()); print(f"→ TRADE {buy}", flush=True)
            else:
                if len(ticks)%20==0: print(f"[{time.strftime('%H:%M:%S')}] {reason}", flush=True)
        elif "balance" in msg:
            print(f"Balance: {msg['balance']}", flush=True)

async def main():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                accs = await get_accounts(session)
                # Prefer configured ID
                chosen = next((a for a in accs if a["account_id"]==ACCOUNT_ID), None) or accs[0]
                acc_id = chosen["account_id"]
                print(f"[MAIN] Using {acc_id}", flush=True)
                ws_url = await get_otp_url(session, acc_id)
                print(f"[WS] Connecting...", flush=True)
                async with websockets.connect(ws_url) as ws:
                    print("✓ CONNECTED - No 1006!", flush=True)
                    await trading_loop(ws)
        except Exception as e:
            import traceback; traceback.print_exc()
            print(f"[RETRY] {e} in 10s", flush=True)
            await asyncio.sleep(10)

if __name__=="__main__":
    asyncio.run(main())
