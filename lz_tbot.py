
import os, asyncio, json, time
import websockets
import aiohttp
import numpy as np

print("=== LZ-TBot v4 FINAL - Deriv v5 API - App ID 341aJK71v75g15Vud3q6w ===", flush=True)

TOKEN = os.environ.get("DERIV_TOKEN","").strip()
APP_ID = "341aJK71v75g15Vud3q6w"  # Your alphanumeric ID - HARDCODED
ACCOUNT_ID = os.environ.get("DERIV_ACCOUNT_ID","").strip()  # e.g. DOT93742818 or CR... - optional auto
SYMBOL = os.environ.get("SYMBOL","R_75")
STAKE = float(os.environ.get("STAKE","0.35"))

# Clean token
if TOKEN.lower().startswith("pat_"):
    # keep as is, but ensure Bearer uses original
    pass

print(f"App ID: {APP_ID}", flush=True)
print(f"Token: {TOKEN[:8]}...{TOKEN[-4:]}", flush=True)
print(f"Account override: {ACCOUNT_ID or 'AUTO'}", flush=True)

if not TOKEN:
    print("[FATAL] DERIV_TOKEN missing in Render Environment!", flush=True)
    while True: time.sleep(60)

async def get_accounts(session):
    """GET /trading/v1/accounts -> list of accounts"""
    url = "https://api.deriv.com/trading/v1/accounts"
    headers = {"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {TOKEN}"}
    print(f"[REST] GET {url}", flush=True)
    async with session.get(url, headers=headers) as r:
        txt = await r.text()
        print(f"[REST] Accounts {r.status}: {txt[:1000]}", flush=True)
        if r.status!=200:
            raise RuntimeError(f"Accounts failed {r.status}: {txt}")
        data = json.loads(txt)
        # Format from your token: data = [{"account_id": "DOT93742818", ...}, {"account_id": "ROT92214897"}]
        accounts = []
        if isinstance(data, dict) and "data" in data:
            accounts = data["data"]
        elif isinstance(data, list):
            accounts = data
        elif isinstance(data, dict) and "accounts" in data:
            accounts = data["accounts"]
        else:
            accounts = data
        
        if not accounts:
            raise RuntimeError(f"No accounts found: {txt}")
        # Normalize
        norm = []
        for a in accounts:
            acc_id = a.get("account_id") or a.get("id") or a.get("accountId")
            if acc_id:
                norm.append({"account_id": acc_id, "raw": a})
        print(f"[REST] Found accounts: {[x['account_id'] for x in norm]}", flush=True)
        return norm

async def get_otp_url(session, account_id):
    """POST /trading/v1/options/accounts/{id}/otp -> {data: {url: wss://...}}"""
    url = f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"
    headers = {"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    print(f"[REST] POST {url}", flush=True)
    async with session.post(url, headers=headers, json={}) as r:
        txt = await r.text()
        print(f"[REST] OTP {r.status}: {txt[:1000]}", flush=True)
        if r.status!=200:
            raise RuntimeError(f"OTP failed {r.status}: {txt}")
        data = json.loads(txt)
        ws_url = None
        if "data" in data:
            ws_url = data["data"].get("url") or data["data"].get("ws_url")
        ws_url = ws_url or data.get("url")
        if not ws_url:
            raise RuntimeError(f"No WS URL in OTP response: {data}")
        print(f"[REST] ✓ OTP URL: {ws_url[:120]}...", flush=True)
        return ws_url

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
    return None, f"WAIT EMA20{ema20:.1f} EMA50{ema50:.1f} RSI{rsi:.1f}"

async def trading_loop(ws):
    await ws.send(json.dumps({"ticks": SYMBOL, "subscribe":1}))
    await ws.send(json.dumps({"balance":1, "subscribe":1}))
    print(f"✓ LIVE - Listening {SYMBOL} Stake ${STAKE}", flush=True)
    while True:
        msg=json.loads(await ws.recv())
        # print(f"MSG: {msg}", flush=True)
        if "tick" in msg:
            price=float(msg["tick"]["quote"])
            ticks.append(price)
            if len(ticks)>300: ticks.pop(0)
            if len(ticks)%10==0:
                print(f"Tick {len(ticks)}: {price}", flush=True)
            signal, reason = calc_signal(ticks)
            if signal:
                print(f"[SIGNAL] {signal} | {reason}", flush=True)
                # Proposal on new API: same format? Let's try
                proposal={"proposal":1,"amount":STAKE,"basis":"stake","contract_type":signal,"currency":"USD","duration":1,"duration_unit":"m","symbol":SYMBOL}
                await ws.send(json.dumps(proposal))
                resp=json.loads(await ws.recv())
                print(f"Proposal resp: {resp}", flush=True)
                if "proposal" in resp:
                    await ws.send(json.dumps({"buy": resp["proposal"]["id"], "price": STAKE}))
                    buy=json.loads(await ws.recv())
                    print(f"→ TRADE RESULT: {buy}", flush=True)
            else:
                if len(ticks)%20==0:
                    print(f"[{time.strftime('%H:%M:%S')}] {reason}", flush=True)
        elif "balance" in msg:
            print(f"Balance update: {msg['balance']}", flush=True)

async def main():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                accounts = await get_accounts(session)
                # Choose account: use override if set, else first demo DOT, else first
                chosen = None
                if ACCOUNT_ID:
                    chosen = next((a for a in accounts if a["account_id"]==ACCOUNT_ID), None) or {"account_id": ACCOUNT_ID}
                else:
                    # Prefer demo account DOT
                    chosen = next((a for a in accounts if a["account_id"].startswith("DOT")), None) or accounts[0]
                acc_id = chosen["account_id"]
                print(f"[MAIN] Using account {acc_id}", flush=True)
                
                ws_url = await get_otp_url(session, acc_id)
                
                print(f"[WS] Connecting to {ws_url[:80]}...", flush=True)
                async with websockets.connect(ws_url) as ws:
                    print(f"✓ CONNECTED - No 1006! Pre-authenticated!", flush=True)
                    await trading_loop(ws)
        except Exception as e:
            import traceback
            print(f"[RECONNECT] {e}", flush=True)
            traceback.print_exc()
            await asyncio.sleep(10)

if __name__=="__main__":
    asyncio.run(main())
