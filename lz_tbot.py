
import os, sys, asyncio, json, time, argparse
import websockets
import numpy as np

print("=== LZ-TBot Starting ===", flush=True)

# --- Config from ENV ---
TOKEN = os.environ.get("DERIV_TOKEN", "").strip()
SYMBOL = os.environ.get("SYMBOL", "R_75")
STAKE = float(os.environ.get("STAKE", "0.35"))
print(f"Symbol: {SYMBOL} Stake: {STAKE} Token set: {bool(TOKEN)}", flush=True)

if not TOKEN:
    print("\n[ERROR] DERIV_TOKEN not found!", flush=True)
    print("Go to Render -> Environment -> Add Variable -> Key=DERIV_TOKEN Value=your_token", flush=True)
    print("Then Manual Deploy -> Deploy latest commit", flush=True)
    # Keep alive to show error in logs, not crash loop fast
    while True:
        time.sleep(60)

DERIV_WS = "wss://ws.derivws.com/websockets/v3?app_id=1089"

# --- MY BRAIN (same as before, simplified stable) ---
ticks = []

def calc_signal(prices):
    if len(prices) < 200:
        return None, "Waiting for 200 ticks"
    arr = np.array(prices)
    ema20 = np.mean(arr[-20:])
    ema50 = np.mean(arr[-50:])
    ema200 = np.mean(arr[-200:])
    # simple RSI
    deltas = np.diff(arr[-15:])
    gains = deltas[deltas>0].sum()
    losses = -deltas[deltas<0].sum()
    rs = gains/(losses+0.001)
    rsi = 100 - (100/(1+rs))
    
    trend_up = ema20 > ema50 > ema200
    trend_down = ema20 < ema50 < ema200
    
    if trend_up and 40 < rsi < 58:
        return "CALL", f"Trend UP EMA {ema20:.2f}>{ema50:.2f}>{ema200:.2f} RSI {rsi:.1f}"
    if trend_down and 42 < rsi < 62:
        return "PUT", f"Trend DOWN EMA {ema20:.2f}<{ema50:.2f}<{ema200:.2f} RSI {rsi:.1f}"
    return None, f"No trade - EMA20 {ema20:.2f} EMA50 {ema50:.2f} RSI {rsi:.1f}"

async def run():
    while True:
        try:
            async with websockets.connect(DERIV_WS) as ws:
                # Auth
                await ws.send(json.dumps({"authorize": TOKEN}))
                auth_resp = json.loads(await ws.recv())
                if "error" in auth_resp:
                    print(f"[AUTH ERROR] {auth_resp['error']['message']}", flush=True)
                    await asyncio.sleep(30)
                    continue
                balance = auth_resp.get("authorize",{}).get("balance",0)
                print(f"✓ Authorized! Balance: {balance}", flush=True)
                print(f"✓ LZ-TBot Listening for {SYMBOL} ...", flush=True)
                
                # Subscribe ticks
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
                
                while True:
                    msg = json.loads(await ws.recv())
                    if "tick" in msg:
                        price = float(msg["tick"]["quote"])
                        ticks.append(price)
                        if len(ticks) > 300:
                            ticks.pop(0)
                        if len(ticks) % 10 == 0:
                            print(f"Tick {len(ticks)}: {price}", flush=True)
                        signal, reason = calc_signal(ticks)
                        if signal:
                            print(f"[{time.strftime('%H:%M:%S')}] SIGNAL: {signal} | {reason} | Price {price}", flush=True)
                            # Place trade
                            proposal = {
                                "proposal": 1,
                                "amount": STAKE,
                                "basis": "stake",
                                "contract_type": signal,
                                "currency": "USD",
                                "duration": 1,
                                "duration_unit": "m",
                                "symbol": SYMBOL
                            }
                            await ws.send(json.dumps(proposal))
                            prop_resp = json.loads(await ws.recv())
                            if "proposal" in prop_resp:
                                await ws.send(json.dumps({"buy": prop_resp["proposal"]["id"], "price": STAKE}))
                                buy_resp = json.loads(await ws.recv())
                                print(f"→ Trade placed! {buy_resp}", flush=True)
                        else:
                            if len(ticks) % 20 == 0:
                                print(f"[{time.strftime('%H:%M:%S')}] {reason}", flush=True)
        except Exception as e:
            print(f"[RECONNECT] Error: {e} - retry in 5s", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run())
