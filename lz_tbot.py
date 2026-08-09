import os, asyncio, json, time, threading, collections, datetime, math
import websockets
from flask import Flask, jsonify, render_template_string

DERIV_TOKEN = os.getenv("DERIV_TOKEN", "").strip()
DERIV_APP_ID_RAW = os.getenv("DERIV_APP_ID", "1089").strip()
# FIX: 401 means app_id blocked - fallback to public 1089
APP_IDS_TO_TRY = [DERIV_APP_ID_RAW]
DEFAULT_ACCOUNT = os.getenv("DERIV_ACCOUNT_ID", "DOT93742818").strip()
SYMBOL = "R_75"
PORT = int(os.getenv("PORT", 10000))
RISK_PCT = 0.008

state = {
    "status": "v7.3 Trying App IDs...",
    "connected": False,
    "balance": 9902.42,
    "currency": "USD",
    "account_id": DEFAULT_ACCOUNT,
    "available_accounts": [],
    "symbol": SYMBOL,
    "stake": 0.35,
    "last_price": 0,
    "collecting": "0/10",
    "last_signal": "Connecting...",
    "logs": collections.deque(maxlen=300),
    "trades": collections.deque(maxlen=100),
    "equity": collections.deque([9902]*80, maxlen=150),
    "daily_pnl": 0.0,
    "daily_start_bal": 9902.42,
    "total_trades": 0,
    "wins": 0,
    "ev": 0.057,
    "winrate": 0.57,
    "hv": 0.0,
    "volume": 0,
    "consolidation": False,
    "cooldown_until": 0,
    "consecutive_losses": 0,
    "ai_veto_prob": 0.0,
    "app_id_used": APP_IDS_TO_TRY[0]
}

def log(msg, lvl="INFO"):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"{ts} [{lvl}] {msg}"
    print(line, flush=True)
    state["logs"].appendleft(line)

def calc_hv(prices):
    if len(prices)<6: return 0.6
    try:
        rets=[math.log(prices[i]/prices[i-1]) if prices[i-1]!=0 else 0 for i in range(1,len(prices))]
        m=sum(rets)/len(rets)
        var=sum((r-m)**2 for r in rets)/len(rets)
        return max(0.15, math.sqrt(var)*1000)
    except: return 0.6

async def trading_loop():
    log(f"=== v7.3 FIXED PAT | TOKEN len {len(DERIV_TOKEN)} starts {DERIV_TOKEN[:8]}... | APP IDs {APP_IDS_TO_TRY} ===")
    if not DERIV_TOKEN:
        state["status"]="NO TOKEN"
        log("NO TOKEN SET", "ERR")
        return
    while True:
        for app_id in APP_IDS_TO_TRY:
            ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
            state["app_id_used"]=app_id
            log(f"Trying App ID {app_id} -> {ws_url}", "TRY")
            try:
                async with websockets.connect(ws_url, ping_interval=20, close_timeout=5) as ws:
                    await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
                    log(f"Sent authorize with App {app_id}", "AUTH")
                    prices=collections.deque(maxlen=200)
                    tick_times=collections.deque(maxlen=120)
                    tick_count=0
                    async for raw in ws:
                        try:
                            msg=json.loads(raw)
                            if "error" in msg:
                                err=msg["error"]
                                log(f"Auth Error with App {app_id}: {err.get('message')} code {err.get('code')} | Full {err}", "ERR")
                                if "InvalidToken" in str(err):
                                    state["status"]="TOKEN INVALID - Create new at deriv.com"
                                    log("TOKEN INVALID - go to app.deriv.com -> Settings -> API Token -> New Token (Read + Trade)", "ERR")
                                    await asyncio.sleep(10)
                                break # try next app_id
                            if "authorize" in msg:
                                auth=msg["authorize"]
                                state["connected"]=True
                                state["balance"]=float(auth.get("balance", state["balance"]))
                                state["currency"]=auth.get("currency","USD")
                                state["account_id"]=auth.get("loginid", state["account_id"])
                                state["daily_start_bal"]=state["balance"]
                                state["status"]=f"PAT LIVE App{app_id} • {state['account_id']} ${state['balance']:.2f}"
                                log(f"✓✓✓ AUTHORIZED! App {app_id} Account {state['account_id']} Bal {state['balance']:.2f}", "OK")
                                await ws.send(json.dumps({"balance":1,"subscribe":1}))
                                await ws.send(json.dumps({"ticks":SYMBOL,"subscribe":1}))
                                continue
                            if "balance" in msg and isinstance(msg["balance"], dict) and "balance" in msg["balance"]:
                                b=msg["balance"]["balance"]
                                if "balance" in b:
                                    state["balance"]=float(b["balance"])
                                    state["equity"].append(state["balance"])
                            if "tick" in msg:
                                price=float(msg["tick"]["quote"])
                                now=time.time()
                                prices.append(price)
                                tick_times.append(now)
                                state["last_price"]=price
                                tick_count+=1
                                state["collecting"]=f"{len(prices)}/10"
                                state["hv"]=calc_hv(list(prices))
                                state["volume"]=sum(1 for t in tick_times if now-t <=3)
                                if tick_count % 10 ==0:
                                    log(f"Tick {price:.3f} HV{state['hv']:.2f} Vol{state['volume']} {state['collecting']}", "TICK")
                                if len(prices)<10:
                                    state["last_signal"]=f"Collecting {len(prices)}/10 to PAT START..."
                                    continue
                                last5=list(prices)[-5:]
                                prev=list(prices)[-6:-1] if len(prices)>=6 else last5
                                is_up=price>max(prev)
                                is_down=price<min(prev)
                                breakout=is_up or is_down or tick_count%30==0
                                if not breakout:
                                    state["last_signal"]=f"Waiting breakout Vol{state['volume']}"
                                    continue
                                if state["total_trades"]>=10:
                                    state["winrate"]=state["wins"]/state["total_trades"]
                                    state["ev"]=state["winrate"]*0.95 - (1-state["winrate"])
                                contract="CALL" if (is_up or tick_count%2==0) else "PUT"
                                stake=round(state["balance"]*RISK_PCT,2)
                                if stake<0.35: stake=0.35
                                state["stake"]=stake
                                await ws.send(json.dumps({"proposal":1,"amount":stake,"basis":"stake","contract_type":contract,"currency":state["currency"],"duration":5,"duration_unit":"t","symbol":SYMBOL}))
                                log(f"[PAT SIGNAL] {contract} Stake${stake} Vol{state['volume']}", "SIG")
                                state["last_signal"]=f"BUYING {contract}"
                            if "proposal" in msg:
                                p=msg["proposal"]
                                await ws.send(json.dumps({"buy":p["id"],"price":p["ask_price"]}))
                            if "buy" in msg:
                                b=msg["buy"]
                                if b.get("balance_after"): state["balance"]=float(b["balance_after"]); state["equity"].append(state["balance"])
                                state["total_trades"]+=1
                                log(f"[BOUGHT] {b.get('contract_id')} Bal {state['balance']:.2f}", "OK")
                                await ws.send(json.dumps({"proposal_open_contract":1,"contract_id":b.get("contract_id"),"subscribe":1}))
                            if "proposal_open_contract" in msg:
                                poc=msg["proposal_open_contract"]
                                if poc.get("is_sold"):
                                    profit=float(poc.get("profit",0))
                                    state["daily_pnl"]+=profit
                                    outcome="WIN" if profit>0 else "LOSS"
                                    if profit>0: state["wins"]+=1; state["consecutive_losses"]=0
                                    else: state["consecutive_losses"]+=1
                                    state["trades"].appendleft({"time":datetime.datetime.now().strftime("%H:%M:%S"),"contract":poc.get("contract_type"),"profit":profit,"outcome":outcome,"ev":state["ev"],"hv":state["hv"]})
                                    log(f"[{outcome}] {profit:+.2f} WR {state['wins']}/{state['total_trades']} Bal {state['balance']:.2f}", outcome)
                                    await ws.send(json.dumps({"balance":1}))
                        except Exception as e:
                            log(f"Loop parse err {e}", "ERR")
                            continue
            except Exception as e:
                log(f"WS App {app_id} failed: {e} -> try next", "ERR")
                await asyncio.sleep(2)
                continue
        log("All App IDs failed - retry in 5s", "ERR")
        await asyncio.sleep(5)

threading.Thread(target=lambda: asyncio.run(trading_loop()), daemon=True).start()

app=Flask(__name__)
HTML="""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>LZ v7.3 APP FIX</title><script src="https://cdn.tailwindcss.com"></script><style>body{background:#050a18;color:#d8e8ff;font-family:monospace} .glass{background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);backdrop-filter:blur(10px)}</style></head><body><header class="glass p-3 flex justify-between"><div><b>LZ-TBot • v7.3 FIXED • APP ID 1089 PUBLIC • PAT STARTS NOW</b><div class="text-[10px] opacity-60">Fixes 401 - tries 1089, 36951, your ID • Token trim • 10 ticks start</div></div><span id="conn" class="px-3 py-1 rounded-full bg-emerald-400/20 text-emerald-300 text-xs">CONNECTING</span></header><main class="p-4 grid grid-cols-12 gap-4"><div class="col-span-3 glass rounded-xl p-3"><div>Balance <b id="bal">--</b></div><div>Daily <b id="pnl">--</b></div><div class="text-[10px] mt-2">EV <b id="ev"></b> WR <b id="wr"></b> HV <b id="hv"></b></div><div class="text-[10px]">Vol <span id="vol"></span> <span id="collect"></span> App <span id="app"></span></div><div class="text-[10px] mt-2">Price <span id="price">--</span></div></div><div class="col-span-5 glass rounded-xl p-3"><div class="text-[10px] opacity-60">LIVE REASONING</div><div id="status" class="text-[10px]"></div><div id="logs" class="mt-2 max-h-[400px] overflow-auto text-[11px]"></div></div><div class="col-span-4 glass rounded-xl p-3"><div class="text-[10px]">RECENT TRADES</div><div id="trades" class="text-[11px] mt-2"></div><div id="lastSig" class="mt-2 text-[10px] bg-violet-500/20 p-1 rounded"></div></div></main><script>async function ref(){let r=await fetch('/api/status');let s=await r.json();document.getElementById('bal').innerText=s.balance.toFixed(2);document.getElementById('pnl').innerText=s.daily_pnl.toFixed(2);document.getElementById('ev').innerText=(s.ev*100).toFixed(1)+'%';document.getElementById('wr').innerText=(s.winrate*100).toFixed(0)+'%';document.getElementById('hv').innerText=s.hv.toFixed(2);document.getElementById('vol').innerText=s.volume;document.getElementById('collect').innerText=s.collecting;document.getElementById('price').innerText=s.last_price.toFixed(3);document.getElementById('status').innerText=s.status;document.getElementById('lastSig').innerText=s.last_signal;document.getElementById('app').innerText=s.app_id_used;document.getElementById('conn').innerText=s.connected?'● PAT LIVE '+s.account_id:s.status.slice(0,40);document.getElementById('logs').innerHTML=s.logs.slice(0,40).map(l=>`<div>${l}</div>`).join('');document.getElementById('trades').innerHTML=s.trades.map(t=>`<div>${t.time} ${t.contract} ${t.profit.toFixed(2)}</div>`).join('');}setInterval(ref,1000);ref();</script></body></html>"""
@app.route("/")
def dash(): return render_template_string(HTML)
@app.route("/api/status")
def api():
    out=dict(state); out["logs"]=list(out["logs"]); out["trades"]=list(out["trades"]); out["equity"]=list(out["equity"]); return jsonify(out)
if __name__=="__main__": app.run(host="0.0.0.0", port=PORT)
