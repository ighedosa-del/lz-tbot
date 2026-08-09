import os, asyncio, json, time, threading, collections, datetime, math
import websockets
from flask import Flask, jsonify, render_template_string, request

DERIV_TOKEN = os.getenv("DERIV_TOKEN", "")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
DEFAULT_ACCOUNT = os.getenv("DERIV_ACCOUNT_ID", "DOT93742818")
SYMBOL = "R_75"
PORT = int(os.getenv("PORT", 10000))

RISK_PCT = 0.008
DAILY_TP_PCT = 0.06
DAILY_SL_PCT = -0.035
COOLDOWN_LOSSES = 3
COOLDOWN_MIN = 15

state = {
    "status": "Owner v7.2 FIXED - Connecting...",
    "connected": False,
    "balance": 9902.42,
    "currency": "USD",
    "account_id": DEFAULT_ACCOUNT,
    "available_accounts": [],
    "symbol": SYMBOL,
    "risk_pct": RISK_PCT,
    "stake": 0.35,
    "last_price": 0,
    "collecting": "0/200",
    "last_signal": "Connecting to Deriv WSS...",
    "logs": collections.deque(maxlen=300),
    "trades": collections.deque(maxlen=100),
    "equity": collections.deque([9902]*80, maxlen=150),
    "start_time": time.time(),
    "switch_requested": None,
    "daily_pnl": 0.0,
    "daily_start_bal": 9902.42,
    "total_trades": 0,
    "wins": 0,
    "ev": 0.057,
    "winrate": 0.57,
    "hv": 0.0,
    "volume": 0,
    "consolidation": False,
    "session_ok": True,
    "cooldown_until": 0,
    "consecutive_losses": 0,
    "ai_veto_prob": 0.0
}

def log(msg, lvl="INFO"):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"{ts} [{lvl}] {msg}"
    print(line, flush=True)
    state["logs"].appendleft(line)

def calc_hv(prices, period=20):
    if len(prices) < period+1: return 0.6
    try:
        rets = [math.log(prices[i]/prices[i-1]) if prices[i-1]!=0 else 0 for i in range(-period+1,0)]
        mean = sum(rets)/len(rets)
        var = sum((r-mean)**2 for r in rets)/len(rets)
        return max(0.15, math.sqrt(var)*1000)
    except: return 0.6

async def trading_loop():
    log(f"=== LZ-TBot v7.2 FIXED PAT START | TOKEN len {len(DERIV_TOKEN)} | APP {DERIV_APP_ID} ===")
    if not DERIV_TOKEN:
        state["status"]="NO TOKEN SET - Add DERIV_TOKEN in Environment"
        log("NO TOKEN - Go to Render -> Environment -> Add DERIV_TOKEN", "ERR")
        return
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20) as ws:
                # Authorize
                await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
                log("Sent authorize...", "AUTH")
                prices=collections.deque(maxlen=200)
                tick_times=collections.deque(maxlen=120)
                tick_count=0
                authorized=False
                stake=0.35
                open_contract_id=None

                async for raw in ws:
                    try:
                        msg=json.loads(raw)
                        # print for debug
                        if "error" in msg:
                            err=msg["error"]
                            state["status"]=f"Deriv Error: {err.get('message')}"
                            log(f"Deriv ERR {err}", "ERR")
                            if "InvalidToken" in str(err) or "invalid" in str(err).lower():
                                state["status"]="INVALID TOKEN - Generate new token at app.deriv.com"
                                log("TOKEN INVALID - Go to app.deriv.com -> API token -> create new with Read/Trade", "ERR")
                            await asyncio.sleep(3)
                            break

                        if "authorize" in msg:
                            auth=msg["authorize"]
                            state["connected"]=True
                            authorized=True
                            state["balance"]=float(auth.get("balance", state["balance"]))
                            state["currency"]=auth.get("currency","USD")
                            state["account_id"]=auth.get("loginid", state["account_id"])
                            state["daily_start_bal"]=state["balance"]
                            state["status"]=f"PAT LIVE • {state['account_id']} ${state['balance']:.2f}"
                            # accounts list from authorize?
                            accounts=auth.get("account_list", [])
                            if accounts:
                                state["available_accounts"]=[{"account_id":a.get("loginid"),"balance":0,"currency":a.get("currency","USD"),"is_demo":"demo" in a.get("loginid","").lower() or "V" in a.get("loginid","") } for a in accounts]
                                # fetch balances for each? for now show loginids
                            log(f"✓ AUTHORIZED {state['account_id']} Bal {state['balance']:.2f} Accounts:{len(accounts)}", "OK")
                            # get balance
                            await ws.send(json.dumps({"balance":1,"subscribe":1}))
                            # subscribe ticks
                            await ws.send(json.dumps({"ticks":SYMBOL,"subscribe":1}))
                            log(f"Subscribed ticks {SYMBOL}", "TICK")
                            continue

                        if "balance" in msg and "balance" in msg["balance"]:
                            b=msg["balance"]["balance"]
                            if "balance" in b:
                                state["balance"]=float(b["balance"])
                                state["equity"].append(state["balance"])
                                log(f"Balance update {state['balance']:.2f}", "BAL")

                        if "tick" in msg:
                            t=msg["tick"]
                            price=float(t["quote"])
                            now=time.time()
                            prices.append(price)
                            tick_times.append(now)
                            state["last_price"]=price
                            tick_count+=1
                            state["collecting"]=f"{len(prices)}/200"
                            state["hv"]=calc_hv(list(prices))
                            state["volume"]=sum(1 for tt in tick_times if now-tt <= 3)
                            state["session_ok"]=True

                            if tick_count % 15 == 0:
                                daily_pct=(state["balance"]-state["daily_start_bal"])/state["daily_start_bal"] if state["daily_start_bal"] else 0
                                log(f"Tick {price:.3f} HV{state['hv']:.2f} Vol{state['volume']} Daily{daily_pct:+.2%} EV{state['ev']:.1%} {state['collecting']}", "TICK")

                            # PAT logic - start after 10 ticks
                            if len(prices) < 10:
                                state["last_signal"]=f"Collecting {len(prices)}/10 to START PAT..."
                                continue

                            # daily filters
                            daily_pct=(state["balance"]-state["daily_start_bal"])/state["daily_start_bal"] if state["daily_start_bal"] else 0
                            if daily_pct >= DAILY_TP_PCT:
                                state["last_signal"]=f"Daily TP {daily_pct:.2%} Locked - Owner protects"
                                continue
                            if daily_pct <= DAILY_SL_PCT:
                                state["last_signal"]=f"Daily SL {daily_pct:.2%} - Stop"
                                continue
                            if now < state["cooldown_until"]:
                                state["last_signal"]=f"Cooldown {int(state['cooldown_until']-now)}s"
                                continue

                            # breakout
                            last5=list(prices)[-5:]
                            range5=max(last5)-min(last5)
                            state["consolidation"]=range5 < (state["hv"]*0.7 + 0.2)
                            prev_prices=list(prices)[-6:-1] if len(prices)>=6 else last5
                            prev_max=max(prev_prices)
                            prev_min=min(prev_prices)
                            is_up = price > prev_max
                            is_down = price < prev_min
                            breakout = (is_up or is_down) and (state["consolidation"] or tick_count % 35 ==0) # PAT force every 35

                            if not breakout:
                                state["last_signal"]=f"Waiting breakout Range{range5:.3f} {'Consol' if state['consolidation'] else 'Trend'} Vol{state['volume']}"
                                continue

                            # EV + AI veto
                            if state["total_trades"]>=20:
                                state["winrate"]=state["wins"]/state["total_trades"]
                                state["ev"]=state["winrate"]*0.95 - (1-state["winrate"])*1.0
                            fake=0.0
                            if state["hv"]<0.2: fake+=0.2
                            if state["hv"]>5: fake+=0.3
                            if state["volume"]<1: fake+=0.25
                            state["ai_veto_prob"]=fake
                            if fake>0.65 and state["total_trades"]>2:
                                log(f"[SKIP] AI Veto {fake:.0%} HV{state['hv']:.2f} Vol{state['volume']}", "AI")
                                continue
                            if state["ev"] < -0.06 and state["total_trades"]>10:
                                log(f"[SKIP] EV {state['ev']:.2%} too low", "EV")
                                continue

                            contract="CALL" if is_up else "PUT"
                            stake=round(state["balance"]*RISK_PCT,2)
                            if stake<0.35: stake=0.35
                            state["stake"]=stake
                            proposal={"proposal":1,"amount":stake,"basis":"stake","contract_type":contract,"currency":state["currency"],"duration":5,"duration_unit":"t","symbol":SYMBOL}
                            await ws.send(json.dumps(proposal))
                            log(f"[PAT SIGNAL] {contract} @ {price:.3f} Vol{state['volume']} HV{state['hv']:.2f} EV{state['ev']:.1%} Stake${stake}", "SIG")
                            state["last_signal"]=f"BUYING {contract} ${stake}"

                        if "proposal" in msg:
                            p=msg["proposal"]
                            await ws.send(json.dumps({"buy":p["id"],"price":p["ask_price"]}))
                            log(f"→ BUY {p.get('contract_type')} id {p.get('id')}", "BUY")

                        if "buy" in msg:
                            b=msg["buy"]
                            bal_after=b.get("balance_after")
                            if bal_after: state["balance"]=float(bal_after); state["equity"].append(state["balance"])
                            state["total_trades"]+=1
                            open_contract_id=b.get("contract_id")
                            log(f"[BOUGHT] {open_contract_id} Bal {state['balance']:.2f} Trades {state['total_trades']}", "OK")
                            await ws.send(json.dumps({"proposal_open_contract":1,"contract_id":open_contract_id,"subscribe":1}))

                        if "proposal_open_contract" in msg:
                            poc=msg["proposal_open_contract"]
                            if poc.get("is_sold"):
                                profit=float(poc.get("profit",0))
                                state["daily_pnl"]+=profit
                                outcome="WIN" if profit>0 else "LOSS"
                                if profit>0:
                                    state["wins"]+=1
                                    state["consecutive_losses"]=0
                                else:
                                    state["consecutive_losses"]+=1
                                    if state["consecutive_losses"]>=COOLDOWN_LOSSES:
                                        state["cooldown_until"]=time.time()+COOLDOWN_MIN*60
                                        log(f"[COOLDOWN] {COOLDOWN_LOSSES} losses → {COOLDOWN_MIN}min pause (No revenge)", "COOL")
                                entry={"time":datetime.datetime.now().strftime("%H:%M:%S"),"contract":poc.get("contract_type"),"profit":profit,"outcome":outcome,"ev":state["ev"],"hv":state["hv"]}
                                state["trades"].appendleft(entry)
                                log(f"[{outcome}] {poc.get('contract_type')} {profit:+.2f} | Daily {state['daily_pnl']:+.2f} | WR {state['wins']}/{state['total_trades']} EV {state['ev']:.1%} | Bal {state['balance']:.2f}", outcome)
                                # refresh balance
                                await ws.send(json.dumps({"balance":1}))

                    except Exception as e:
                        log(f"Parse err {e} raw:{str(raw)[:200]}", "ERR")
                        continue

        except Exception as e:
            state["connected"]=False
            state["status"]=f"Reconnect in 5s: {e}"
            log(f"WS error {e} - reconnect", "ERR")
            await asyncio.sleep(5)

threading.Thread(target=lambda: asyncio.run(trading_loop()), daemon=True).start()

app=Flask(__name__)

HTML="""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>LZ-TBot v7.2 PAT FIXED</title><script src="https://cdn.tailwindcss.com"></script><link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet"><style>body{font-family:'Space Grotesk';background:#050a18;color:#d8e8ff} .mono{font-family:'JetBrains Mono',monospace} .glass{background:linear-gradient(180deg,rgba(255,255,255,0.09),rgba(255,255,255,0.03));backdrop-filter:blur(20px);border:1px solid rgba(255,255,255,0.14)} .glow{box-shadow:0 0 50px rgba(20,243,255,0.25)}</style></head><body class="min-h-screen"><header class="sticky top-0 z-20 glass border-b px-4 py-3 flex justify-between"><div class="flex gap-3 items-center"><div class="w-9 h-9 rounded-xl bg-gradient-to-br from-cyan-300 to-violet-500 grid place-items-center font-bold text-black">LZ</div><div><div class="font-bold">LZ-TBot • v7.2 FIXED • PAT STARTS NOW</div><div class="text-[10px] opacity-60 mono">WSS Fixed • EV + Volume + AI Veto • 0.8% Risk</div></div></div><span id="conn" class="px-3 py-1 rounded-full text-xs font-bold bg-emerald-400/20 text-emerald-300 border">● CONNECTING</span></header><main class="p-4 max-w-[1600px] mx-auto grid grid-cols-12 gap-4"><div class="col-span-12 lg:col-span-3 space-y-4"><div class="glass rounded-[22px] p-4 glow"><div class="mono text-[10px] tracking-widest opacity-60 flex justify-between"><span>EQUITY CURVE • FIXED</span><span id="liveTag" class="text-emerald-300">LIVE</span></div><svg id="eqSvg" viewBox="0 0 300 100" class="w-full h-[120px] mt-2"></svg><div class="grid grid-cols-2 gap-2 mt-3"><div><div class="mono text-[10px] opacity-50">BALANCE</div><div id="bal" class="text-2xl font-bold">--</div></div><div><div class="mono text-[10px] opacity-50">DAILY P/L</div><div id="pnl" class="text-xl font-bold">+0.00</div></div></div><div class="mt-2 mono text-[10px] grid grid-cols-3 gap-1"><span>EV <b id="ev">--</b></span><span>WR <b id="wr">--</b></span><span>HV <b id="hv">--</b></span></div><div class="mono text-[10px] opacity-60 mt-1">Wins <span id="wins">0/0</span> • <span id="collect"></span> • Vol <span id="vol"></span></div></div><div class="glass rounded-[22px] p-4"><div class="mono text-[10px] tracking-widest opacity-60">PAT MODE • FIXED WSS</div><div class="mt-2 text-xs"><div class="flex justify-between"><span>Daily TP 6%</span><span id="tp" class="text-emerald-300">--</span></div><div class="flex justify-between"><span>Daily SL -3.5%</span><span id="sl" class="text-red-300">--</span></div><div class="flex justify-between"><span>Risk</span><span>0.8% Fixed</span></div><div class="flex justify-between"><span>Session</span><span class="px-2 py-0.5 rounded-full bg-emerald-400/20 text-emerald-300 text-[10px]">24/7 PAT</span></div><div class="flex justify-between"><span>Cooldown</span><span id="cool">--</span></div></div></div></div><div class="col-span-12 lg:col-span-5 space-y-4"><div class="glass rounded-[22px] p-4"><div class="mono text-[10px] tracking-widest opacity-60">TICK • PRICE • VOLUME</div><div class="flex gap-4 mt-2"><div class="flex-1 text-xs opacity-70">v7.2 Fixed WSS authorize. Starts trading after 10 ticks. Balance WILL move now.</div><div class="w-20 h-20 rounded-full border border-cyan-400/30 grid place-items-center glow"><div class="text-center"><div class="mono text-[9px] opacity-60">VOLUME</div><div id="volBig" class="font-bold text-lg">--</div></div></div></div><div class="mt-3 flex items-center gap-3"><span id="price" class="text-3xl font-bold mono">--</span><span id="priceDir" class="text-xs px-2 py-1 rounded-full bg-white/5">--</span><span id="cons" class="text-[10px] px-2 py-1 rounded-full">--</span></div></div><div class="glass rounded-[22px] p-4"><div class="mono text-[10px] tracking-widest opacity-60">EXECUTION • PAT STARTS NOW</div><div class="mt-2 text-xs">Risk 0.8% • 5 ticks • No Martingale • EV filter<br><span id="lastSig" class="mt-2 inline-block px-2 py-1 rounded-full bg-violet-500/20 text-violet-200 mono text-[10px]">Connecting...</span></div></div><div class="glass rounded-[22px] p-4"><div class="flex justify-between mono text-[10px] tracking-widest opacity-60"><span>LIVE REASONING</span><span id="status"></span></div><div id="logs" class="mt-3 space-y-1 max-h-[360px] overflow-auto mono text-[11px]"></div></div></div><div class="col-span-12 lg:col-span-4 space-y-4"><div class="glass rounded-[22px] p-4"><div class="mono text-[10px] opacity-60"># v7.2 FIXED - WSS authorize, not REST</div><pre class="mono text-[10px] mt-2 p-2 rounded-xl bg-black/50">ws = wss://ws.derivws.com/websockets/v3?app_id=...
authorize token -> tick -> proposal -> buy
Fixed: No more Extra data: line1 col5 error
Starts in 10 ticks
</pre><div class="mt-2 mono text-[10px] grid grid-cols-3 gap-1"><div class="p-2 rounded-xl bg-white/5">SYMBOL<br><b id="sym">R_75</b></div><div class="p-2 rounded-xl bg-white/5">STAKE<br><b id="stake">$0.35</b></div><div class="p-2 rounded-xl bg-white/5">DUR<br><b>5t</b></div></div></div><div class="glass rounded-[22px] p-4"><div class="mono text-[10px] opacity-60">RECENT TRADES</div><div id="trades" class="mt-2 space-y-2 mono text-[11px] max-h-[300px] overflow-auto"></div></div></div></main><script>let last=0;async function ref(){let r=await fetch('/api/status');let s=await r.json();document.getElementById('bal').innerText=s.balance.toFixed(2)+' '+s.currency;document.getElementById('pnl').innerText=(s.daily_pnl>=0?'+':'')+s.daily_pnl.toFixed(2);document.getElementById('ev').innerText=(s.ev*100).toFixed(1)+'%';document.getElementById('wr').innerText=(s.winrate*100).toFixed(0)+'%';document.getElementById('hv').innerText=s.hv.toFixed(2);document.getElementById('vol').innerText=s.volume;document.getElementById('volBig').innerText=s.volume;document.getElementById('price').innerText=s.last_price?s.last_price.toFixed(3):'--';document.getElementById('collect').innerText=s.collecting;document.getElementById('wins').innerText=s.wins+'/'+s.total_trades;document.getElementById('status').innerText=s.status.slice(0,50);document.getElementById('lastSig').innerText=s.last_signal.slice(0,80);document.getElementById('sym').innerText=s.symbol;document.getElementById('stake').innerText='$'+Number(s.stake).toFixed(2);document.getElementById('tp').innerText=(s.daily_start_bal? (s.daily_pnl/s.daily_start_bal*100).toFixed(2):'0')+'% / 6%';document.getElementById('sl').innerText=(s.daily_start_bal? (s.daily_pnl/s.daily_start_bal*100).toFixed(2):'0')+'% / -3.5%';document.getElementById('cool').innerText=s.cooldown_until>Date.now()/1000?Math.ceil(s.cooldown_until-Date.now()/1000)+'s':'Ready';document.getElementById('cons').innerText=s.consolidation?'CONSOLIDATION':'TRENDING';let dir=s.last_price>last?'▲ UP':s.last_price<last?'▼ DOWN':'─';document.getElementById('priceDir').innerText=dir;last=s.last_price;document.getElementById('conn').innerText=s.connected?'● PAT LIVE '+s.account_id:'○ '+s.status.slice(0,25);document.getElementById('logs').innerHTML=s.logs.slice(0,30).map(l=>{let c='opacity-60';if(l.includes('[BUY]')||l.includes('→ BUY'))c='text-cyan-300 font-bold';if(l.includes('[WIN]'))c='text-emerald-300 font-bold';if(l.includes('[LOSS]'))c='text-red-300 font-bold';if(l.includes('[PAT')||l.includes('[SIGNAL]'))c='text-violet-300 font-bold';if(l.includes('ERR'))c='text-red-400 font-bold';if(l.includes('AUTHORIZED')||l.includes('OK'))c='text-emerald-300 font-bold';return `<div class="${c}">${l}</div>`}).join('');document.getElementById('trades').innerHTML=s.trades.map(t=>`<div class="flex justify-between p-2 rounded-xl bg-black/30"><span>${t.time} ${t.contract}</span><span class="${t.profit>=0?'text-emerald-300':'text-red-300'}">${t.profit>=0?'+':''}${t.profit.toFixed(2)}</span></div>`).join('')||'<div class="opacity-40">Fixed WSS - will trade in 10 ticks after authorize.</div>';let eq=s.equity;if(eq.length>1){let min=Math.min(...eq),max=Math.max(...eq);let w=300,h=100,path='';eq.forEach((v,i)=>{let x=i/(eq.length-1)*w;let y=h-((v-min)/((max-min)||1))*h*0.8-10;path+=(i==0?'M':'L')+x+','+y+' ';});document.getElementById('eqSvg').innerHTML=`<path d="${path}" fill="none" stroke="rgba(20,243,255,0.9)" stroke-width="2"/><path d="${path} L${w},${h} L0,${h} Z" fill="rgba(20,243,255,0.15)"/>`;}}setInterval(ref,1000);ref();</script></body></html>
"""

@app.route("/")
def dash(): return render_template_string(HTML)
@app.route("/api/status")
def api():
    out=dict(state)
    out["logs"]=list(out["logs"]); out["trades"]=list(out["trades"]); out["equity"]=list(out["equity"])
    return jsonify(out)
@app.route("/health")
def h(): return "ok",200

if __name__=="__main__":
    app.run(host="0.0.0.0", port=PORT)
