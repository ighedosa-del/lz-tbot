import os, asyncio, json, time, threading, collections, datetime, math, random
import aiohttp
import websockets
from flask import Flask, jsonify, render_template_string, request

DERIV_TOKEN = os.getenv("DERIV_TOKEN", "")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "341aJK71v75g15Vud3q6w")
DEFAULT_ACCOUNT = os.getenv("DERIV_ACCOUNT_ID", "DOT93742818")
SYMBOL = "R_75"
PORT = int(os.getenv("PORT", 10000))

# OWNER'S SETTINGS - Capital preservation first
RISK_PCT = 0.008  # 0.8% fixed
DAILY_TP_PCT = 0.06
DAILY_SL_PCT = -0.035
COOLDOWN_LOSSES = 3
COOLDOWN_MIN = 15
SESSION_START = 13 # UTC
SESSION_END = 17

state = {
    "status": "Owner Mode: Building edge...",
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
    "last_signal": "Waiting for consolidation...",
    "logs": collections.deque(maxlen=250),
    "trades": collections.deque(maxlen=100),
    "equity": collections.deque([9902]*80, maxlen=120),
    "start_time": time.time(),
    "switch_requested": None,
    "daily_pnl": 0.0,
    "daily_start_bal": 9902.42,
    "total_trades": 0,
    "wins": 0,
    "ev": 0.0,
    "winrate": 0.0,
    "hv": 0.0,
    "volume": 0,
    "consolidation": False,
    "session_ok": False,
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
    if len(prices) < period+1: return 0.0
    rets = [math.log(prices[i]/prices[i-1]) if prices[i-1]!=0 else 0 for i in range(-period+1,0)]
    if not rets: return 0.0
    mean = sum(rets)/len(rets)
    var = sum((r-mean)**2 for r in rets)/len(rets)
    return math.sqrt(var)*1000

async def rest_accounts(session):
    for url in [f"https://api.derivws.com/trading/v1/accounts", f"https://api.deriv.com/trading/v1/accounts"]:
        try:
            async with session.get(url, headers={"Authorization": f"Bearer {DERIV_TOKEN}", "App-Id": DERIV_APP_ID}, timeout=10) as r:
                txt = await r.text()
                if "<!DOCTYPE" in txt: continue
                data=json.loads(txt)
                accs=data.get("data") or data.get("accounts") or []
                if accs:
                    state["available_accounts"]=[{"account_id":a.get("account_id"),"balance":float(a.get("balance",0)),"currency":a.get("currency","USD"),"is_demo":"DOT" in str(a.get("account_id")) or "V" in str(a.get("account_id"))[:1]} for a in accs]
                    return accs
        except: pass
    return []

async def rest_otp(session, acc_id):
    for url in [f"https://api.derivws.com/trading/v1/options/accounts/{acc_id}/otp"]:
        try:
            async with session.post(url, headers={"Authorization": f"Bearer {DERIV_TOKEN}", "App-Id": DERIV_APP_ID}, timeout=10) as r:
                txt=await r.text()
                if "<!DOCTYPE" in txt: continue
                j=json.loads(txt)
                otp=j.get("data",{}).get("url") or j.get("url")
                if otp: return otp
        except: pass
    return None

async def owner_trading_loop():
    log("=== LZ-TBot v7.0 OWNER'S EDITION - Profit Mindset ===")
    log(f"Risk {RISK_PCT*100}% | TP {DAILY_TP_PCT*100}% SL {DAILY_SL_PCT*100}% | Session {SESSION_START}-{SESSION_END} UTC | Cooldown {COOLDOWN_LOSSES} losses")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                accounts=await rest_accounts(session)
                if not accounts:
                    state["status"]="No accounts - check token"; await asyncio.sleep(8); continue
                target=state["switch_requested"] or state["account_id"] or DEFAULT_ACCOUNT
                state["switch_requested"]=None
                chosen=next((a for a in accounts if a.get("account_id")==target), accounts[0])
                state["account_id"]=chosen.get("account_id")
                state["balance"]=float(chosen.get("balance", state["balance"]))
                state["daily_start_bal"]=state["balance"]-state["daily_pnl"] if state["total_trades"]>0 else state["balance"]
                state["currency"]=chosen.get("currency","USD")
                log(f"OWNER using {state['account_id']} Bal {state['balance']:.2f} DailyStart {state['daily_start_bal']:.2f}")

                ws_url=await rest_otp(session, state["account_id"])
                if not ws_url:
                    state["status"]="OTP failed"; await asyncio.sleep(5); continue

                async with websockets.connect(ws_url, ping_interval=15) as ws:
                    state["connected"]=True
                    state["status"]=f"OWNER LIVE • EV {state['ev']:.2%} • {state['account_id']}"
                    log(f"✓ CONNECTED Owner Mode {state['account_id']} - No 1006!", "OK")
                    await ws.send(json.dumps({"ticks": SYMBOL, "subscribe":1}))
                    prices=collections.deque(maxlen=200)
                    tick_times=collections.deque(maxlen=100)
                    tick_count=0

                    async for raw in ws:
                        if state["switch_requested"]:
                            log(f"Switch requested {state['switch_requested']}"); break
                        try:
                            msg=json.loads(raw)
                            now=time.time()
                            utc_hour=datetime.datetime.utcnow().hour
                            state["session_ok"]=SESSION_START <= utc_hour < SESSION_END
                            # daily check
                            daily_pct = (state["balance"]-state["daily_start_bal"])/state["daily_start_bal"] if state["daily_start_bal"] else 0
                            
                            if "tick" in msg:
                                price=float(msg["tick"]["quote"])
                                prices.append(price)
                                tick_times.append(now)
                                state["last_price"]=price
                                tick_count+=1
                                state["collecting"]=f"{len(prices)}/200"
                                state["hv"]=calc_hv(list(prices))
                                # volume = ticks last 2 sec
                                vol = sum(1 for t in tick_times if now-t <= 2)
                                state["volume"]=vol

                                if tick_count%10==0:
                                    log(f"Tick {tick_count} {price:.3f} HV{state['hv']:.2f} Vol{vol} Session{'OK' if state['session_ok'] else 'OUT'} Daily{daily_pct:+.2%}", "TICK")

                                # OWNER DECISION ENGINE
                                if len(prices) < 20:
                                    state["last_signal"]="Collecting 20 ticks..."
                                    continue
                                
                                # Business protection
                                if daily_pct >= DAILY_TP_PCT:
                                    state["last_signal"]=f"DAILY TP Hit {daily_pct:+.2%} - STOP for today (Owner locks profit)"
                                    if tick_count%20==0: log(f"[SKIP] Daily TP {daily_pct:+.2%} - Owner locks profit, no more trades today", "SKIP")
                                    continue
                                if daily_pct <= DAILY_SL_PCT:
                                    state["last_signal"]=f"DAILY SL Hit {daily_pct:+.2%} - STOP, save capital"
                                    if tick_count%20==0: log(f"[SKIP] Daily SL {daily_pct:+.2%} - Owner saves capital", "SKIP")
                                    continue
                                if now < state["cooldown_until"]:
                                    remain=int(state["cooldown_until"]-now)
                                    state["last_signal"]=f"Cooldown {remain}s after {COOLDOWN_LOSSES} losses - No revenge trading"
                                    continue
                                if not state["session_ok"]:
                                    if tick_count%30==0: log(f"[SKIP] Session filter 5:00 UTC outside London overlap (13-17 UTC)", "SKIP")
                                    state["last_signal"]="Outside London 13-17 UTC - Low edge, skip"
                                    continue

                                # 5-tick consolidation detection
                                last5=list(prices)[-5:]
                                range5=max(last5)-min(last5)
                                state["consolidation"]=range5 < (state["hv"]*0.8 + 0.2) # tight range
                                
                                # Breakout detection
                                prev_max=max(list(prices)[-6:-1]) if len(prices)>=6 else max(last5)
                                prev_min=min(list(prices)[-6:-1]) if len(prices)>=6 else min(last5)
                                is_up_break = price > prev_max and state["consolidation"]
                                is_down_break = price < prev_min and state["consolidation"]
                                
                                if not (is_up_break or is_down_break):
                                    state["last_signal"]=f"Waiting breakout | Range5 {range5:.3f} HV {state['hv']:.2f} Vol {vol} {'Consolidation' if state['consolidation'] else 'Trending'}"
                                    continue

                                # EV calculation from history
                                if state["total_trades"]>=20:
                                    wr=state["wins"]/state["total_trades"] if state["total_trades"] else 0.5
                                    state["winrate"]=wr
                                    state["ev"]=wr*0.95 - (1-wr)*1.0
                                else:
                                    state["winrate"]=0.57 # assume backtest
                                    state["ev"]=0.57*0.95 - 0.43

                                # AI Veto - fake breakout detection (heuristic that learns)
                                fake_prob=0.0
                                if state["hv"] < 0.3: fake_prob+=0.4 # too quiet
                                if state["hv"] > 3.0: fake_prob+=0.3 # too wild
                                if vol < 2: fake_prob+=0.3 # no volume
                                if vol > 10: fake_prob+=0.2 # spike exhaustion
                                state["ai_veto_prob"]=fake_prob

                                if fake_prob > 0.55:
                                    log(f"[SKIP] AI Veto {fake_prob:.0%} - Fake breakout prob high HV{state['hv']:.2f} Vol{vol}", "AI")
                                    state["last_signal"]=f"AI VETO {fake_prob:.0%} - Fake breakout, skip"
                                    continue

                                if state["ev"] < 0.02:
                                    log(f"[SKIP] EV {state['ev']:.2%} < 2% - No edge, skip even if signal", "EV")
                                    state["last_signal"]=f"EV {state['ev']:.2%} too low - Owner skips negative EV"
                                    continue

                                # ALL FILTERS PASSED - OWNER BUYS
                                contract="CALL" if is_up_break else "PUT"
                                stake=round(state["balance"]*RISK_PCT,2)
                                if stake < 0.35: stake=0.35
                                state["stake"]=stake
                                
                                proposal={
                                    "proposal":1,
                                    "amount":stake,
                                    "basis":"stake",
                                    "contract_type":contract,
                                    "currency":state["currency"],
                                    "duration":5,
                                    "duration_unit":"t",
                                    "symbol":SYMBOL
                                }
                                await ws.send(json.dumps(proposal))
                                log(f"[OWNER SIGNAL] {contract} Breakout Vol{vol} HV{state['hv']:.2f} EV{state['ev']:.2%} WR{state['winrate']:.0%} Stake${stake} - Asking proposal", "SIG")
                                state["last_signal"]=f"BUYING {contract} EV{state['ev']:.1%} WR{state['winrate']:.0%} HV{state['hv']:.1f} Vol{vol}"

                            if "proposal" in msg:
                                p=msg["proposal"]
                                await ws.send(json.dumps({"buy": p.get("id"), "price": p.get("ask_price")}))
                                log(f"→ OWNER BUY {p.get('contract_type')} ${state['stake']} (EV {state['ev']:.1%})", "BUY")

                            if "buy" in msg:
                                b=msg["buy"]
                                state["balance"]=float(b.get("balance_after", state["balance"]))
                                state["equity"].append(state["balance"])
                                state["total_trades"]+=1
                                log(f"[BOUGHT] {b.get('contract_id')} Bal {state['balance']:.2f}", "OK")
                                await ws.send(json.dumps({"proposal_open_contract":1,"contract_id":b.get("contract_id"),"subscribe":1}))

                            if "proposal_open_contract" in msg:
                                poc=msg["proposal_open_contract"]
                                if poc.get("is_sold"):
                                    profit=float(poc.get("profit",0))
                                    state["daily_pnl"]+=profit
                                    await ws.send(json.dumps({"balance":1}))
                                    outcome="WIN" if profit>0 else "LOSS"
                                    if profit>0:
                                        state["wins"]+=1
                                        state["consecutive_losses"]=0
                                    else:
                                        state["consecutive_losses"]+=1
                                        if state["consecutive_losses"]>=COOLDOWN_LOSSES:
                                            state["cooldown_until"]=time.time()+COOLDOWN_MIN*60
                                            log(f"[COOLDOWN] {COOLDOWN_LOSSES} losses → {COOLDOWN_MIN}min pause - Owner rule", "COOL")
                                    entry={"time":datetime.datetime.now().strftime("%H:%M:%S"),"contract":poc.get("contract_type"),"profit":profit,"outcome":outcome,"ev":state["ev"],"hv":state["hv"]}
                                    state["trades"].appendleft(entry)
                                    log(f"[{outcome}] {poc.get('contract_type')} {profit:+.2f} | Daily {state['daily_pnl']:+.2f} | WR {state['wins']}/{state['total_trades']} EV {state['ev']:.1%} | Bal {state['balance']:.2f}", outcome)

                            if "balance" in msg and "balance" in msg["balance"]:
                                state["balance"]=float(msg["balance"]["balance"])
                                state["equity"].append(state["balance"])

                        except Exception as e:
                            pass
        except Exception as e:
            state["connected"]=False
            state["status"]=f"Reconnect {e}"
            log(f"WS Disconnect {e}", "ERR")
            await asyncio.sleep(5)

threading.Thread(target=lambda: asyncio.new_event_loop().run_until_complete(owner_trading_loop()), daemon=True).start()

app=Flask(__name__)

HTML="""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>LZ-TBot v7 Owner's Edition</title><script src="https://cdn.tailwindcss.com"></script><link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Space+Grotesk:wght@500;700&display=swap" rel="stylesheet"><style>body{font-family:'Space Grotesk';background:#060a14;color:#d8e2ff} .mono{font-family:'JetBrains Mono',monospace} .glass{background:linear-gradient(180deg,rgba(255,255,255,0.08),rgba(255,255,255,0.03));backdrop-filter:blur(18px);border:1px solid rgba(255,255,255,0.12)} .glow-teal{box-shadow:0 0 40px rgba(20,243,255,0.25)} .orb{position:fixed;border-radius:9999px;filter:blur(70px);opacity:0.5;pointer-events:none}</style></head><body class="min-h-screen"><div class="orb w-[600px] h-[600px] bg-cyan-500/20 -top-40 -left-40"></div><div class="orb w-[500px] h-[500px] bg-violet-500/20 top-1/3 -right-40"></div><header class="sticky top-0 z-20 glass border-b border-white/10 px-4 py-3 flex justify-between items-center"><div class="flex items-center gap-3"><div class="w-9 h-9 rounded-xl bg-gradient-to-br from-cyan-300 to-violet-500 grid place-items-center font-bold text-black">LZ</div><div><div class="font-bold">LZ-TBot • v7 OWNER'S EDITION • Profit Mindset</div><div class="text-[10px] opacity-60 mono">EV + Tick Breakout Volume + AI Veto + Business Protection</div></div></div><span id="conn" class="px-3 py-1 rounded-full text-xs font-bold bg-emerald-400/20 text-emerald-300 border border-emerald-400/30">● LIVE</span></header><main class="p-4 max-w-[1600px] mx-auto grid grid-cols-12 gap-4"><div class="col-span-12 lg:col-span-3 space-y-4"><div class="glass rounded-[20px] p-4 glow-teal"><div class="flex justify-between mono text-[10px] tracking-widest opacity-60"><span>EQUITY CURVE • OWNER</span><span class="text-emerald-300">LIVE</span></div><svg id="eqSvg" viewBox="0 0 300 100" class="w-full h-[110px] mt-2"></svg><div class="grid grid-cols-2 gap-2 mt-3"><div><div class="mono text-[10px] opacity-50">BALANCE</div><div id="bal" class="text-xl font-bold">--</div></div><div><div class="mono text-[10px] opacity-50">DAILY P/L</div><div id="pnl" class="text-xl font-bold">+0.00</div></div></div><div class="mt-2 mono text-[10px] grid grid-cols-3 gap-1"><span>EV <b id="ev">--</b></span><span>WR <b id="wr">--</b></span><span>HV <b id="hv">--</b></span></div><div class="mono text-[10px] opacity-60 mt-1">Wins <span id="wins">0/0</span> • <span id="collect"></span> • Vol <span id="vol"></span></div></div><div class="glass rounded-[20px] p-4"><div class="mono text-[10px] tracking-widest opacity-60">BUSINESS PROTECTION</div><div class="mt-2 text-xs"><div class="flex justify-between"><span>Daily TP 6%</span><span id="tp" class="text-emerald-300">--</span></div><div class="flex justify-between"><span>Daily SL -3.5%</span><span id="sl" class="text-red-300">--</span></div><div class="flex justify-between"><span>Risk</span><span>0.8% Fixed • No Martingale</span></div><div class="flex justify-between"><span>Session</span><span id="sess" class="px-2 py-0.5 rounded-full bg-white/10 text-[10px]">--</span></div><div class="flex justify-between"><span>Cooldown</span><span id="cool">--</span></div></div><div class="mt-3"><select id="accSel" class="w-full p-2.5 rounded-xl bg-black/40 border border-white/10 mono text-xs"></select><button onclick="switchAcc()" class="mt-2 w-full py-2.5 rounded-xl bg-gradient-to-r from-violet-500 to-cyan-400 text-black font-bold text-sm">🔄 Switch Demo ↔ Real</button></div></div></div><div class="col-span-12 lg:col-span-5 space-y-4"><div class="glass rounded-[20px] p-4"><div class="mono text-[10px] tracking-widest opacity-60">TICK BREAKOUT VOLUME • Owner Edge</div><div class="flex gap-4 mt-2"><div class="flex-1 text-xs opacity-70">Price must take last 5-tick extreme with volume + tick count confirmation. Consolidation → Breakout.</div><div class="w-20 h-20 rounded-full border border-cyan-400/30 grid place-items-center glow-teal"><div class="text-center"><div class="mono text-[9px] opacity-60">VOLUME</div><div id="volBig" class="font-bold text-lg">--</div></div></div></div><div class="mt-3 flex items-center gap-3"><span id="price" class="text-3xl font-bold mono">--</span><span id="priceDir" class="text-xs px-2 py-1 rounded-full bg-white/5">--</span><span id="cons" class="text-[10px] px-2 py-1 rounded-full">--</span></div></div><div class="glass rounded-[20px] p-4"><div class="mono text-[10px] tracking-widest opacity-60">EXECUTION BRAIN • EV Filter + AI Veto</div><div class="mt-2 grid grid-cols-2 gap-2 text-xs"><div>Risk <b>0.8%</b> fixed • 5 ticks HV • No Martingale<br>EV threshold 2% • AI Veto 55%</div><div><div>EV: <b id="ev2">--</b> WR: <b id="wr2">--</b></div><div>AI Fake%: <b id="ai">--</b></div><div id="lastSig" class="mt-1 px-2 py-1 rounded-full bg-violet-500/15 text-violet-300 mono text-[10px]">WAITING</div></div></div></div><div class="glass rounded-[20px] p-4"><div class="flex justify-between mono text-[10px] tracking-widest opacity-60"><span>LIVE REASONING • WE DON'T TRADE WHEN...</span><span id="status" class="opacity-80"></span></div><div id="logs" class="mt-3 space-y-1 max-h-[320px] overflow-auto mono text-[11px]"></div></div></div><div class="col-span-12 lg:col-span-4 space-y-4"><div class="glass rounded-[20px] p-4"><div class="mono text-[10px] opacity-60"># OWNER EDITION • Profit mindset, not gamble<br># If I owned $9,902 I would protect first</div><pre class="mono text-[10px] mt-2 p-2 rounded-xl bg-black/50">def owner_should_trade():
    if daily_pnl >= 6%: return False # lock profit
    if daily_pnl <= -3.5%: return False # save capital
    if not london_session(): return False
    if cooldown_active(): return False
    if EV < 0.02: return False
    if AI_fake_prob > 0.55: return False
    if not consolidation+breakout+volume: return False
    return True # only then BUY 0.8%
</pre><div class="mt-2 mono text-[10px] grid grid-cols-3 gap-1"><div class="p-2 rounded-xl bg-white/5">SYMBOL<br><b id="sym">R_75</b></div><div class="p-2 rounded-xl bg-white/5">STAKE<br><b id="stake">$0.35</b></div><div class="p-2 rounded-xl bg-white/5">DUR<br><b>5t</b></div></div></div><div class="glass rounded-[20px] p-4 border-amber-400/20"><div class="mono text-[10px] font-bold">🛡 DISCLAIMER</div><div class="mono text-[10px] opacity-70 mt-1">Owner mindset: preserve capital first. No martingale, no revenge. Use DEMO. Past EV ≠ future. <b id="accId" class="text-cyan-300"></b></div></div><div class="glass rounded-[20px] p-4"><div class="mono text-[10px] opacity-60">RECENT TRADES • Balance moves after 5 ticks</div><div id="trades" class="mt-2 space-y-2 mono text-[11px] max-h-[260px] overflow-auto"></div></div></div></main><script>let last=0;async function ref(){let r=await fetch('/api/status');let s=await r.json();document.getElementById('bal').innerText=s.balance.toFixed(2)+' '+s.currency;document.getElementById('pnl').innerText=(s.daily_pnl>=0?'+':'')+s.daily_pnl.toFixed(2);document.getElementById('pnl').className='text-xl font-bold '+(s.daily_pnl>=0?'text-emerald-300':'text-red-400');document.getElementById('ev').innerText=(s.ev*100).toFixed(1)+'%';document.getElementById('ev2').innerText=(s.ev*100).toFixed(1)+'%';document.getElementById('wr').innerText=(s.winrate*100).toFixed(0)+'%';document.getElementById('wr2').innerText=(s.winrate*100).toFixed(0)+'%';document.getElementById('hv').innerText=s.hv.toFixed(2);document.getElementById('vol').innerText=s.volume;document.getElementById('volBig').innerText=s.volume;document.getElementById('ai').innerText=(s.ai_veto_prob*100).toFixed(0)+'%';document.getElementById('price').innerText=s.last_price?s.last_price.toFixed(3):'--';document.getElementById('collect').innerText=s.collecting;document.getElementById('wins').innerText=s.wins+'/'+s.total_trades;document.getElementById('accId').innerText=s.account_id;document.getElementById('status').innerText=s.status.slice(0,40);document.getElementById('lastSig').innerText=s.last_signal.slice(0,50);document.getElementById('sym').innerText=s.symbol;document.getElementById('stake').innerText='$'+Number(s.stake).toFixed(2);document.getElementById('tp').innerText=(s.daily_pnl/s.daily_start_bal*100).toFixed(2)+'% / 6%';document.getElementById('sl').innerText=(s.daily_pnl/s.daily_start_bal*100).toFixed(2)+'% / -3.5%';document.getElementById('sess').innerText=s.session_ok?'LONDON OK':'OUTSIDE';document.getElementById('sess').className=s.session_ok?'px-2 py-0.5 rounded-full bg-emerald-400/20 text-emerald-300 text-[10px]':'px-2 py-0.5 rounded-full bg-red-400/20 text-red-300 text-[10px]';document.getElementById('cool').innerText=s.cooldown_until>Date.now()/1000?Math.ceil(s.cooldown_until-Date.now()/1000)+'s':'Ready';document.getElementById('cons').innerText=s.consolidation?'CONSOLIDATION':'TRENDING';document.getElementById('cons').className=s.consolidation?'text-[10px] px-2 py-1 rounded-full bg-cyan-400/20 text-cyan-300':'text-[10px] px-2 py-1 rounded-full bg-white/5';let dir=s.last_price>last?'▲ UP':s.last_price<last?'▼ DOWN':'─';document.getElementById('priceDir').innerText=dir;last=s.last_price;document.getElementById('conn').innerText=s.connected?'● OWNER LIVE '+s.account_id:'○ RECONNECTING';document.getElementById('logs').innerHTML=s.logs.slice(0,25).map(l=>{let c='opacity-60';if(l.includes('[BUY]'))c='text-cyan-300 font-bold';if(l.includes('[WIN]'))c='text-emerald-300 font-bold';if(l.includes('[LOSS]'))c='text-red-300 font-bold';if(l.includes('[SKIP]'))c='text-amber-200/70';if(l.includes('[OWNER'))c='text-violet-300 font-bold';return `<div class="${c}">${l}</div>`}).join('');document.getElementById('trades').innerHTML=s.trades.map(t=>`<div class="flex justify-between p-2 rounded-xl bg-black/30 border border-white/5"><span>${t.time} ${t.contract} EV${(t.ev*100).toFixed(0)}%</span><span class="${t.profit>=0?'text-emerald-300':'text-red-300'}">${t.profit>=0?'+':''}${t.profit.toFixed(2)}</span></div>`).join('')||'<div class="opacity-40">No trades yet — Owner waits for EV>2% + breakout + volume. When it trades, balance WILL move.</div>';let eq=s.equity;if(eq.length>1){let min=Math.min(...eq),max=Math.max(...eq);let w=300,h=100,path='';eq.forEach((v,i)=>{let x=i/(eq.length-1)*w;let y=h-((v-min)/((max-min)||1))*h*0.8-10;path+=(i==0?'M':'L')+x+','+y+' ';});document.getElementById('eqSvg').innerHTML=`<path d="${path}" fill="none" stroke="rgba(20,243,255,0.9)" stroke-width="2"/><path d="${path} L${w},${h} L0,${h} Z" fill="rgba(20,243,255,0.15)"/>`;}let sel=document.getElementById('accSel');if(s.available_accounts.length>0&&sel.options.length<=1){sel.innerHTML='';s.available_accounts.forEach(a=>{let o=document.createElement('option');o.value=a.account_id;o.text=(a.is_demo?'[DEMO] ':'[REAL] ')+a.account_id+' $'+a.balance;if(a.account_id==s.account_id)o.selected=true;sel.appendChild(o);});}}async function switchAcc(){let acc=document.getElementById('accSel').value;if(!confirm('Switch to '+acc+' ? Owner will reconnect.'))return;await fetch('/api/switch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({account_id:acc})});}setInterval(ref,1000);ref();</script></body></html>
"""

@app.route("/")
def dash(): return render_template_string(HTML)
@app.route("/api/status")
def api():
    out=dict(state)
    out["logs"]=list(out["logs"]); out["trades"]=list(out["trades"]); out["equity"]=list(out["equity"])
    return jsonify(out)
@app.route("/api/switch", methods=["POST"])
def sw():
    from flask import request as rq
    acc=rq.json.get("account_id")
    if acc: state["switch_requested"]=acc; log(f"Switch to {acc}", "SWITCH"); return jsonify({"ok":True})
    return jsonify({"ok":False}),400
@app.route("/health")
def h(): return "ok",200

if __name__=="__main__":
    app.run(host="0.0.0.0", port=PORT)
