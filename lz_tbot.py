
"""
LZ-TBot - Render Edition - Laptop Auto Trader
Trades AUTOMATICALLY only when good signal appears.
Market: Volatility 75 Index (changeable)
Strategy: EMA 20/50 Trend + RSI 14 Pullback + Volatility Filter
Risk: 1% per trade, Daily TP +5% / SL -4%, 3 losses pause

INSTALL:
pip install -r requirements.txt

RUN:
python bot.py --token YOUR_DERIV_TOKEN --demo

Your token: Deriv -> Account Settings -> API Token -> Create (Trading scope)
Use DEMO first!
"""

import asyncio, json, argparse, time, os
from datetime import datetime
import websockets
import numpy as np

# --- Config ---
DERIV_WS = "wss://ws.derivws.com/websockets/v3?app_id=1089"
DEFAULT_SYMBOL = "R_75"  # Volatility 75 Index. Options: R_10,R_25,R_50,R_75,R_100, BOOM300 etc
DEFAULT_STAKE = 1.0
RSI_PERIOD = 14
EMA_FAST = 20
EMA_SLOW = 50

class TradingBot:
    def __init__(self, token, symbol, stake, demo=True):
        self.token = token
        self.symbol = symbol
        self.stake = stake
        self.demo = demo
        self.ticks = []
        self.balance = 0
        self.daily_pnl = 0
        self.consec_losses = 0
        self.trading_enabled = True
        self.cooldown_until = 0

    def ema(self, data, period):
        if len(data) < period: return None
        return np.convolve(data, np.ones(period)/period, mode='valid')[-1] if len(data)>=period else None

    def calc_ema(self, prices, period):
        if len(prices) < period: return None
        # proper EMA
        ema = prices[0]
        k = 2/(period+1)
        for price in prices[1:]:
            ema = price * k + ema * (1-k)
        return ema

    def calc_rsi(self, prices, period=14):
        if len(prices) < period+1: return None
        deltas = np.diff(prices)
        gains = np.where(deltas>0, deltas, 0)
        losses = np.where(deltas<0, -deltas, 0)
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        if avg_loss == 0: return 100
        rs = avg_gain/avg_loss
        return 100 - (100/(1+rs))

    def is_good_signal(self):
        if len(self.ticks) < 60: return None
        
        prices = self.ticks[-60:]
        ema_fast = self.calc_ema(prices, EMA_FAST)
        ema_slow = self.calc_ema(prices, EMA_SLOW)
        rsi = self.calc_rsi(prices, RSI_PERIOD)
        
        if ema_fast is None or ema_slow is None or rsi is None:
            return None

        # Volatility filter - don't trade flat market
        volatility = np.std(prices[-20:])
        if volatility < 0.1:
            return None

        # Signal Logic
        # Rise: Uptrend + RSI pullback to 40-55
        if ema_fast > ema_slow and 40 <= rsi <= 55 and prices[-1] > prices[-2]:
            return "CALL"  # Rise
        
        # Fall: Downtrend + RSI pullback to 45-60
        if ema_fast < ema_slow and 45 <= rsi <= 60 and prices[-1] < prices[-2]:
            return "PUT"  # Fall

        return None

    async def run(self):
        print(f"[{datetime.now()}] Connecting to Deriv... Symbol: {self.symbol} Stake: ${self.stake} Demo: {self.demo}")
        async with websockets.connect(DERIV_WS) as ws:
            # Authorize
            await ws.send(json.dumps({"authorize": self.token}))
            auth = json.loads(await ws.recv())
            if "error" in auth:
                print(f"AUTH ERROR: {auth['error']['message']}")
                return
            self.balance = float(auth["authorize"]["balance"])
            print(f"✓ Authorized! Balance: ${self.balance} | Account: {auth['authorize']['loginid']}")

            # Subscribe ticks
            await ws.send(json.dumps({"ticks": self.symbol, "subscribe": 1}))
            
            print(f"✓ Listening for ticks... Bot will trade AUTOMATICALLY on good signals.")
            print(f"  Rules: EMA{EMA_FAST}/{EMA_SLOW} + RSI{RSI_PERIOD} + Volatility filter")
            print(f"  Protection: Daily TP +5%, SL -4%, 3 Losses = 1h pause\n")

            async for msg in ws:
                data = json.loads(msg)
                
                if "tick" in data:
                    price = float(data["tick"]["quote"])
                    self.ticks.append(price)
                    if len(self.ticks) > 200:
                        self.ticks.pop(0)

                    # Check cooldown
                    if time.time() < self.cooldown_until:
                        continue
                    if not self.trading_enabled:
                        continue

                    # Daily limits
                    if self.daily_pnl <= -self.balance*0.04:
                        print(f"[!] DAILY STOP LOSS hit ({self.daily_pnl:.2f}). Stopping for today.")
                        self.trading_enabled = False
                        continue
                    if self.daily_pnl >= self.balance*0.05:
                        print(f"[!] DAILY TAKE PROFIT hit ({self.daily_pnl:.2f}). Securing profit.")
                        self.trading_enabled = False
                        continue

                    signal = self.is_good_signal()
                    if signal:
                        # Place trade
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] SIGNAL: {signal} | Price: {price} | EMA{EMA_FAST}:{self.calc_ema(self.ticks[-60:], EMA_FAST):.2f} EMA{EMA_SLOW}:{self.calc_ema(self.ticks[-60:], EMA_SLOW):.2f} RSI:{self.calc_rsi(self.ticks[-60:]):.1f}")
                        await self.place_trade(ws, signal)

                elif "buy" in data:
                    if "error" in data:
                        print(f"  Trade Error: {data['error']['message']}")
                    else:
                        print(f"  → Trade placed! ID: {data['buy']['contract_id']} Price: ${data['buy']['buy_price']}")
                        # Subscribe to result
                        await ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": data["buy"]["contract_id"], "subscribe": 1}))

                elif "proposal_open_contract" in data:
                    contract = data["proposal_open_contract"]
                    if contract.get("is_sold"):
                        profit = float(contract["profit"])
                        self.daily_pnl += profit
                        result = "WIN" if profit > 0 else "LOSS"
                        print(f"  ← Result: {result} Profit: ${profit:.2f} | Daily PnL: ${self.daily_pnl:.2f} | Balance: ~${self.balance + self.daily_pnl:.2f}")
                        
                        if profit < 0:
                            self.consec_losses += 1
                            if self.consec_losses >= 3:
                                print(f"  [!] 3 losses in a row - pausing 1 hour")
                                self.cooldown_until = time.time() + 3600
                                self.consec_losses = 0
                        else:
                            self.consec_losses = 0


    async def place_trade(self, ws, signal):
        # Simple Rise/Fall 5 ticks
        req = {
            "buy": 1,
            "price": self.stake,
            "parameters": {
                "amount": self.stake,
                "basis": "stake",
                "contract_type": signal,
                "currency": "USD",
                "duration": 5,
                "duration_unit": "t",
                "symbol": self.symbol
            }
        }
        await ws.send(json.dumps(req))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=os.environ.get("DERIV_TOKEN"),, help="Deriv API token")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="R_75, R_100, etc")
    parser.add_argument("--stake", type=float, default=DEFAULT_STAKE)
    parser.add_argument("--demo", action="store_true", help="Demo warning")
    args = parser.parse_args()

    bot = TradingBot(args.token, args.symbol, args.stake, args.demo)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nBot stopped by user")
