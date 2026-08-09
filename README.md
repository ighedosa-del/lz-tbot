# LZ-TBot - Render.com Deploy

### 1. Push to GitHub
- Create new repo: github.com/new -> name lz-tbot
- Upload ALL files in this folder to repo

### 2. Deploy to Render (FREE)
1. Go to dashboard.render.com
2. Click New + -> Blueprint -> Connect your lz-tbot repo
3. It will read render.yaml automatically
4. When asked for DERIV_TOKEN, paste your Deriv Demo token first
5. Click Apply -> Deploy

Your bot will start in 30 seconds and trade AUTOMATICALLY when signal is good.

### 3. Logs
Render dashboard -> lz-tbot -> Logs
You will see:
[LZ-TBot] SIGNAL: CALL | Price: 1234.56 | EMA20:1230 EMA50:1220 RSI:52.3
-> Trade placed!
<- Result: WIN Profit: $0.33

### 4. Stop / Control
Render -> Suspend service to stop bot
Render -> Environment -> Change stake/symbol -> Redeploy

### Safety
- Token stored in Render Environment Variables, encrypted, never in code
- Start with DEMO token, test 3 days
- Real token: $100 balance, $0.35 stake only

### Change symbol/stake without code
Render -> Environment -> Add:
SYMBOL = R_100 or BOOM300 or CRASH500
STAKE = 1

We use fixed fractional 0.8% risk, D'Alembert disabled by default for safety.
