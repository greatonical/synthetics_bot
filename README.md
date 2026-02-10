# Deriv Synthetics EMA+RSI Bot (Multipliers, Trailing, Daily Cap)

A practical Deriv bot that trades Synthetic Indices with:
- EMA+RSI trend-following signals
- Multiplier orders (MULTUP/MULTDOWN) using proposal → buy → monitor → sell
- Manual trailing stop (P/L-based)
- Daily loss cap
- Tiny CLI dashboard
- Stake sizing by fixed USD or % of balance (with cap)
- Single or multi-symbol scan

## Quickstart (Demo)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env:
#  - Put your DEMO token in DERIV_API_TOKEN
#  - Pick SYMBOL=R_75 (or R_50,R_75,R_100)
#  - Keep LIVE=false to test paper behavior first
python bot.py