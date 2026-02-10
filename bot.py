#!/usr/bin/env python3
import os, json, time, sys
from datetime import datetime, timezone, date
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from websocket import create_connection

load_dotenv()

# --- ENV ---
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN", "")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOL", "R_75").split(",") if s.strip()]
GRANULARITY = int(os.getenv("GRANULARITY", "60"))
LIVE = os.getenv("LIVE", "false").lower() == "true"

# Stake sizing
STAKE_MODE = os.getenv("STAKE_MODE", "percent")  # 'fixed' | 'percent'
STAKE_USD = float(os.getenv("STAKE_USD", "1"))
CAPITAL_ALLOCATION_PCT = float(os.getenv("CAPITAL_ALLOCATION_PCT", "0.01"))
STAKE_USD_CAP = float(os.getenv("STAKE_USD_CAP", "10"))

# Multipliers & risk
MULTIPLIER = int(os.getenv("MULTIPLIER", "50"))
MULT_TP_USD = float(os.getenv("MULT_TP_USD", "6"))
MULT_SL_USD = float(os.getenv("MULT_SL_USD", "3"))

# Trailing stop
TRAIL_START_USD = float(os.getenv("TRAIL_START_USD", "4"))
TRAIL_DISTANCE_USD = float(os.getenv("TRAIL_DISTANCE_USD", "2"))

# Daily loss cap
DAILY_LOSS_CAP_USD = float(os.getenv("DAILY_LOSS_CAP_USD", "20"))

# Strategy
EMA_FAST = int(os.getenv("EMA_FAST", "50"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "200"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_LONG_MIN = int(os.getenv("RSI_LONG_MIN", "50"))
RSI_SHORT_MAX = int(os.getenv("RSI_SHORT_MAX", "50"))
WARMUP_CANDLES = int(os.getenv("WARMUP_CANDLES", "250"))

# --- Endpoint with env override + fallbacks ---
DERIV_WS_URL = os.getenv(
    "DERIV_WS_URL",
    f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
)
FALLBACK_WS_URLS = [
    DERIV_WS_URL,
    f"wss://ws.binaryws.com/websockets/v3?app_id={DERIV_APP_ID}",  # legacy fallback
]

# --- Indicators ---
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain, index=series.index).rolling(window=period).mean()
    roll_down = pd.Series(loss, index=series.index).rolling(window=period).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100.0 - (100.0 / (1.0 + rs))

# --- Data model ---
@dataclass
class Candle:
    epoch: int
    open: float
    high: float
    low: float
    close: float

# --- WebSocket client ---
class DerivWS:
    def __init__(self, url: str = None):
        self.url = url or DERIV_WS_URL
        self.ws = None

    def connect(self):
        last_err = None
        for candidate in FALLBACK_WS_URLS:
            try:
                print(f"Trying {candidate}")
                self.ws = create_connection(candidate, timeout=30)
                self.url = candidate
                print(f"[OK] Connected via {candidate}")
                return
            except Exception as e:
                last_err = e
                print(f"[WARN] Connect failed for {candidate}: {e}")
                time.sleep(1)
        # If we reached here, all candidates failed
        raise RuntimeError(f"All WebSocket endpoints failed. Last error: {last_err}")

    def send(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.ws.send(json.dumps(payload))
        raw = self.ws.recv()
        return json.loads(raw)

    def authorize(self, token: str):
        if not token:
            return None
        return self.send({"authorize": token})

    def get_balance(self) -> Tuple[float, str]:
        resp = self.send({"balance": 1, "subscribe": 0})
        if "balance" in resp:
            bal = float(resp["balance"]["balance"])
            cur = resp["balance"]["currency"]
            return bal, cur
        return 0.0, "USD"

    def get_candles(self, symbol: str, count: int, granularity: int) -> List[Candle]:
        payload = {
            "ticks_history": symbol,
            "style": "candles",
            "granularity": granularity,
            "end": "latest",
            "count": count,
            "adjust_start_time": 1
        }
        resp = self.send(payload)
        if "candles" not in resp:
            raise RuntimeError(f"Bad candle response: {resp}")
        out: List[Candle] = []
        for c in resp["candles"]:
            out.append(Candle(
                epoch=int(c["epoch"]),
                open=float(c["open"]),
                high=float(c["high"]),
                low=float(c["low"]),
                close=float(c["close"]),
            ))
        return out

    # --- Multipliers order flow ---
    def proposal_multiplier(self, direction: str, symbol: str, stake_usd: float) -> Dict[str, Any]:
        contract_type = "MULTUP" if direction == "long" else "MULTDOWN"
        proposal = {
            "proposal": 1,
            "amount": round(stake_usd, 2),
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "symbol": symbol,
            "multiplier": MULTIPLIER,
            # ✅ TP/SL must be wrapped under limit_order for Multipliers
            "limit_order": {
                "take_profit": round(MULT_TP_USD, 2),
                "stop_loss": round(MULT_SL_USD, 2),
            },
        }
        return self.send(proposal)

    def buy(self, proposal_id: str, price: float) -> Dict[str, Any]:
        return self.send({"buy": proposal_id, "price": price})

    def subscribe_open_contract(self, contract_id: int) -> Dict[str, Any]:
        return self.send({"proposal_open_contract": 1, "contract_id": int(contract_id), "subscribe": 1})

    def sell(self, contract_id: int, price: float = 0) -> Dict[str, Any]:
        return self.send({"sell": int(contract_id), "price": price})
    
    
# --- Strategy ---
class EmaRsiStrategy:
    def build_dataframe(self, candles: List[Candle]) -> pd.DataFrame:
        df = pd.DataFrame([{
            "epoch": c.epoch,
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close
        } for c in candles]).set_index("epoch")
        df["ema_fast"] = ema(df["close"], EMA_FAST)
        df["ema_slow"] = ema(df["close"], EMA_SLOW)
        df["rsi"] = rsi(df["close"], RSI_PERIOD)
        return df

    def generate_signal(self, df: pd.DataFrame) -> Optional[str]:
        if len(df) < max(EMA_SLOW, WARMUP_CANDLES):
            return None
        last = df.iloc[-1]
        if last["ema_fast"] > last["ema_slow"] and last["rsi"] > RSI_LONG_MIN:
            return "long"
        if last["ema_fast"] < last["ema_slow"] and last["rsi"] < RSI_SHORT_MAX:
            return "short"
        return None

# --- Position model ---
@dataclass
class OpenPosition:
    symbol: str
    direction: str            # 'long' | 'short'
    contract_id: Optional[int]
    buy_price: Optional[float]   # contract purchase price
    entry_spot: Optional[float]  # underlying spot (paper mode)
    stake: float
    start_time: float
    trailing_active: bool = False
    trail_anchor_pl: float = 0.0  # highest PL seen (both dirs are positive PL when profit)
    last_pl: float = 0.0
    currency: str = "USD"

# --- Bot ---
class Bot:
    def __init__(self, ws: DerivWS):
        self.ws = ws
        self.strategy = EmaRsiStrategy()
        self.balance = 0.0
        self.currency = "USD"
        self.realized_today = 0.0
        self.day = date.today()
        self.open_pos: Optional[OpenPosition] = None

    # Wallet
    def refresh_balance(self):
        bal, cur = self.ws.get_balance()
        self.balance, self.currency = bal, cur

    # Day roll-over
    def daily_reset_if_needed(self):
        today = date.today()
        if today != self.day:
            self.realized_today = 0.0
            self.day = today

    # Loss cap
    def daily_cap_hit(self) -> bool:
        return -self.realized_today >= DAILY_LOSS_CAP_USD

    # Stake sizing
    def compute_stake(self) -> float:
        if STAKE_MODE == "fixed":
            return STAKE_USD
        return min(self.balance * CAPITAL_ALLOCATION_PCT, STAKE_USD_CAP)

    # Main trading decision
    def scan_and_trade(self):
        if self.daily_cap_hit():
            return "DAILY_CAP", None

        if self.open_pos is not None:
            return "MANAGE", self.open_pos.symbol

        # Scan symbols for a signal (first match wins this cycle)
        for sym in SYMBOLS:
            try:
                candles = self.ws.get_candles(sym, count=max(WARMUP_CANDLES, EMA_SLOW) + 5, granularity=GRANULARITY)
            except Exception as e:
                print(f"[WARN] Candle fetch error for {sym}: {e}")
                continue

            df = self.strategy.build_dataframe(candles)
            sig = self.strategy.generate_signal(df)
            if not sig:
                continue

            stake = round(self.compute_stake(), 2)
            if stake <= 0:
                return "NO_STAKE", sym

            if LIVE and DERIV_API_TOKEN:
                # Live (Demo/Real) multiplier path
                prop = self.ws.proposal_multiplier(sig, sym, stake)
                if "error" in prop:
                    print(f"[ORDER-ERR] proposal: {prop['error']}")
                    return "ORDER_ERR", sym

                proposal_id = prop.get("proposal", {}).get("id") or prop.get("proposal", {}).get("proposal_id")
                ask_price = float(prop.get("proposal", {}).get("ask_price", stake))
                if not proposal_id:
                    print(f"[ORDER-ERR] No proposal_id in response: {prop}")
                    return "ORDER_ERR", sym

                buy = self.ws.buy(proposal_id, ask_price)
                if "error" in buy:
                    print(f"[ORDER-ERR] buy: {buy['error']}")
                    return "ORDER_ERR", sym

                info = buy.get("buy", {})
                contract_id = info.get("contract_id")

                self.open_pos = OpenPosition(
                    symbol=sym, direction=sig, contract_id=contract_id,
                    buy_price=float(info.get("price", stake)), entry_spot=None,
                    stake=stake, start_time=time.time(), currency=self.currency
                )
                print(f"[LIVE-ENTER] {sig.upper()} {sym} stake={stake} contract_id={contract_id}")

                if contract_id:
                    self.ws.subscribe_open_contract(contract_id)
                return "ENTERED", sym

            else:
                # Paper entry
                self.open_pos = OpenPosition(
                    symbol=sym, direction=sig, contract_id=None, buy_price=None,
                    entry_spot=df['close'].iloc[-1], stake=stake, start_time=time.time(), currency=self.currency
                )
                print(f"[PAPER-ENTER] {sig.upper()} {sym} @ {self.open_pos.entry_spot:.5f} stake={stake}")
                return "ENTERED", sym

        return "NO_SIGNAL", None

    # Manage open position (live or paper)
    def manage_open(self):
        if self.open_pos is None:
            return

        # --- LIVE (Demo/Real) path ---
        if LIVE and self.open_pos.contract_id:
            try:
                upd = self.ws.send({"proposal_open_contract": 1, "contract_id": int(self.open_pos.contract_id)})
            except Exception as e:
                print(f"[WARN] open_contract poll error: {e}")
                return

            poc = upd.get("proposal_open_contract", {})
            current_pl = float(poc.get("profit", 0.0))
            status = poc.get("status")
            entry_spot = poc.get("entry_tick") or poc.get("entry_spot")
            buy_price = poc.get("buy_price")

            self.open_pos.last_pl = current_pl
            if entry_spot is not None:
                try: self.open_pos.entry_spot = float(entry_spot)
                except: pass
            if buy_price is not None:
                try: self.open_pos.buy_price = float(buy_price)
                except: pass

            # Trailing activation and check (profit is positive for both long/short)
            if current_pl >= TRAIL_START_USD:
                if not self.open_pos.trailing_active:
                    self.open_pos.trailing_active = True
                    self.open_pos.trail_anchor_pl = current_pl
                else:
                    self.open_pos.trail_anchor_pl = max(self.open_pos.trail_anchor_pl, current_pl)
                    if current_pl <= self.open_pos.trail_anchor_pl - TRAIL_DISTANCE_USD:
                        self.close_position_live("TRAIL")

            # Natural contract end
            if status in ("sold", "expired"):
                realized = current_pl
                self.realized_today += realized
                print(f"[LIVE-EXIT] status={status} realized={realized:+.2f} USD")
                self.open_pos = None

        # --- PAPER path ---
        else:
            try:
                candles = self.ws.get_candles(self.open_pos.symbol, count=2, granularity=GRANULARITY)
                last_close = candles[-1].close
            except Exception as e:
                print(f"[WARN] Paper candle refresh error: {e}")
                return

            # Crude paper P/L approximation in USD from ticks
            move = (last_close - self.open_pos.entry_spot) if self.open_pos.direction == "long" else (self.open_pos.entry_spot - last_close)
            pl_ticks = move * 10000.0
            current_pl_usd = (pl_ticks / 100.0) * (MULT_SL_USD / 3.0)  # soft scaling
            self.open_pos.last_pl = current_pl_usd

            # Trailing (paper)
            if current_pl_usd >= TRAIL_START_USD:
                if not self.open_pos.trailing_active:
                    self.open_pos.trailing_active = True
                    self.open_pos.trail_anchor_pl = current_pl_usd
                else:
                    self.open_pos.trail_anchor_pl = max(self.open_pos.trail_anchor_pl, current_pl_usd)
                    if current_pl_usd <= self.open_pos.trail_anchor_pl - TRAIL_DISTANCE_USD:
                        print("[PAPER-EXIT] TRAIL stop")
                        self.realized_today += current_pl_usd
                        self.open_pos = None
                        return

            # Hard TP/SL (paper)
            if current_pl_usd >= MULT_TP_USD:
                print(f"[PAPER-EXIT] TP {current_pl_usd:+.2f} USD")
                self.realized_today += current_pl_usd
                self.open_pos = None
            elif current_pl_usd <= -MULT_SL_USD:
                print(f"[PAPER-EXIT] SL {current_pl_usd:+.2f} USD")
                self.realized_today += current_pl_usd
                self.open_pos = None

    # Close live (Demo/Real) position by selling the contract
    def close_position_live(self, reason: str):
        if not (LIVE and self.open_pos and self.open_pos.contract_id):
            return
        try:
            sell_resp = self.ws.sell(self.open_pos.contract_id, 0)
            if "error" in sell_resp:
                print(f"[SELL-ERR] {sell_resp['error']}")
                return
            time.sleep(1)
            upd = self.ws.send({"proposal_open_contract": 1, "contract_id": int(self.open_pos.contract_id)})
            poc = upd.get("proposal_open_contract", {})
            realized = float(poc.get("profit", 0.0))
            self.realized_today += realized
            print(f"[LIVE-EXIT] {reason} realized={realized:+.2f} USD")
            self.open_pos = None
        except Exception as e:
            print(f"[SELL-ERR] exception: {e}")

    # CLI dashboard
    def dashboard(self):
        os.system("cls" if os.name == "nt" else "clear")
        print("=== Deriv EMA+RSI Multiplier Bot (Demo-ready) ===")
        print(f"Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Account: {self.currency} {self.balance:.2f} | Day Realized: {self.realized_today:+.2f} USD | Daily Cap: {DAILY_LOSS_CAP_USD:.2f} USD")
        print("STATUS:", "🔴 Daily loss cap reached — pausing new trades" if self.daily_cap_hit() else "🟢 Active")
        if self.open_pos:
            op = self.open_pos
            print(f"Open: {op.symbol} | {op.direction.upper()} | stake={op.stake} | entry_spot={op.entry_spot} | contract_id={op.contract_id}")
            print(f"Unrealized P/L: {op.last_pl:+.2f} USD | Trailing: {op.trailing_active} (anchor={op.trail_anchor_pl:+.2f})")
        else:
            print("Open: None")
        print(f"Symbols scan list: {', '.join(SYMBOLS)}")
        print(f"EMA({EMA_FAST}) / EMA({EMA_SLOW}) | RSI({RSI_PERIOD}) | TF={GRANULARITY}s")
        print("-"*60)

def main():
    ws = DerivWS()   # no need to pass WS_URL explicitly anymore
    print(f"Connecting to {ws.url}")
    ws.connect()

    if DERIV_API_TOKEN:
        auth = ws.authorize(DERIV_API_TOKEN)
        if auth and 'error' in auth:
            print(f"[WARN] Authorization failed: {auth}")
        else:
            print("[OK] Authorized.")
    else:
        print("[INFO] No API token provided — live orders disabled.")

    bot = Bot(ws)
    bot.refresh_balance()

    loop_sleep = max(GRANULARITY, 10)  # don't hammer the API

    while True:
        bot.daily_reset_if_needed()
        bot.dashboard()

        state, sym = bot.scan_and_trade()
        # state is informational; management happens below
        bot.manage_open()

        # refresh balance occasionally
        if int(time.time()) % 30 == 0:
            bot.refresh_balance()

        time.sleep(loop_sleep)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting...")