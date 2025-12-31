import time
import datetime as dt
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from config import *
from candle_aggregator import CandleAggregator
from ema_calculator import calculate_indicators
from supabase_writer import upsert_indicators

# =========================
# GLOBAL STATE
# =========================

aggregator = CandleAggregator()

# =========================
# UTILITIES
# =========================

def in_market_hours(ts):
    t = ts.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE

def login():
    smart = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET).now()
    session = smart.generateSession(CLIENT_ID, MPIN, totp)
    return session["data"]["jwtToken"], session["data"]["feedToken"]

# =========================
# WEBSOCKET RUNNER
# =========================

def run_websocket():
    """
    Blocks forever.
    Handles reconnect + session regeneration.
    """
    while True:
        try:
            jwt, feed = login()
            ws = SmartWebSocketV2(jwt, API_KEY, CLIENT_ID, feed)

            def on_open(_):
                print("WebSocket connected")
                ws.subscribe(
                    correlation_id="ema",
                    mode=1,
                    token_list=[{
                        "exchangeType": 1,
                        "tokens": list(SYMBOLS.keys())
                    }]
                )

            def on_message(msg):
                try:
                    # ---- Guard: token ----
                    token = msg.get("token")
                    if token is None:
                        return
                    token = str(token)

                    # ---- Guard: price ----
                    price = msg.get("last_traded_price")
                    if price is None:
                        return
                    price = float(price)

                    # ---- Timestamp ----
                    ts_ms = msg.get("exchange_timestamp")
                    if ts_ms:
                        ts = dt.datetime.fromtimestamp(ts_ms / 1000, tz=IST)
                    else:
                        ts = dt.datetime.now(IST)

                    # ---- Market hours guard ----
                    if not in_market_hours(ts):
                        return

                    # ---- Candle aggregation ----
                    candles = aggregator.process_tick(token, price, ts)
                    if candles is None:
                        return

                    # ---- Indicator calculation ----
                    indicators = calculate_indicators(candles)
                    if not indicators:
                        return

                    symbol = SYMBOLS[token]

                    print(
                        f"[{indicators['time']}] {symbol} | "
                        f"EMA9={indicators['ema9']} | "
                        f"EMA21={indicators['ema21']} | "
                        f"RSI14={indicators['rsi14']}"
                    )

                    upsert_indicators(
                        symbol,
                        indicators["ema9"],
                        indicators["ema21"],
                        indicators["rsi14"]
                    )

                except Exception as e:
                    print("Tick processing error:", e, msg)


            ws.on_open = on_open
            ws.on_message = on_message

            # Force reconnect on ANY failure
            ws.on_error = lambda *_: (_ for _ in ()).throw(Exception("WebSocket error"))
            ws.on_close = lambda *_: (_ for _ in ()).throw(Exception("WebSocket closed"))

            ws.connect()

        except Exception as e:
            print("WebSocket crashed. Reconnecting in", RECONNECT_DELAY, "seconds:", e)
            time.sleep(RECONNECT_DELAY)

# =========================
# MAIN ENTRY POINT
# =========================

def main():
    print("EMA Engine starting...")
    run_websocket()

if __name__ == "__main__":
    main()
