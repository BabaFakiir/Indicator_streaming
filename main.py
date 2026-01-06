import time
import datetime as dt
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from broadcaster import broadcast


from config import *
from candle_aggregator import CandleAggregator
from ema_calculator import calculate_indicators
from tell_trend import tell_trend
import pandas as pd

# =========================
# GLOBAL STATE
# =========================

aggregator = CandleAggregator()
# token -> last indicators
LAST_INDICATORS = {}
# token -> last trend (BULLISH, BEARISH, SIDEWAYS, or None)
LAST_TRENDS = {}


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
    print("Tick processor: Starting WebSocket connection to SmartAPI...")
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
                    ts = (
                        dt.datetime.fromtimestamp(ts_ms / 1000, tz=IST)
                        if ts_ms else dt.datetime.now(IST)
                    )

                    # ---- Market hours ----
                    if not in_market_hours(ts):
                        return

                    # ---- Guard: token must be in SYMBOLS ----
                    if token not in SYMBOLS:
                        return
                    
                    symbol = SYMBOLS[token]

                    # ---- Candle aggregation (may or may not close candle) ----
                    candles = aggregator.process_tick(token, price, ts)

                    # ---- Indicator update ONLY on candle close ----
                    if candles is not None:
                        # Calculate EMA indicators for ema_crossover strategy
                        indicators = calculate_indicators(candles)
                        if indicators:
                            LAST_INDICATORS[token] = indicators
                        
                        # Calculate trend for stock-15min strategy (need at least 20 candles)
                        if len(candles) >= 20:
                            try:
                                # Convert candles to DataFrame for tell_trend
                                df = pd.DataFrame(list(candles))
                                trend = tell_trend(df)
                                LAST_TRENDS[token] = trend
                            except Exception as e:
                                print(f"Trend calculation error for {symbol}: {e}")

                    # ---- Fetch last known indicators and trend ----
                    last = LAST_INDICATORS.get(token)
                    trend = LAST_TRENDS.get(token)

                    # ---- STREAM EVERY TICK ----
                    # Broadcast for ema_crossover strategy
                    indicator_time = None
                    if last and last.get("time"):
                        indicator_time = last.get("time").isoformat()
                    
                    broadcast({
                        "symbol": symbol,
                        "token": token,
                        "timestamp": ts.isoformat(),
                        "price": price,
                        "ema9": last.get("ema9") if last else None,
                        "ema21": last.get("ema21") if last else None,
                        "rsi14": last.get("rsi14") if last else None,
                        "indicator_time": indicator_time
                    }, strategy="ema_crossover")
                    
                    # Broadcast for stock-15min strategy
                    broadcast({
                        "symbol": symbol,
                        "token": token,
                        "timestamp": ts.isoformat(),
                        "price": price,
                        "trend": trend
                    }, strategy="stock-15min")


                except Exception as e:
                    import traceback
                    print(f"Tick processing error for token {token if 'token' in locals() else 'unknown'}: {e}")
                    print(f"Error details: {traceback.format_exc()}")
                    print(f"Message: {msg}")



            ws.on_open = on_open
            ws.on_data = lambda wsapp, msg: on_message(msg)


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
