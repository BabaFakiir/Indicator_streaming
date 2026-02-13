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
# For BANKNIFTY: track first candle of day and ATM strike
BANKNIFTY_FIRST_CANDLE = {}  # token -> {"date": date, "open": price}
BANKNIFTY_ATM_STRIKE = {}  # token -> strike value


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

                    # ---- Track first candle of day for BANKNIFTY (for ATM strike calculation) ----
                    if symbol == "BANKNIFTY":
                        current_date = ts.date()
                        # Check if we need to calculate ATM strike for today
                        if token not in BANKNIFTY_FIRST_CANDLE or BANKNIFTY_FIRST_CANDLE[token].get("date") != current_date:
                            # When a candle closes, capture the first candle's open price
                            if candles is not None and len(candles) > 0:
                                # Get the oldest candle (first one in queue) - this is the first candle of the day
                                first_candle = candles[0]
                                first_candle_open = first_candle["open"]
                                
                                BANKNIFTY_FIRST_CANDLE[token] = {
                                    "date": current_date,
                                    "open": first_candle_open
                                }
                                
                                # Calculate ATM strike: convert to decimal, round to nearest 100
                                # Example: 6035300 / 100 = 60353, round(60353/100)*100 = 60400
                                price_decimal = first_candle_open / 100
                                atm_strike = round(price_decimal / 100) * 100
                                BANKNIFTY_ATM_STRIKE[token] = atm_strike

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
                        "ema34": last.get("ema34") if last else None,
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
                    
                    # Broadcast for bank_nifty_ema strategy (BANKNIFTY only)
                    if symbol == "BANKNIFTY":
                        atm_strike = BANKNIFTY_ATM_STRIKE.get(token)
                        broadcast({
                            "symbol": symbol,
                            "token": token,
                            "timestamp": ts.isoformat(),
                            "price": price,
                            "ema21": last.get("ema21") if last else None,
                            "ema34": last.get("ema34") if last else None,
                            "strike": atm_strike
                        }, strategy="bank_nifty_ema")


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
