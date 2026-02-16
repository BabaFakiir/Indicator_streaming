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
BANKNIFTY_FIRST_CANDLE = {}  # token -> {"date": date, "open": price, "high": price, "low": price}
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
                        stored_date = BANKNIFTY_FIRST_CANDLE.get(token, {}).get("date")
                        
                        # Reset strike if it's a new day (explicit date comparison)
                        needs_reset = False
                        if stored_date is None:
                            needs_reset = True
                        elif isinstance(stored_date, dt.date):
                            needs_reset = stored_date != current_date
                        else:
                            # Handle case where stored_date might be a string or other type
                            needs_reset = True
                        
                        if needs_reset:
                            # Clear old strike for new day
                            BANKNIFTY_ATM_STRIKE[token] = None
                            BANKNIFTY_FIRST_CANDLE[token] = {}
                            
                            # Try to get the first candle of TODAY
                            first_candle_open = None
                            
                            # Option 1: Check if we have a closed candle for today
                            first_candle_data = None
                            if candles is not None and len(candles) > 0:
                                # Find the first candle that belongs to today
                                for candle in candles:
                                    candle_date = candle["time"].date()
                                    if candle_date == current_date:
                                        first_candle_data = candle
                                        break
                            
                            # Option 2: If no closed candle yet, check if we're in the first candle period
                            if first_candle_data is None:
                                # Check if we're in the first 5-minute period of the day (9:15-9:20)
                                if ts.hour == 9 and 15 <= ts.minute < 20:
                                    # This is the first candle of the day
                                    # Get data from the aggregator's current candle
                                    current_candle = aggregator.current.get(token)
                                    if current_candle and current_candle.get("time").date() == current_date:
                                        first_candle_data = current_candle
                            
                            # Store first candle data and calculate strike
                            if first_candle_data is not None:
                                first_candle_open = first_candle_data.get("open", price)
                                first_candle_high = first_candle_data.get("high", price)
                                first_candle_low = first_candle_data.get("low", price)
                                
                                BANKNIFTY_FIRST_CANDLE[token] = {
                                    "date": current_date,
                                    "open": first_candle_open,
                                    "high": first_candle_high,
                                    "low": first_candle_low
                                }
                                
                                # Calculate ATM strike: convert to decimal, round to nearest 100
                                # Example: 5994780 / 100 = 59947.8, round(59947.8/100)*100 = round(599.478)*100 = 599*100 = 59900
                                price_decimal = first_candle_open / 100
                                atm_strike = round(price_decimal / 100) * 100
                                BANKNIFTY_ATM_STRIKE[token] = atm_strike
                        else:
                            # Update high/low if we're still in the first candle of the day
                            # Check if we're currently in the first candle period
                            if ts.hour == 9 and 15 <= ts.minute < 20:
                                current_candle = aggregator.current.get(token)
                                if current_candle:
                                    candle_time = current_candle.get("time")
                                    if candle_time and candle_time.date() == current_date and candle_time.hour == 9 and candle_time.minute == 15:
                                        # Update high and low as the candle is being built
                                        if token in BANKNIFTY_FIRST_CANDLE:
                                            BANKNIFTY_FIRST_CANDLE[token]["high"] = current_candle.get("high", price)
                                            BANKNIFTY_FIRST_CANDLE[token]["low"] = current_candle.get("low", price)
                            # Also update when first candle closes
                            elif candles is not None and len(candles) > 0:
                                # Check if the first candle of today just closed
                                for candle in candles:
                                    candle_date = candle["time"].date()
                                    if candle_date == current_date and candle["time"].hour == 9 and candle["time"].minute == 15:
                                        # First candle closed, update with final high/low
                                        if token in BANKNIFTY_FIRST_CANDLE:
                                            BANKNIFTY_FIRST_CANDLE[token]["high"] = candle.get("high")
                                            BANKNIFTY_FIRST_CANDLE[token]["low"] = candle.get("low")
                                        break

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
                        first_candle = BANKNIFTY_FIRST_CANDLE.get(token, {})
                        broadcast({
                            "symbol": symbol,
                            "token": token,
                            "timestamp": ts.isoformat(),
                            "price": price,
                            "ltp": price,
                            "ema21": last.get("ema21") if last else None,
                            "ema34": last.get("ema34") if last else None,
                            "strike": atm_strike,
                            "high": first_candle.get("high"),
                            "low": first_candle.get("low")
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
