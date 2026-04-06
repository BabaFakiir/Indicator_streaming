import time as pytime
import datetime as dt
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from broadcaster import broadcast


from config import *  # SYMBOLS, NSE_TOKENS, NFO_TOKENS, STOCK_FUT_GAPDOWN_PCT, etc.
from candle_aggregator import CandleAggregator
from ema_calculator import calculate_indicators
from tell_trend import tell_trend
import pandas as pd

import stock_fut_breakout_cache as stock_fut_cache

# =========================
# GLOBAL STATE
# =========================

aggregator = CandleAggregator()
# token -> last indicators
LAST_INDICATORS = {}
# token -> last trend (BULLISH, BEARISH, SIDEWAYS, or None)
LAST_TRENDS = {}
# token -> first 15-minute candle range for non-BANKNIFTY stocks (stock-15min strategy)
# We only populate High/Low after the first 15 minutes are complete (9:15-9:30).
FIRST_15_CANDLE_OF_DAY = {}  # token -> {"date": date, "high": price, "low": price}
# token -> whether price has breached the first 15-minute range after 9:30
STOCK_15_BREACHED = {}  # token -> bool
# token -> last traded price (for options LTP when forwarding from underlying ticks)
LAST_PRICE = {}
# For BANKNIFTY: track first candle of day and ATM strike
# Track first candle for all BANKNIFTY symbols (underlying and options)
FIRST_CANDLE_OF_DAY = {}  # token -> {"date": date, "open": price, "high": price, "low": price}
BANKNIFTY_ATM_STRIKE = {}  # token -> strike value (only for underlying BANKNIFTY)

# =========================
# STRATEGY-SPECIFIC TOKEN SETS
# =========================
# These come from config.SUBSCRIPTION_GUIDE: {strategy: {token: instrument_name}}
STOCK15_TOKENS = set(SUBSCRIPTION_GUIDE.get("stock-15min", {}).keys())
EMA_TOKENS = set(SUBSCRIPTION_GUIDE.get("ema_crossover", {}).keys())
NIFTY30_TOKENS = set(SUBSCRIPTION_GUIDE.get("nifty_30min_breakout", {}).keys())
BANKNIFTY_CROSSOVER_TOKENS = set(SUBSCRIPTION_GUIDE.get("bank_nifty_crossover", {}).keys())
STOCK_FUT_BREAKOUT_TOKENS = set(SUBSCRIPTION_GUIDE.get("stock_fut_breakout", {}).keys())

# stock_fut_breakout: tick-derived prior session close (committed when calendar day changes)
# token -> {"price": float, "timestamp": datetime, "session_date": date}
STOCK_FUT_LAST_SESSION_CLOSE = {}
# Rolling last tick for the active session day (used to commit session close on next day)
# token -> {"price": float, "timestamp": datetime}
STOCK_FUT_INTRADAY_LAST_TICK = {}
# token -> date (calendar day of last STOCK_FUT_INTRADAY_LAST_TICK update)
STOCK_FUT_TICK_DAY = {}
# First tick of the current session day (for gap vs prior session — timestamps must differ by date)
# token -> {"price": float, "timestamp": datetime}
STOCK_FUT_FIRST_TICK_OF_DAY = {}
# token -> None | bool — None until gap is computable (prior session + opening tick on a new date)
STOCK_FUT_IS_GAPDOWN = {}
# token -> {"date", "high", "low"} first 75 minutes (gapdown names only)
STOCK_FUT_75M_RANGE = {}
# token -> bool
STOCK_FUT_BROKEN_OUT = {}

# First 75 minutes = fifteen 5m candles starting 9:15
_STOCK_FUT_75M_MIN_9 = frozenset({15, 20, 25, 30, 35, 40, 45, 50, 55})
_STOCK_FUT_75M_MIN_10 = frozenset({0, 5, 10, 15, 20, 25})


_STOCK_FUT_LAST_DISK_SAVE = 0.0


def _stock_fut_persist(force: bool = False):
    """Persist tick state; throttle disk writes unless force (e.g. session commit)."""
    global _STOCK_FUT_LAST_DISK_SAVE
    now = pytime.time()
    if not force and (now - _STOCK_FUT_LAST_DISK_SAVE) < 2.0:
        return
    _STOCK_FUT_LAST_DISK_SAVE = now
    stock_fut_cache.save_state(
        {
            "last_session_close": STOCK_FUT_LAST_SESSION_CLOSE,
            "intraday_last_tick": STOCK_FUT_INTRADAY_LAST_TICK,
            "tick_day": STOCK_FUT_TICK_DAY,
            "first_tick_of_day": STOCK_FUT_FIRST_TICK_OF_DAY,
        }
    )


def _stock_fut_load():
    global STOCK_FUT_LAST_SESSION_CLOSE, STOCK_FUT_INTRADAY_LAST_TICK, STOCK_FUT_TICK_DAY, STOCK_FUT_FIRST_TICK_OF_DAY
    raw = stock_fut_cache.load_state()
    if not raw:
        return
    STOCK_FUT_LAST_SESSION_CLOSE = raw.get("last_session_close", {})
    STOCK_FUT_INTRADAY_LAST_TICK = raw.get("intraday_last_tick", {})
    STOCK_FUT_TICK_DAY = raw.get("tick_day", {})
    STOCK_FUT_FIRST_TICK_OF_DAY = raw.get("first_tick_of_day", {})


_stock_fut_load()

# token -> first 30-minute candle range for NIFTY breakout (nifty_30min_breakout)
FIRST_30_CANDLE_OF_DAY = {}  # token -> {"date": date, "high": price, "low": price}
# token -> whether price has breached the first 30-minute range after 9:45
NIFTY_30_BREACHED = {}  # token -> bool


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
    if not session or not isinstance(session, dict):
        raise RuntimeError("SmartAPI login failed: empty response")
    data = session.get("data") or {}
    jwt_token = data.get("jwtToken")
    feed_token = data.get("feedToken")
    if not jwt_token or not feed_token:
        msg = session.get("message") or "jwtToken/feedToken missing"
        raise RuntimeError(f"SmartAPI login failed: {msg}")
    return jwt_token, feed_token


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
                # NSE_CM (1): stocks + indices; NSE_FO (2): BANKNIFTY options (F&O)
                token_list = [{"exchangeType": 1, "tokens": NSE_TOKENS}]
                if NFO_TOKENS:
                    token_list.append({"exchangeType": 2, "tokens": list(NFO_TOKENS)})
                ws.subscribe(correlation_id="ema", mode=1, token_list=token_list)

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

                    # ---- Track last price per token (needed for option LTP when forwarding from underlying) ----
                    LAST_PRICE[token] = price

                    # ---- Candle aggregation (may or may not close candle) ----
                    candles = aggregator.process_tick(token, price, ts)

                    # ---- Track first 15-minute candle breakout for stock-15min ----
                    if token in STOCK15_TOKENS:
                        current_date = ts.date()
                        stored_date = FIRST_15_CANDLE_OF_DAY.get(token, {}).get("date")

                        # Daily reset at first tick after day change.
                        needs_reset = False
                        if stored_date is None:
                            needs_reset = True
                        elif isinstance(stored_date, dt.date):
                            needs_reset = stored_date != current_date
                        else:
                            needs_reset = True

                        if needs_reset:
                            FIRST_15_CANDLE_OF_DAY[token] = {}
                            STOCK_15_BREACHED[token] = False

                        # Compute the first 15-min high/low after the first 15 minutes complete.
                        # First 15 minutes = 9:15-9:30, made from 5-min candles at 9:15, 9:20, 9:25.
                        first15 = FIRST_15_CANDLE_OF_DAY.get(token, {})
                        if (
                            ts.hour == 9
                            and ts.minute >= 30
                            and (first15.get("high") is None or first15.get("low") is None)
                            and candles is not None
                            and len(candles) > 0
                        ):
                            required_minutes = {15, 20, 25}
                            found = {}
                            for c in candles:
                                ct = c.get("time")
                                if not ct:
                                    continue
                                if ct.date() != current_date or ct.hour != 9:
                                    continue
                                if ct.minute in required_minutes:
                                    found[ct.minute] = c

                            if len(found) == 3:
                                highs = [v.get("high") for v in found.values() if v.get("high") is not None]
                                lows = [v.get("low") for v in found.values() if v.get("low") is not None]
                                if highs and lows:
                                    FIRST_15_CANDLE_OF_DAY[token] = {
                                        "date": current_date,
                                        "high": max(highs),
                                        "low": min(lows),
                                    }

                        # Breakout check after 9:30 once we know first 15m high/low.
                        breached = STOCK_15_BREACHED.get(token, False)
                        first15 = FIRST_15_CANDLE_OF_DAY.get(token, {})
                        first15_high = first15.get("high")
                        first15_low = first15.get("low")
                        if (
                            not breached
                            and first15_high is not None
                            and first15_low is not None
                            and (ts.hour > 9 or (ts.hour == 9 and ts.minute >= 30))
                        ):
                            if price > first15_high or price < first15_low:
                                STOCK_15_BREACHED[token] = True

                    # ---- stock_fut_breakout: prior session = last tick of last session day; gap vs first tick today ----
                    if token in STOCK_FUT_BREAKOUT_TOKENS:
                        current_date = ts.date()
                        old_day = STOCK_FUT_TICK_DAY.get(token)

                        # New calendar day: commit previous session's last tick as closing price
                        if old_day is not None and old_day != current_date:
                            last = STOCK_FUT_INTRADAY_LAST_TICK.get(token)
                            if (
                                last
                                and last["timestamp"].date() == old_day
                            ):
                                STOCK_FUT_LAST_SESSION_CLOSE[token] = {
                                    "price": last["price"],
                                    "timestamp": last["timestamp"],
                                    "session_date": old_day,
                                }
                                _stock_fut_persist(force=True)
                            STOCK_FUT_75M_RANGE.pop(token, None)
                            STOCK_FUT_BROKEN_OUT.pop(token, None)
                            STOCK_FUT_FIRST_TICK_OF_DAY.pop(token, None)
                            STOCK_FUT_IS_GAPDOWN.pop(token, None)

                        if old_day is None or old_day != current_date:
                            STOCK_FUT_FIRST_TICK_OF_DAY[token] = {
                                "price": price,
                                "timestamp": ts,
                            }

                        STOCK_FUT_TICK_DAY[token] = current_date
                        STOCK_FUT_INTRADAY_LAST_TICK[token] = {
                            "price": price,
                            "timestamp": ts,
                        }
                        _stock_fut_persist()

                        last_sess = STOCK_FUT_LAST_SESSION_CLOSE.get(token)
                        first = STOCK_FUT_FIRST_TICK_OF_DAY.get(token)
                        gapdown = None
                        if (
                            last_sess is not None
                            and first is not None
                            and last_sess["session_date"] < first["timestamp"].date()
                        ):
                            prev_px = last_sess["price"]
                            open_px = first["price"]
                            if prev_px > 0:
                                gap_pct = (open_px - prev_px) / prev_px * 100.0
                                gapdown = gap_pct <= -STOCK_FUT_GAPDOWN_PCT
                        STOCK_FUT_IS_GAPDOWN[token] = gapdown

                        if gapdown is True and candles is not None and (
                            ts.hour > 10 or (ts.hour == 10 and ts.minute >= 30)
                        ):
                            found75 = {}
                            for c in candles:
                                ct = c.get("time")
                                if not ct or ct.date() != current_date:
                                    continue
                                if ct.hour == 9 and ct.minute in _STOCK_FUT_75M_MIN_9:
                                    found75[(9, ct.minute)] = c
                                elif ct.hour == 10 and ct.minute in _STOCK_FUT_75M_MIN_10:
                                    found75[(10, ct.minute)] = c
                            if len(found75) == 15:
                                hs = [
                                    v.get("high")
                                    for v in found75.values()
                                    if v.get("high") is not None
                                ]
                                ls = [
                                    v.get("low")
                                    for v in found75.values()
                                    if v.get("low") is not None
                                ]
                                if hs and ls:
                                    STOCK_FUT_75M_RANGE[token] = {
                                        "date": current_date,
                                        "high": max(hs),
                                        "low": min(ls),
                                    }

                        rr = STOCK_FUT_75M_RANGE.get(token, {})
                        if gapdown is True and rr.get("date") == current_date:
                            h75 = rr.get("high")
                            l75 = rr.get("low")
                            if (
                                h75 is not None
                                and l75 is not None
                                and (
                                    ts.hour > 10
                                    or (ts.hour == 10 and ts.minute >= 30)
                                )
                            ):
                                if price > h75 or price < l75:
                                    STOCK_FUT_BROKEN_OUT[token] = True

                    # ---- Track first 30-minute candle breakout for nifty_30min_breakout ----
                    if token in NIFTY30_TOKENS:
                        current_date = ts.date()
                        stored_date = FIRST_30_CANDLE_OF_DAY.get(token, {}).get("date")

                        needs_reset = False
                        if stored_date is None:
                            needs_reset = True
                        elif isinstance(stored_date, dt.date):
                            needs_reset = stored_date != current_date
                        else:
                            needs_reset = True

                        if needs_reset:
                            FIRST_30_CANDLE_OF_DAY[token] = {}
                            NIFTY_30_BREACHED[token] = False

                        first30 = FIRST_30_CANDLE_OF_DAY.get(token, {})
                        if (
                            ts.hour == 9
                            and ts.minute >= 45
                            and (first30.get("high") is None or first30.get("low") is None)
                            and candles is not None
                            and len(candles) > 0
                        ):
                            # First 30-min = 9:15-9:45 => 6 five-minute candles: 15,20,25,30,35,40
                            required_minutes = {15, 20, 25, 30, 35, 40}
                            found = {}
                            for c in candles:
                                ct = c.get("time")
                                if not ct:
                                    continue
                                if ct.date() != current_date or ct.hour != 9:
                                    continue
                                if ct.minute in required_minutes:
                                    found[ct.minute] = c

                            if len(found) == 6:
                                highs = [v.get("high") for v in found.values() if v.get("high") is not None]
                                lows = [v.get("low") for v in found.values() if v.get("low") is not None]
                                if highs and lows:
                                    FIRST_30_CANDLE_OF_DAY[token] = {
                                        "date": current_date,
                                        "high": max(highs),
                                        "low": min(lows),
                                    }

                        breached30 = NIFTY_30_BREACHED.get(token, False)
                        first30_high = first30.get("high")
                        first30_low = first30.get("low")
                        if (
                            not breached30
                            and first30_high is not None
                            and first30_low is not None
                            and (ts.hour > 9 or (ts.hour == 9 and ts.minute >= 45))
                        ):
                            if price > first30_high or price < first30_low:
                                NIFTY_30_BREACHED[token] = True

                    # ---- Track first candle of day for all BANKNIFTY symbols (underlying and options) ----
                    if symbol.startswith("BANKNIFTY"):
                        current_date = ts.date()
                        stored_date = FIRST_CANDLE_OF_DAY.get(token, {}).get("date")
                        
                        # Reset if it's a new day (explicit date comparison)
                        needs_reset = False
                        if stored_date is None:
                            needs_reset = True
                        elif isinstance(stored_date, dt.date):
                            needs_reset = stored_date != current_date
                        else:
                            # Handle case where stored_date might be a string or other type
                            needs_reset = True
                        
                        if needs_reset:
                            # Clear old first candle data for new day
                            FIRST_CANDLE_OF_DAY[token] = {}
                            
                            # For underlying BANKNIFTY only, also clear ATM strike
                            if symbol == "BANKNIFTY":
                                BANKNIFTY_ATM_STRIKE[token] = None
                            
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
                            
                            # Store first candle data
                            if first_candle_data is not None:
                                first_candle_open = first_candle_data.get("open", price)
                                first_candle_high = first_candle_data.get("high", price)
                                first_candle_low = first_candle_data.get("low", price)
                                
                                FIRST_CANDLE_OF_DAY[token] = {
                                    "date": current_date,
                                    "open": first_candle_open,
                                    "high": first_candle_high,
                                    "low": first_candle_low
                                }
                                
                                # Calculate ATM strike only for underlying BANKNIFTY
                                if symbol == "BANKNIFTY":
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
                                        if token in FIRST_CANDLE_OF_DAY:
                                            FIRST_CANDLE_OF_DAY[token]["high"] = current_candle.get("high", price)
                                            FIRST_CANDLE_OF_DAY[token]["low"] = current_candle.get("low", price)
                            # Also update when first candle closes
                            elif candles is not None and len(candles) > 0:
                                # Check if the first candle of today just closed
                                for candle in candles:
                                    candle_date = candle["time"].date()
                                    if candle_date == current_date and candle["time"].hour == 9 and candle["time"].minute == 15:
                                        # First candle closed, update with final high/low
                                        if token in FIRST_CANDLE_OF_DAY:
                                            FIRST_CANDLE_OF_DAY[token]["high"] = candle.get("high")
                                            FIRST_CANDLE_OF_DAY[token]["low"] = candle.get("low")
                                        break

                    # ---- Indicator update ONLY on candle close ----
                    if candles is not None:
                        # EMA indicators for ema_crossover + bank_nifty_crossover
                        if token in EMA_TOKENS or token in BANKNIFTY_CROSSOVER_TOKENS:
                            indicators = calculate_indicators(candles)
                            if indicators:
                                LAST_INDICATORS[token] = indicators

                        # Trend (Heikin-Ashi + structure) for stock-15min strategy
                        if token in STOCK15_TOKENS and len(candles) >= 20:
                            try:
                                df = pd.DataFrame(list(candles))
                                trend = tell_trend(df)
                                LAST_TRENDS[token] = trend
                            except Exception as e:
                                print(f"Trend calculation error for {symbol}: {e}")

                    # ---- Fetch last known indicators and trend ----
                    last = LAST_INDICATORS.get(token)
                    trend = LAST_TRENDS.get(token)

                    # ---- STREAM EVERY TICK ----
                    # ema_crossover
                    if token in EMA_TOKENS:
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
                            "indicator_time": indicator_time,
                        }, strategy="ema_crossover")

                    # stock-15min
                    if token in STOCK15_TOKENS:
                        first15_data = FIRST_15_CANDLE_OF_DAY.get(token, {})
                        first15_high = first15_data.get("high")
                        first15_low = first15_data.get("low")
                        breached = STOCK_15_BREACHED.get(token, False)
                        # Before the first 15m candle is formed, always project breached as False.
                        if first15_high is None or first15_low is None:
                            breached = False

                        broadcast({
                            "symbol": symbol,
                            "token": token,
                            "timestamp": ts.isoformat(),
                            "price": price,
                            "trend": trend,
                            "high": first15_high,
                            "low": first15_low,
                            "breached": breached,
                        }, strategy="stock-15min")

                    # nifty_30min_breakout (NIFTY50)
                    if token in NIFTY30_TOKENS:
                        first30_data = FIRST_30_CANDLE_OF_DAY.get(token, {})
                        first30_high = first30_data.get("high")
                        first30_low = first30_data.get("low")
                        breached30 = NIFTY_30_BREACHED.get(token, False)
                        if first30_high is None or first30_low is None:
                            breached30 = False

                        broadcast({
                            "symbol": symbol,
                            "token": token,
                            "timestamp": ts.isoformat(),
                            "price": price,
                            "high": first30_high,
                            "low": first30_low,
                            "breached": breached30,
                        }, strategy="nifty_30min_breakout")

                    # stock_fut_breakout (NSE cash; execution layer trades futures)
                    if token in STOCK_FUT_BREAKOUT_TOKENS:
                        gd = STOCK_FUT_IS_GAPDOWN.get(token)
                        ls = STOCK_FUT_LAST_SESSION_CLOSE.get(token)
                        prev_close = ls["price"] if ls else None
                        ls_ts = (
                            ls["timestamp"].isoformat()
                            if ls and ls.get("timestamp")
                            else None
                        )
                        ft = STOCK_FUT_FIRST_TICK_OF_DAY.get(token)
                        open_ts = (
                            ft["timestamp"].isoformat()
                            if ft and ft.get("timestamp")
                            else None
                        )
                        sf_high = sf_low = None
                        if gd is True:
                            r75 = STOCK_FUT_75M_RANGE.get(token, {})
                            if r75.get("date") == ts.date():
                                sf_high = r75.get("high")
                                sf_low = r75.get("low")

                        broadcast(
                            {
                                "symbol": symbol,
                                "token": token,
                                "timestamp": ts.isoformat(),
                                "price": price,
                                "prev_close": prev_close,
                                "last_session_close_timestamp": ls_ts,
                                "opening_tick_timestamp": open_ts,
                                "gapdown": gd,
                                "high": sf_high,
                                "low": sf_low,
                                "broken_out": STOCK_FUT_BROKEN_OUT.get(token, False),
                            },
                            strategy="stock_fut_breakout",
                        )

                    # bank_nifty_crossover (BANKNIFTY + options)
                    if token in BANKNIFTY_CROSSOVER_TOKENS:
                        underlying_token = "99926009" if symbol != "BANKNIFTY" else token
                        atm_strike = BANKNIFTY_ATM_STRIKE.get(underlying_token)
                        first_candle = FIRST_CANDLE_OF_DAY.get(token, {})

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
                            "low": first_candle.get("low"),
                        }, strategy="bank_nifty_crossover")


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
            pytime.sleep(RECONNECT_DELAY)

# =========================
# MAIN ENTRY POINT
# =========================

def main():
    print("EMA Engine starting...")
    run_websocket()

if __name__ == "__main__":
    main()
