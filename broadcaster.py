# broadcaster.py
import json
import asyncio
import logging
from typing import Dict
from fastapi import WebSocket

logger = logging.getLogger(__name__)

CLIENT_SUBSCRIPTIONS: Dict[WebSocket, Dict[str, set]] = {}
MAIN_LOOP = None


def set_main_loop(loop):
    global MAIN_LOOP
    MAIN_LOOP = loop


def broadcast(tick: dict, strategy: str = None):
    symbol = tick["symbol"]
    message = json.dumps(tick)

    websockets_to_send = []
    option_subscribers = {}  # ws -> list of option symbols they're subscribed to
    
    for ws, subscriptions in CLIENT_SUBSCRIPTIONS.items():
        if symbol in subscriptions:
            client_strategies = subscriptions[symbol]
            # If strategy is None, send to all subscribers of this symbol
            # Otherwise, send only if client subscribed to this specific strategy
            if strategy is None or strategy in client_strategies or "chart" in client_strategies:
                websockets_to_send.append(ws)
        
        # Special handling for bank_nifty_ema: if broadcasting underlying BANKNIFTY,
        # also send to clients subscribed to any BANKNIFTY option
        if strategy == "bank_nifty_ema" and symbol == "BANKNIFTY":
            for sub_symbol, client_strategies in subscriptions.items():
                if sub_symbol.startswith("BANKNIFTY") and sub_symbol != "BANKNIFTY":
                    if strategy in client_strategies:
                        if ws not in websockets_to_send:
                            if ws not in option_subscribers:
                                option_subscribers[ws] = []
                            option_subscribers[ws].append(sub_symbol)

    # Send to direct subscribers
    if websockets_to_send:
        async def send_all():
            for ws in websockets_to_send:
                try:
                    await ws.send_text(message)
                except Exception:
                    pass

        if MAIN_LOOP and MAIN_LOOP.is_running():
            asyncio.run_coroutine_threadsafe(send_all(), MAIN_LOOP)
    
    # Send to option subscribers with modified symbol/token
    if option_subscribers:
        from config import SYMBOLS
        # Import FIRST_CANDLE_OF_DAY from main to get option's own first candle data
        import main
        
        async def send_to_options():
            for ws, option_symbols in option_subscribers.items():
                for option_symbol in option_symbols:
                    try:
                        # Create a modified message with the option's symbol and token
                        # Keep all the underlying BANKNIFTY data (price, EMA, strike, etc.)
                        # but change the symbol and token to match the option
                        option_tick = tick.copy()
                        option_tick["symbol"] = option_symbol
                        # Get the option token from SYMBOLS (reverse lookup)
                        option_token = None
                        for tok, sym in SYMBOLS.items():
                            if sym == option_symbol:
                                option_token = tok
                                break
                        if option_token:
                            option_ltp = main.LAST_PRICE.get(option_token)
                            if option_ltp is None:
                                continue  # Don't send underlying's price as option LTP
                            option_tick["token"] = option_token
                            option_tick["price"] = option_ltp
                            option_tick["ltp"] = option_ltp
                            
                            # Use option's own EMAs (not underlying's index-scaled EMAs)
                            option_last = main.LAST_INDICATORS.get(option_token)
                            option_tick["ema21"] = option_last.get("ema21") if option_last else None
                            option_tick["ema34"] = option_last.get("ema34") if option_last else None
                            
                            # Update high/low to use the option's own first candle data
                            option_first_candle = main.FIRST_CANDLE_OF_DAY.get(option_token, {})
                            option_tick["high"] = option_first_candle.get("high")
                            option_tick["low"] = option_first_candle.get("low")
                        
                        await ws.send_text(json.dumps(option_tick))
                    except Exception:
                        pass
        
        if MAIN_LOOP and MAIN_LOOP.is_running():
            asyncio.run_coroutine_threadsafe(send_to_options(), MAIN_LOOP)
    
    if not websockets_to_send and not option_subscribers:
        return
