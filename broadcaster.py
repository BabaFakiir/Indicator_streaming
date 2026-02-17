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
    for ws, subscriptions in CLIENT_SUBSCRIPTIONS.items():
        if symbol in subscriptions:
            client_strategies = subscriptions[symbol]
            # If strategy is None, send to all subscribers of this symbol
            # Otherwise, send only if client subscribed to this specific strategy
            if strategy is None or strategy in client_strategies:
                websockets_to_send.append(ws)
    
    # Debug logging for bank_nifty_ema
    if strategy == "bank_nifty_ema":
        logger.info(f"Broadcasting {strategy} for {symbol}: {len(websockets_to_send)} clients, total clients: {len(CLIENT_SUBSCRIPTIONS)}")
        if CLIENT_SUBSCRIPTIONS:
            for ws, subs in CLIENT_SUBSCRIPTIONS.items():
                logger.info(f"  Client subscriptions: {subs}")

    if not websockets_to_send:
        # Debug: log when no subscribers found
        logger.debug(f"No subscribers for {symbol} with strategy {strategy}. Total clients: {len(CLIENT_SUBSCRIPTIONS)}")
        if CLIENT_SUBSCRIPTIONS:
            logger.debug(f"Current subscriptions: {[(list(ws.subscriptions.keys()) if hasattr(ws, 'subscriptions') else 'N/A') for ws in CLIENT_SUBSCRIPTIONS.keys()]}")
        return

    async def send_all():
        for ws in websockets_to_send:
            try:
                await ws.send_text(message)
            except Exception:
                pass

    if MAIN_LOOP and MAIN_LOOP.is_running():
        asyncio.run_coroutine_threadsafe(send_all(), MAIN_LOOP)
