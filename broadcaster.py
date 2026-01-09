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

    if not websockets_to_send:
        # Quick debug - uncomment if needed
        # print(f"No subscribers for {symbol} with strategy {strategy}. Total clients: {len(CLIENT_SUBSCRIPTIONS)}")
        return

    async def send_all():
        for ws in websockets_to_send:
            try:
                await ws.send_text(message)
            except Exception:
                pass

    if MAIN_LOOP and MAIN_LOOP.is_running():
        asyncio.run_coroutine_threadsafe(send_all(), MAIN_LOOP)
