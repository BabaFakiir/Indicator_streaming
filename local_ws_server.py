import asyncio
import json
import threading
import websockets

# websocket -> set(symbols)
CLIENT_SUBSCRIPTIONS = {}
EVENT_LOOP = asyncio.new_event_loop()

# -------------------------
# WebSocket Handler
# -------------------------
async def handler(websocket):
    CLIENT_SUBSCRIPTIONS[websocket] = set()

    try:
        async for message in websocket:
            data = json.loads(message)

            # subscription message
            if data.get("action") == "subscribe":
                symbols = set(data.get("symbols", []))
                CLIENT_SUBSCRIPTIONS[websocket] = symbols
                await websocket.send(json.dumps({
                    "status": "subscribed",
                    "symbols": list(symbols)
                }))

    finally:
        CLIENT_SUBSCRIPTIONS.pop(websocket, None)

# -------------------------
# Server Runner
# -------------------------
async def start_server():
    async with websockets.serve(handler, "0.0.0.0", 9001):
        await asyncio.Future()

def run_server():
    asyncio.set_event_loop(EVENT_LOOP)
    EVENT_LOOP.run_until_complete(start_server())

threading.Thread(target=run_server, daemon=True).start()

# -------------------------
# Broadcast Function
# -------------------------
def broadcast(tick: dict):
    symbol = tick["symbol"]

    coros = []
    for ws, symbols in CLIENT_SUBSCRIPTIONS.items():
        if symbol in symbols:
            coros.append(ws.send(json.dumps(tick)))

    if coros:
        asyncio.run_coroutine_threadsafe(
            asyncio.gather(*coros),
            EVENT_LOOP
        )
