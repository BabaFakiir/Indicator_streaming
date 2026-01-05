import asyncio
import json
import threading
import websockets

# websocket -> dict mapping symbol to strategy
# Example: {"TCS": "ema_crossover", "ICICI": "stock-15min"}
CLIENT_SUBSCRIPTIONS = {}
EVENT_LOOP = asyncio.new_event_loop()

# -------------------------
# WebSocket Handler
# -------------------------
async def handler(websocket):
    CLIENT_SUBSCRIPTIONS[websocket] = {}

    try:
        async for message in websocket:
            data = json.loads(message)

            # subscription message
            if data.get("action") == "subscribe":
                subscriptions = {}
                
                # Support new format: {"action": "subscribe", "subscriptions": [{"symbol": "TCS", "strategy": "ema_crossover"}]}
                if "subscriptions" in data:
                    for sub in data["subscriptions"]:
                        symbol = sub.get("symbol")
                        strategy = sub.get("strategy", "ema_crossover")  # default to ema_crossover
                        if symbol:
                            subscriptions[symbol] = strategy
                
                # Support legacy format: {"action": "subscribe", "symbols": ["TCS", "ICICI"]} (defaults to ema_crossover)
                elif "symbols" in data:
                    for symbol in data["symbols"]:
                        subscriptions[symbol] = "ema_crossover"
                
                # Support single subscription: {"action": "subscribe", "symbol": "TCS", "strategy": "stock-15min"}
                elif "symbol" in data:
                    symbol = data["symbol"]
                    strategy = data.get("strategy", "ema_crossover")
                    subscriptions[symbol] = strategy
                
                CLIENT_SUBSCRIPTIONS[websocket] = subscriptions
                await websocket.send(json.dumps({
                    "status": "subscribed",
                    "subscriptions": subscriptions
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
# Helper async function for sending
# -------------------------
async def _send_message(websocket, message):
    try:
        await websocket.send(message)
    except Exception as e:
        # Silently ignore errors (websocket might be closed)
        pass

# -------------------------
# Broadcast Function
# -------------------------
def broadcast(tick: dict, strategy: str = None):
    symbol = tick["symbol"]
    message = json.dumps(tick)

    # Collect websockets to send to
    websockets_to_send = []
    for ws, subscriptions in CLIENT_SUBSCRIPTIONS.items():
        # Check if client is subscribed to this symbol with matching strategy
        if symbol in subscriptions:
            client_strategy = subscriptions[symbol]
            # If strategy is specified in broadcast, only send to matching strategy subscriptions
            # If strategy is None, send to all subscriptions for this symbol (backward compatibility)
            if strategy is None or client_strategy == strategy:
                websockets_to_send.append(ws)

    if websockets_to_send:
        # Create a single coroutine that sends to all websockets
        async def send_all():
            tasks = []
            for ws in websockets_to_send:
                tasks.append(_send_message(ws, message))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        
        try:
            # Schedule the coroutine on the event loop
            asyncio.run_coroutine_threadsafe(send_all(), EVENT_LOOP)
        except Exception as e:
            print(f"Broadcast error: {e}")
