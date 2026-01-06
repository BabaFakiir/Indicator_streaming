"""
FastAPI application for Indicator Streaming Service
Hosts WebSocket server for real-time indicator and trend data
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import json
import asyncio
from typing import Dict, Set
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Indicator Streaming API", version="1.0.0")

# websocket -> dict mapping symbol to strategy
# Example: {"TCS": "ema_crossover", "ICICI": "stock-15min"}
CLIENT_SUBSCRIPTIONS: Dict[WebSocket, Dict[str, str]] = {}

# Global event loop reference (set on startup)
MAIN_LOOP = None

@app.on_event("startup")
async def startup_event():
    """Store reference to the main event loop"""
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_event_loop()
    logger.info("FastAPI startup complete, event loop stored")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "Indicator Streaming API",
        "active_connections": len(CLIENT_SUBSCRIPTIONS)
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "connections": len(CLIENT_SUBSCRIPTIONS)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time indicator streaming"""
    await websocket.accept()
    logger.info(f"New WebSocket connection: {websocket.client}")
    
    CLIENT_SUBSCRIPTIONS[websocket] = {}

    try:
        while True:
            # Receive message from client
            message = await websocket.receive_text()
            data = json.loads(message)
            logger.info(f"Received message: {data}")

            # Handle subscription message
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
                logger.info(f"Client subscribed: {subscriptions}")
                
                await websocket.send_json({
                    "status": "subscribed",
                    "subscriptions": subscriptions
                })

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {websocket.client}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        CLIENT_SUBSCRIPTIONS.pop(websocket, None)
        logger.info(f"Client unsubscribed: {websocket.client}")


def broadcast(tick: dict, strategy: str = None):
    """
    Broadcast tick data to subscribed clients
    
    Args:
        tick: Dictionary containing tick data (symbol, token, timestamp, price, etc.)
        strategy: Strategy filter ("ema_crossover" or "stock-15min"). If None, sends to all.
    """
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

    if not websockets_to_send:
        return

    # Create async function to send to all websockets
    async def send_all():
        for ws in websockets_to_send:
            try:
                await ws.send_text(message)
            except Exception as e:
                logger.debug(f"Error sending to client: {e}")
    
    # Schedule the coroutine on the event loop
    # This function is called from a background thread (tick processor)
    # so we need to schedule it on the main event loop
    try:
        # Use the stored main event loop
        if MAIN_LOOP and MAIN_LOOP.is_running():
            asyncio.run_coroutine_threadsafe(send_all(), MAIN_LOOP)
        else:
            # Fallback: try to get the running loop
            try:
                loop = asyncio.get_running_loop()
                asyncio.create_task(send_all())
            except RuntimeError:
                # No running loop, try to get any loop
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.run_coroutine_threadsafe(send_all(), loop)
                    else:
                        loop.run_until_complete(send_all())
                except RuntimeError:
                    logger.warning("No event loop available for broadcast")
    except Exception as e:
        logger.error(f"Broadcast error: {e}", exc_info=True)


# Export broadcast function for use in main.py
__all__ = ['app', 'broadcast', 'CLIENT_SUBSCRIPTIONS']

