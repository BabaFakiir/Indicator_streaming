"""
FastAPI application for Indicator Streaming Service
Hosts WebSocket server for real-time indicator and trend data
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import asyncio
import logging
from main import run_websocket
from broadcaster import CLIENT_SUBSCRIPTIONS, set_main_loop

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Indicator Streaming API", version="1.0.0")

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    set_main_loop(loop)
    logger.info("FastAPI startup complete")
    
    # Start tick processor in background thread (non-blocking)
    # run_websocket() is a blocking function, so we run it in a daemon thread
    import threading
    logger.info("Starting tick processor in background thread...")
    tick_thread = threading.Thread(target=run_websocket, daemon=True)
    tick_thread.start()
    logger.info("Tick processor thread started")


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
    await websocket.accept()
    logger.info(f"New WebSocket connection: {websocket.client}")
    CLIENT_SUBSCRIPTIONS[websocket] = {}

    try:
        while True:
            data = json.loads(await websocket.receive_text())

            if data.get("action") == "subscribe":
                subs = {}
                for sub in data.get("subscriptions", []):
                    subs[sub["symbol"]] = sub.get("strategy", "ema_crossover")
                CLIENT_SUBSCRIPTIONS[websocket] = subs
                logger.info(f"Client subscribed: {subs}")
                await websocket.send_json({"status": "subscribed", "subscriptions": subs})

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {websocket.client}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        CLIENT_SUBSCRIPTIONS.pop(websocket, None)
        logger.info(f"Client unsubscribed: {websocket.client}")

