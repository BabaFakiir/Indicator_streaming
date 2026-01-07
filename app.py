"""
FastAPI application for Indicator Streaming Service
Hosts WebSocket server for real-time indicator and trend data
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
import logging
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import here to avoid circular imports and catch errors
try:
    from broadcaster import CLIENT_SUBSCRIPTIONS, set_main_loop
except ImportError as e:
    logger.error(f"Failed to import broadcaster: {e}")
    raise

app = FastAPI(title="Indicator Streaming API", version="1.0.0")

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins - adjust for production if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def run_tick_processor():
    """Run the tick processor - wrapped to catch errors"""
    try:
        from main import run_websocket
        logger.info("Tick processor: Starting WebSocket connection to SmartAPI...")
        run_websocket()
    except Exception as e:
        logger.error(f"Tick processor error: {e}", exc_info=True)
        # Don't exit, let FastAPI keep running

@app.on_event("startup")
async def startup_event():
    try:
        loop = asyncio.get_running_loop()
        set_main_loop(loop)
        logger.info("FastAPI startup complete, event loop stored")
        
        # Start tick processor in background thread (non-blocking)
        # run_websocket() is a blocking function, so we run it in a daemon thread
        logger.info("Starting tick processor in background thread...")
        tick_thread = threading.Thread(target=run_tick_processor, daemon=True)
        tick_thread.start()
        logger.info("Tick processor thread started")
    except Exception as e:
        logger.error(f"Startup error: {e}", exc_info=True)
        # Don't raise - let FastAPI start even if tick processor fails


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
    # Log connection attempt details
    logger.info(f"WebSocket connection attempt from {websocket.client}")
    logger.info(f"Headers: {dict(websocket.headers)}")
    
    # Accept the WebSocket connection
    # This should handle CORS and origin checking
    try:
        await websocket.accept()
        logger.info(f"WebSocket connection accepted: {websocket.client}")
    except Exception as e:
        logger.error(f"Failed to accept WebSocket connection: {e}", exc_info=True)
        return
    
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

