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
    try:
        await websocket.accept()
    except Exception as e:
        logger.error(f"Failed to accept WebSocket connection: {e}", exc_info=True)
        return
    
    CLIENT_SUBSCRIPTIONS[websocket] = {}

    try:
        while True:
            data = json.loads(await websocket.receive_text())

            if data.get("action") == "subscribe":
                subs = {}

                # Import only when needed to avoid circular imports at startup.
                from config import SUBSCRIPTION_GUIDE

                available_modes = set(SUBSCRIPTION_GUIDE.keys())

                # Support aliases from the client side.
                # Canonicalization ensures broadcaster/main can compare strategy strings consistently.
                strategy_aliases = {
                    "nifty-30min-breakout": "nifty_30min_breakout",
                    "bank_nifty_ema": "bank_nifty_crossover",  # backward-compat alias
                }

                for sub in data.get("subscriptions", []):
                    symbol = sub.get("symbol")
                    strategy = sub.get("strategy")

                    if not symbol:
                        continue

                    # Deny if strategy is missing or unknown.
                    if not strategy:
                        await websocket.send_json({
                            "status": "error",
                            "message": f"Missing required field 'strategy' for symbol {symbol}"
                        })
                        continue

                    strategy = strategy_aliases.get(strategy, strategy)

                    if strategy not in available_modes:
                        await websocket.send_json({
                            "status": "error",
                            "message": f"Unknown strategy '{strategy}'. Available: {sorted(available_modes)}"
                        })
                        continue

                    # Token/symbol allow-list per mode (derived from SUBSCRIPTION_GUIDE)
                    allowed_symbols = set(SUBSCRIPTION_GUIDE[strategy].values())
                    if symbol not in allowed_symbols:
                        await websocket.send_json({
                            "status": "error",
                            "message": f"Strategy '{strategy}' is not available for symbol '{symbol}'"
                        })
                        continue

                    # Store strategies as a set to support multiple strategies per symbol
                    if symbol not in subs:
                        subs[symbol] = set()
                    subs[symbol].add(strategy)

                # Convert sets to lists for JSON serialization in response
                subs_for_response = {k: list(v) for k, v in subs.items()}
                CLIENT_SUBSCRIPTIONS[websocket] = subs
                await websocket.send_json({"status": "subscribed", "subscriptions": subs_for_response})
            elif data.get("action") == "ping":
                await websocket.send_json({"status": "pong"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        CLIENT_SUBSCRIPTIONS.pop(websocket, None)

