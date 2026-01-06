"""
Startup script for FastAPI server with background tick processing
This runs both the FastAPI server and the tick processing in the background
"""
import asyncio
import threading
import uvicorn
import os
from app import app
import main as tick_processor

def run_tick_processor():
    """Run the tick processor in a separate thread"""
    print("Starting tick processor...")
    try:
        tick_processor.run_websocket()
    except Exception as e:
        print(f"Tick processor error: {e}")
        import traceback
        traceback.print_exc()

def start_server():
    """Start the FastAPI server"""
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting FastAPI server on port {port}...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )

if __name__ == "__main__":
    # Start tick processor in background thread
    tick_thread = threading.Thread(target=run_tick_processor, daemon=True)
    tick_thread.start()
    
    # Start FastAPI server (blocks)
    start_server()

