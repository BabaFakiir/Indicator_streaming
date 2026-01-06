# Quick Start Guide

## Local Testing

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start the server**:
   ```bash
   python start_server.py
   ```

3. **Test the service**:
   - Health check: `curl http://localhost:8000/health`
   - WebSocket: Use the updated `client.py`

## Key Changes Made

1. **Created `app.py`**: FastAPI application with WebSocket support
   - Replaces `local_ws_server.py`
   - Endpoints: `/`, `/health`, `/ws` (WebSocket)

2. **Updated `main.py`**: 
   - Changed import from `local_ws_server` to `app`
   - Tick processing logic remains the same

3. **Created `start_server.py`**: 
   - Starts FastAPI server and tick processor in background thread
   - Handles both services in one process

4. **Updated `requirements.txt`**: 
   - Added FastAPI, uvicorn, websockets

5. **Updated `client.py`**: 
   - Changed WebSocket URL to `ws://localhost:8000/ws`

## Deployment to Koyeb

1. **Push code to Git repository** (GitHub/GitLab/Bitbucket)

2. **Deploy via Koyeb Dashboard**:
   - Connect repository
   - Build command: `pip install -r requirements.txt`
   - Run command: `python start_server.py`
   - Port: `8000`

3. **Update client** to use Koyeb URL:
   ```python
   uri = "wss://your-app-name.koyeb.app/ws"
   ```

## Testing WebSocket Connection

```python
import asyncio
import websockets
import json

async def test():
    uri = "ws://localhost:8000/ws"  # or wss:// for Koyeb
    async with websockets.connect(uri) as ws:
        # Subscribe
        await ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"symbol": "TCS", "strategy": "ema_crossover"}
            ]
        }))
        
        # Receive messages
        while True:
            msg = await ws.recv()
            print(json.loads(msg))

asyncio.run(test())
```

## Architecture

```
┌─────────────────┐
│  FastAPI App    │  ← Handles WebSocket connections
│  (Port 8000)    │
└────────┬────────┘
         │
         ├─── WebSocket Endpoint (/ws)
         │
         └─── Health Endpoints (/health)
         
┌─────────────────┐
│ Tick Processor  │  ← Background thread
│ (SmartAPI WS)   │     Processes market data
└────────┬────────┘
         │
         └───→ Broadcasts to FastAPI WebSocket clients
```

## Notes

- The service runs both FastAPI and tick processor in the same process
- Background thread handles tick processing (non-blocking)
- FastAPI handles all WebSocket connections
- All WebSocket clients are managed by FastAPI's event loop

