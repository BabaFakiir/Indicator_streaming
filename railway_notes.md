# Railway Deployment Notes

## WebSocket 403 Error Fix

If you're getting a 403 error when connecting to WebSocket on Railway:

### 1. Check Railway Settings
- Make sure your Railway service is set to use **HTTP** (not TCP)
- Railway automatically handles WebSocket upgrades for HTTP services

### 2. Use Correct URL Format
- Use `wss://` (secure WebSocket) for production
- Example: `wss://your-app-name.up.railway.app/ws`

### 3. Check Environment Variables
Railway might need:
- `PORT` - Railway sets this automatically
- Make sure your app uses `os.environ.get("PORT", 8000)`

### 4. Verify WebSocket Endpoint
- The endpoint should be `/ws`
- Full URL: `wss://your-app-name.up.railway.app/ws`

### 5. CORS Configuration
The app now includes CORS middleware that allows all origins. If you need to restrict:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-frontend-domain.com"],  # Specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 6. Testing Connection
Test with:
```python
import asyncio
import websockets
import json

async def test():
    uri = "wss://your-app-name.up.railway.app/ws"
    async with websockets.connect(uri) as ws:
        print("Connected!")
        await ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [{"symbol": "TCS", "strategy": "ema_crossover"}]
        }))
        msg = await ws.recv()
        print(f"Received: {msg}")

asyncio.run(test())
```

### 7. Check Railway Logs
- Go to Railway dashboard → Your service → Logs
- Look for "WebSocket connection attempt" messages
- Check for any error messages

### 8. Common Issues
- **403 Forbidden**: Usually CORS or Railway proxy issue (now fixed with CORS middleware)
- **Connection refused**: Service not running or wrong port
- **Timeout**: Service might be sleeping (Railway free tier)

### 9. Railway-Specific Configuration
Railway uses a reverse proxy, so:
- Your app should listen on `0.0.0.0` (already configured)
- Port is set by Railway via `PORT` env var
- WebSocket upgrades are handled automatically by Railway

