# Deployment Guide for Koyeb

This guide explains how to deploy the Indicator Streaming service on Koyeb.

## Prerequisites

1. A Koyeb account (sign up at https://www.koyeb.com)
2. Your code pushed to a Git repository (GitHub, GitLab, or Bitbucket)

## Deployment Steps

### Option 1: Deploy via Koyeb Dashboard

1. **Log in to Koyeb** and go to your dashboard
2. **Click "Create App"**
3. **Connect your Git repository** (GitHub/GitLab/Bitbucket)
4. **Configure the app:**
   - **Build Command**: `pip install -r requirements.txt`
   - **Run Command**: `python start_server.py`
   - **Port**: `8000` (default FastAPI port)
5. **Add Environment Variables** (if needed):
   - Any sensitive config from `config.py` should be set as environment variables
6. **Deploy**

### Option 2: Deploy via Koyeb CLI

```bash
# Install Koyeb CLI
curl -fsSL https://cli.koyeb.com/install.sh | sh

# Login
koyeb login

# Deploy
koyeb apps create indicator-streaming
koyeb services create \
  --app indicator-streaming \
  --git github.com/yourusername/Indicator_streaming \
  --git-branch main \
  --ports 8000:http \
  --routes /:8000
```

## Configuration

### Environment Variables

You can set these in Koyeb dashboard under "Environment Variables":

- `API_KEY` - Your SmartAPI API key
- `CLIENT_ID` - Your SmartAPI client ID
- `MPIN` - Your SmartAPI MPIN
- `TOTP_SECRET` - Your TOTP secret
- `SUPABASE_URL` - Supabase URL (optional)
- `SUPABASE_KEY` - Supabase key (optional)

### Port Configuration

- **Default Port**: `8000`
- Koyeb will automatically detect and use this port
- The service exposes:
  - `GET /` - Health check
  - `GET /health` - Health check endpoint
  - `WebSocket /ws` - WebSocket endpoint for streaming

## Testing the Deployment

Once deployed, test your service:

1. **Health Check**:
   ```bash
   curl https://your-app-name.koyeb.app/health
   ```

2. **WebSocket Connection**:
   ```python
   import asyncio
   import websockets
   import json
   
   async def test():
       uri = "wss://your-app-name.koyeb.app/ws"
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

## Client Connection

Update your client to use the Koyeb URL:

```python
# Change from:
uri = "ws://localhost:9001"

# To:
uri = "wss://your-app-name.koyeb.app/ws"
```

Note: Use `wss://` (secure WebSocket) for production.

## Monitoring

- Check logs in Koyeb dashboard under "Logs"
- Monitor health endpoint: `https://your-app-name.koyeb.app/health`
- Check active connections: `https://your-app-name.koyeb.app/` (returns connection count)

## Troubleshooting

1. **Service not starting**: Check logs in Koyeb dashboard
2. **WebSocket connection fails**: Ensure you're using `wss://` (not `ws://`)
3. **No data received**: Verify subscription message format
4. **Port issues**: Ensure port 8000 is exposed (default in Koyeb)

## File Structure

```
Indicator_streaming/
├── app.py              # FastAPI application
├── main.py             # Tick processing logic
├── start_server.py     # Startup script
├── requirements.txt    # Python dependencies
├── config.py           # Configuration
├── koyeb.yaml         # Koyeb config (optional)
└── Procfile           # Process file (optional)
```

## Notes

- The service runs both FastAPI server and tick processor in the same process
- Background thread handles tick processing
- FastAPI handles WebSocket connections
- All WebSocket connections are managed by FastAPI's event loop

