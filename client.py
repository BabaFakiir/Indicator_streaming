import asyncio
import websockets
import json

async def main():
    # For local development
    uri = "wss://indicatorstreaming-production.up.railway.app/ws"
    # For Koyeb deployment, use: uri = "wss://your-app-name.koyeb.app/ws"
    
    async with websockets.connect(uri) as ws:
        print("Connected to indicator stream")
        
        # Subscribe to symbols with strategies
        subscription = {
            "action": "subscribe",
            "subscriptions": [
                {"symbol": "BANKNIFTY", "strategy": "ema_crossover"}
            ]
        }
        
        # Send subscription
        await ws.send(json.dumps(subscription))
        print(f"Sent subscription: {subscription}")
        
        # Wait for subscription confirmation
        response = await ws.recv()
        data = json.loads(response)
        print(f"Subscription confirmed: {data}")
        
        # Listen for tick data
        print("\nListening for tick data...\n")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)
                
                # Handle different message types
                if data.get("status") == "subscribed":
                    print(f"Subscription update: {data}")
                else:
                    # It's a tick message
                    if "ema9" in data:
                        # ema_crossover strategy message
                        print(f"[EMA] {data['symbol']}: Price={data['price']}, "
                              f"EMA9={data['ema9']}, EMA21={data['ema21']}, EMA34={data['ema34']}, "
                              f"RSI={data['rsi14']}")
                    elif "trend" in data:
                        # stock-15min strategy message
                        print(f"[TREND] {data['symbol']}: Price={data['price']}, "
                              f"Trend={data['trend']}")
                    else:
                        print(f"Unknown message: {data}")
                        
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed")
                break
            except Exception as e:
                print(f"Error receiving message: {e}")
                break

if __name__ == "__main__":
    asyncio.run(main())
