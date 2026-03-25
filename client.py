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
                {"symbol": "NIFTY50", "strategy": "nifty_30min_breakout"}
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
                        print(
                            f"[TREND] {data['symbol']}: Price={data['price']}, "
                            f"Trend={data['trend']}, "
                            f"High={data.get('high')}, Low={data.get('low')}, "
                            f"Breached={data.get('breached')}"
                        )
                    elif data.get("breached") is not None and "high" in data and "low" in data:
                        # nifty-30min-breakout strategy message
                        print(
                            f"[NIFTY_BREAKOUT] {data['symbol']}: Price={data['price']}, "
                            f"High={data.get('high')}, Low={data.get('low')}, "
                            f"Breached={data.get('breached')}"
                        )
                    elif "strike" in data:
                        # bank_nifty_ema strategy message
                        print(f"[BANK_NIFTY_EMA] {data['symbol']}: Price={data['price']}, "
                              f"LTP={data.get('ltp')}, EMA21={data.get('ema21')}, EMA34={data.get('ema34')}, "
                              f"Strike={data.get('strike')}, High={data.get('high')}, Low={data.get('low')}")
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
