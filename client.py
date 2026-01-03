import asyncio
import websockets
import json

async def main():
    uri = "ws://localhost:9001"
    async with websockets.connect(uri) as ws:
        print("Connected to indicator stream")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            print(data)

asyncio.run(main())
