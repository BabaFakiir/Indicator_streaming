# Subscription Guide

## WebSocket Server
- **Host**: `0.0.0.0` (or `localhost`)
- **Port**: `9001`
- **Protocol**: `websockets` (ws:// or wss://)

---

## Important: Symbol vs Token

**Use symbol names (not tokens) for subscriptions.**

- In `config.py`, `SYMBOLS` dict maps: `token → symbol_name`
  - Example: `"11536" → "TCS"`
- For subscriptions, use the **symbol name** (the value, like `"TCS"`), not the token (the key, like `"11536"`)
- The server broadcasts with symbol names, and matches subscriptions by symbol name

---

## Subscription Formats

### Format 1: Multiple Subscriptions (Recommended)
Subscribe to multiple symbols with different strategies:

```json
{
  "action": "subscribe",
  "subscriptions": [
    {"symbol": "TCS", "strategy": "ema_crossover"},
    {"symbol": "ICICI", "strategy": "stock-15min"},
    {"symbol": "ATGL", "strategy": "ema_crossover"},
    {"symbol": "ADANIENT", "strategy": "stock-15min"}
  ]
}
```

### Format 2: Single Symbol Subscription
Subscribe to a single symbol with a specific strategy:

```json
{
  "action": "subscribe",
  "symbol": "TCS",
  "strategy": "ema_crossover"
}
```

### Format 3: Legacy Format (Defaults to ema_crossover)
Subscribe to multiple symbols, all using `ema_crossover` strategy:

```json
{
  "action": "subscribe",
  "symbols": ["TCS", "ICICI", "ATGL"]
}
```

---

## Subscription Response

After sending a subscription message, you'll receive a confirmation:

```json
{
  "status": "subscribed",
  "subscriptions": {
    "TCS": "ema_crossover",
    "ICICI": "stock-15min"
  }
}
```

---

## Response Formats

### Strategy: `ema_crossover`

**Response Format:**
```json
{
  "symbol": "TCS",
  "token": "11536",
  "timestamp": "2026-01-02T10:30:45.123456+05:30",
  "price": 3456.75,
  "ema9": 3450.25,
  "ema21": 3445.50,
  "ema34": 3440.75,
  "rsi14": 58.75,
  "indicator_time": "2026-01-02T10:30:00+05:30"
}
```

**Fields:**
- `symbol`: Stock symbol (e.g., "TCS", "ICICI")
- `token`: Token ID for the stock
- `timestamp`: Current tick timestamp (ISO format)
- `price`: Last traded price
- `ema9`: 9-period EMA value on 5-minute candles (or `null` if not calculated yet)
- `ema21`: 21-period EMA value on 5-minute candles (or `null` if not calculated yet)
- `ema34`: 34-period EMA value on 5-minute candles (or `null` if not calculated yet)
- `rsi14`: 14-period RSI value (or `null` if not calculated yet)
- `indicator_time`: Timestamp of the last 5-minute candle close (or `null`)

**Note:** Indicators are updated only when a 5-minute candle closes. Until then, `ema9`, `ema21`, `ema34`, `rsi14`, and `indicator_time` will be `null`.

---

### Strategy: `stock-15min`

**Response Format:**
```json
{
  "symbol": "ICICI",
  "token": "4963",
  "timestamp": "2026-01-02T10:30:45.123456+05:30",
  "price": 987.50,
  "trend": "BULLISH"
}
```

**Fields:**
- `symbol`: Stock symbol (e.g., "TCS", "ICICI")
- `token`: Token ID for the stock
- `timestamp`: Current tick timestamp (ISO format)
- `price`: Last traded price
- `trend`: Trend value - one of:
  - `"BULLISH"`: Uptrend detected
  - `"BEARISH"`: Downtrend detected
  - `"SIDEWAYS"`: Sideways/consolidation
  - `null`: Not enough data (need at least 20 five-minute candles)

**Note:** Trend is calculated when a 5-minute candle closes and at least 20 candles are available. Until then, `trend` will be `null`.

---

## Example: Python Client

```python
import asyncio
import websockets
import json

async def subscribe_example():
    uri = "ws://localhost:9001"
    
    async with websockets.connect(uri) as websocket:
        # Subscribe to both strategies
        subscription = {
            "action": "subscribe",
            "subscriptions": [
                {"symbol": "TCS", "strategy": "ema_crossover"},
                {"symbol": "ICICI", "strategy": "stock-15min"}
            ]
        }
        
        await websocket.send(json.dumps(subscription))
        
        # Receive subscription confirmation
        response = await websocket.recv()
        print("Subscription confirmed:", json.loads(response))
        
        # Listen for tick data
        async for message in websocket:
            data = json.loads(message)
            print("Received tick:", data)
            
            # Handle based on strategy
            if "ema9" in data:
                print(f"EMA Strategy - {data['symbol']}: Price={data['price']}, EMA9={data['ema9']}, EMA21={data['ema21']}, EMA34={data['ema34']}, RSI={data['rsi14']}")
            elif "trend" in data:
                print(f"Trend Strategy - {data['symbol']}: Price={data['price']}, Trend={data['trend']}")

# Run the example
asyncio.run(subscribe_example())
```

---

## Example: JavaScript/Node.js Client

```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:9001');

ws.on('open', () => {
  // Subscribe to both strategies
  const subscription = {
    action: "subscribe",
    subscriptions: [
      { symbol: "TCS", strategy: "ema_crossover" },
      { symbol: "ICICI", strategy: "stock-15min" }
    ]
  };
  
  ws.send(JSON.stringify(subscription));
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  
  // Check if it's a subscription confirmation
  if (message.status === "subscribed") {
    console.log("Subscription confirmed:", message);
    return;
  }
  
  // Handle tick data
  if (message.ema9 !== undefined) {
    console.log(`EMA Strategy - ${message.symbol}: Price=${message.price}, EMA9=${message.ema9}, EMA21=${message.ema21}, EMA34=${message.ema34}, RSI=${message.rsi14}`);
  } else if (message.trend !== undefined) {
    console.log(`Trend Strategy - ${message.symbol}: Price=${message.price}, Trend=${message.trend}`);
  }
});
```

---

## Available Symbols

Based on `config.py`, available symbols are (use the symbol name for subscriptions):

| Symbol Name | Token ID | Use in Subscription |
|------------|----------|---------------------|
| `"TCS"` | "11536" | ✅ Use `"TCS"` |
| `"ICICI"` | "4963" | ✅ Use `"ICICI"` |
| `"ATGL"` | "6066" | ✅ Use `"ATGL"` |
| `"ADANIENT"` | "25" | ✅ Use `"ADANIENT"` |
| `"TATAMOTORS"` | "3456" | ✅ Use `"TATAMOTORS"` |
| `"HINDCOPPER"` | "17939" | ✅ Use `"HINDCOPPER"` |
| `"RELIANCE"` | "2885" | ✅ Use `"RELIANCE"` |
| `"HDFCBANK"` | "1333" | ✅ Use `"HDFCBANK"` |
| `"WIPRO"` | "3787" | ✅ Use `"WIPRO"` |
| `"INFY"` | "1594" | ✅ Use `"INFY"` |
| `"PTCIL"` | "16682" | ✅ Use `"PTCIL"` |
| `"CAPLIPOINT"` | "3906" | ✅ Use `"CAPLIPOINT"` |
| `"TRENT"` | "1964" | ✅ Use `"TRENT"` |
| `"TATAINVEST"` | "1621" | ✅ Use `"TATAINVEST"` |
| `"WHIRLPOOL"` | "18011" | ✅ Use `"WHIRLPOOL"` |
| `"POLYMED"` | "25718" | ✅ Use `"POLYMED"` |
| `"ITI"` | "1675" | ✅ Use `"ITI"` |

**Note:** Always use the symbol name (left column) in your subscription messages, not the token ID (middle column).

---

## Important Notes

1. **Data Updates:**
   - Indicators (EMA, RSI) are updated only when a 5-minute candle closes
   - Trend is calculated when a 5-minute candle closes and at least 20 candles are available
   - Price updates on every tick

2. **Market Hours:**
   - Data is only streamed during market hours (9:15 AM - 3:30 PM IST)

3. **Multiple Strategies:**
   - You can subscribe to the same symbol with different strategies
   - Each strategy will receive its own formatted response

4. **Null Values:**
   - Indicators and trends may be `null` initially until enough data is collected
   - This is expected behavior and not an error

