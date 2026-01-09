# Guide: Adding a New Token to the Stream

## Step-by-Step Procedure

### 1. Find the Token ID
- Get the token ID for the instrument you want to stream
- Token IDs are unique identifiers for each instrument (stock, index, etc.)
- Example: `"99926000"` for NIFTY50, `"11536"` for TCS

### 2. Add Token to `config.py`

Open `config.py` and add the token to the `SYMBOLS` dictionary:

```python
SYMBOLS = {
    "11536": "TCS",
    "4963": "ICICI",
    # ... existing tokens ...
    "99926000": "NIFTY50",
    "YOUR_TOKEN_ID": "YOUR_SYMBOL_NAME"  # Add your new token here
}
```

**Format**: `"token_id": "SYMBOL_NAME"`

**Important Notes**:
- Token ID must be a **string** (in quotes)
- Symbol name is what clients will use for subscriptions
- Symbol name can contain spaces (e.g., `"NIFTY 50"` or `"NIFTY50"`)
- Use a clear, recognizable symbol name

### 3. Restart the Server

After adding the token, restart your server:

**Local:**
```bash
# Stop the current server (Ctrl+C)
python start_server.py
```

**Railway/Koyeb:**
- Push the changes to your Git repository
- Railway/Koyeb will automatically redeploy

### 4. Subscribe from Client

Once the server is running, subscribe to the new symbol using the **symbol name** (not the token ID):

```python
subscription = {
    "action": "subscribe",
    "subscriptions": [
        {"symbol": "YOUR_SYMBOL_NAME", "strategy": "ema_crossover"},
        {"symbol": "YOUR_SYMBOL_NAME", "strategy": "stock-15min"}
    ]
}
```

**Important**: Use the **symbol name** (the value from SYMBOLS dict), not the token ID!

## Example: Adding BANKNIFTY

### Step 1: Find Token ID
Let's say BANKNIFTY token ID is `"99926009"`

### Step 2: Add to config.py
```python
SYMBOLS = {
    # ... existing tokens ...
    "99926000": "NIFTY50",
    "99926009": "BANKNIFTY"  # New token added
}
```

### Step 3: Restart Server
```bash
python start_server.py
```

### Step 4: Subscribe from Client
```python
subscription = {
    "action": "subscribe",
    "subscriptions": [
        {"symbol": "BANKNIFTY", "strategy": "ema_crossover"}
    ]
}
```

## Automatic Queue Creation

The `CandleAggregator` automatically creates queues for new tokens when the server starts. You don't need to manually initialize anything - just add the token to `SYMBOLS` and restart.

## Verification

After adding a token and restarting:

1. **Check server logs**: You should see the tick processor connecting and subscribing to all tokens
2. **Check client subscription**: Subscribe to the new symbol and verify you receive data
3. **Verify tick data**: You should see price updates for the new token

## Common Issues

### Issue: Not receiving data after adding token
- **Solution**: Make sure you restarted the server after adding the token
- **Solution**: Verify the token ID is correct (check SmartAPI documentation)
- **Solution**: Ensure you're subscribing with the **symbol name**, not the token ID

### Issue: Token not found errors
- **Solution**: Verify the token ID format (must be a string in quotes)
- **Solution**: Check that the token exists in SmartAPI's system
- **Solution**: Ensure the token is subscribed during market hours

### Issue: Symbol name mismatch
- **Solution**: Use the exact symbol name as defined in `SYMBOLS` dict
- **Solution**: Check for case sensitivity (e.g., `"NIFTY50"` vs `"nifty50"`)
- **Solution**: Check for spaces (e.g., `"NIFTY 50"` vs `"NIFTY50"`)

## Notes

- **Token IDs are strings**: Always use quotes around token IDs
- **Symbol names are flexible**: You can use any name you want (e.g., `"NIFTY50"`, `"NIFTY 50"`, `"NIFTY-50"`)
- **No limit on tokens**: You can add as many tokens as needed
- **Restart required**: Always restart the server after adding new tokens
- **Market hours**: Data only streams during market hours (9:15 AM - 3:30 PM IST)
