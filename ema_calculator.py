import pandas as pd
import talib

def calculate_indicators(candle_queue):
    if len(candle_queue) < 1:
        return None

    df = pd.DataFrame(list(candle_queue))
    close = df["close"].values

    return {
        "time": df.iloc[-1]["time"],
        "ema9": round(float(talib.EMA(close, 9)[-1]), 2),
        "ema21": round(float(talib.EMA(close, 21)[-1]), 2),
        "ema34": round(float(talib.EMA(close, 34)[-1]), 2),
        "rsi14": round(float(talib.RSI(close, 14)[-1]), 2)
    }
