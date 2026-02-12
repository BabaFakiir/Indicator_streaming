import pandas as pd
import talib

def calculate_indicators(candle_queue):
    if len(candle_queue) < 1:
        return None

    df = pd.DataFrame(list(candle_queue))
    close = df["close"].values

    # Calculate EMAs - handle NaN values when not enough data
    ema9_values = talib.EMA(close, 9)
    ema21_values = talib.EMA(close, 21)
    ema34_values = talib.EMA(close, 34)
    
    # Get the last value, or None if NaN
    ema9 = ema9_values[-1] if not pd.isna(ema9_values[-1]) else None
    ema21 = ema21_values[-1] if not pd.isna(ema21_values[-1]) else None
    ema34 = ema34_values[-1] if not pd.isna(ema34_values[-1]) else None
    
    rsi14_values = talib.RSI(close, 14)
    rsi14 = rsi14_values[-1] if not pd.isna(rsi14_values[-1]) else None

    return {
        "time": df.iloc[-1]["time"],
        "ema9": round(float(ema9), 2) if ema9 is not None else None,
        "ema21": round(float(ema21), 2) if ema21 is not None else None,
        "ema34": round(float(ema34), 2) if ema34 is not None else None,
        "rsi14": round(float(rsi14), 2) if rsi14 is not None else None
    }
