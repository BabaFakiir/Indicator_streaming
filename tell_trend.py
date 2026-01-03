import pandas as pd

def tell_trend(df):
    """
    Determine short-term trend using Heikin-Ashi candles and swing structure.
    Input: df with columns ['open', 'high', 'low', 'close']
    """

    if len(df) < 20:
        print("Received less than 20 candles")
        return "UNKNOWN"

    df = df.tail(20).copy()

    # --- Compute Heikin-Ashi candles ---
    ha_df = pd.DataFrame(index=df.index)
    ha_df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

    ha_open = [(df['open'].iloc[0] + df['close'].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i-1] + ha_df['ha_close'].iloc[i-1]) / 2)
    ha_df['ha_open'] = ha_open

    ha_df['ha_high'] = df[['high', 'open', 'close']].max(axis=1)
    ha_df['ha_low']  = df[['low', 'open', 'close']].min(axis=1)

    # --- Step 1: Candle Bias Count (Heikin-Ashi color bias) ---
    bias_count = 0
    for i in range(len(ha_df)-1, -1, -1):
        if ha_df['ha_close'].iloc[i] > ha_df['ha_open'].iloc[i]:
            bias_count += 1
        else:
            bias_count -= 1

    # --- Step 2: Structural Trend Detection (Higher Highs / Higher Lows) ---
    swing_highs = []
    swing_lows = []
    for i in range(1, len(df)-1):
        if df['high'].iloc[i] > df['high'].iloc[i-1] and df['high'].iloc[i] > df['high'].iloc[i+1]:
            swing_highs.append(df['high'].iloc[i])
        if df['low'].iloc[i] < df['low'].iloc[i-1] and df['low'].iloc[i] < df['low'].iloc[i+1]:
            swing_lows.append(df['low'].iloc[i])

    structure_score = 0
    if len(swing_highs) >= 2:
        if swing_highs[-1] > swing_highs[-2]:
            structure_score += 1  # higher highs → bullish
        elif swing_highs[-1] < swing_highs[-2]:
            structure_score -= 1  # lower highs → bearish

    if len(swing_lows) >= 2:
        if swing_lows[-1] > swing_lows[-2]:
            structure_score -= 1  # higher lows but without higher highs could be weak uptrend
        elif swing_lows[-1] < swing_lows[-2]:
            structure_score += 1  # lower lows → bearish continuation

    # --- Step 3: Combine both weights ---
    weighted_score = bias_count * 0.7 + structure_score * 2.5  # structure is given higher importance

    # --- Step 4: Determine majority bias ---
    if weighted_score > 2:
        return "BULLISH"
    elif weighted_score < -2:
        return "BEARISH"
    else:
        return "SIDEWAYS"
