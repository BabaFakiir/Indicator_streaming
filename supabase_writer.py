from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY, SUPABASE_TABLE

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_indicators(symbol, ema9, ema21, rsi14):
    supabase.table(SUPABASE_TABLE).upsert(
        {
            "Security": symbol,
            "EMA_9": ema9,
            "EMA_21": ema21,
            "RSI_14": rsi14
        },
        on_conflict="Security"
    ).execute()
