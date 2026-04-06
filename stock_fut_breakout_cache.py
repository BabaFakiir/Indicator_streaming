"""
Persist stock_fut_breakout tick-derived state (last session close, rolling last tick).
Same idea as indicator_cache for EMA: survive process restarts.
"""
import os
import pickle

from config import STOCK_FUT_CACHE_FILE


def load_state():
    if not os.path.exists(STOCK_FUT_CACHE_FILE):
        return None
    with open(STOCK_FUT_CACHE_FILE, "rb") as f:
        return pickle.load(f)


def save_state(state: dict) -> None:
    tmp = STOCK_FUT_CACHE_FILE + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(state, f)
    os.replace(tmp, STOCK_FUT_CACHE_FILE)
