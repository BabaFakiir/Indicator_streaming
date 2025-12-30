import pickle
import os
from config import CACHE_FILE

def save_state(state):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(state, f)

def load_state():
    if not os.path.exists(CACHE_FILE):
        return None
    with open(CACHE_FILE, "rb") as f:
        return pickle.load(f)
