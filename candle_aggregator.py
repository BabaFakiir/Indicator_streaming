import datetime as dt
from collections import deque
from config import IST, SYMBOLS
from cache import save_state, load_state

class CandleAggregator:
    def __init__(self):
        cached = load_state() or {}
        self.current = cached.get("current", {})
        self.queues = cached.get(
            "queues",
            {t: deque(maxlen=21) for t in SYMBOLS}
        )
        # Ensure all tokens in SYMBOLS have queues (in case new tokens were added)
        for token in SYMBOLS:
            if token not in self.queues:
                self.queues[token] = deque(maxlen=21)

    def _persist(self):
        save_state({
            "current": self.current,
            "queues": self.queues
        })

    def _floor_5min(self, ts):
        return ts.replace(
            minute=(ts.minute // 5) * 5,
            second=0,
            microsecond=0
        )

    def process_tick(self, token, ltp, timestamp):
        candle_time = self._floor_5min(timestamp)

        # Ensure queue exists for this token (in case it was added dynamically)
        if token not in self.queues:
            self.queues[token] = deque(maxlen=21)

        if token not in self.current:
            self.current[token] = self._new_candle(candle_time, ltp)
            self._persist()
            return None

        candle = self.current[token]

        if candle["time"] == candle_time:
            candle["high"] = max(candle["high"], ltp)
            candle["low"] = min(candle["low"], ltp)
            candle["close"] = ltp
            return None

        self.queues[token].append(candle.copy())
        self.current[token] = self._new_candle(candle_time, ltp)
        self._persist()
        return self.queues[token]

    @staticmethod
    def _new_candle(time, price):
        return {
            "time": time,
            "open": price,
            "high": price,
            "low": price,
            "close": price
        }
