import pytz
from datetime import time

API_KEY = "f7JZpXaY"
CLIENT_ID = "S59212605"
MPIN = "9999"
TOTP_SECRET = "UYAYICJJCIYDNW2QT2CREHPLBQ"

IST = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

RECONNECT_DELAY = 5  # seconds

SYMBOLS = {
    "11536": "TCS",
    "4963": "ICICI",
    "6066": "ATGL",
    "25": "ADANIENT",
    "3456": "TATAMOTORS",
    "17939": "HINDCOPPER"
}

SUPABASE_URL = "https://fbcequeftvgcysbrjuma.supabase.co"
SUPABASE_KEY = "<eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZiY2VxdWVmdHZnY3lzYnJqdW1hIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTMwOTQwNzcsImV4cCI6MjA2ODY3MDA3N30.oGrYI1pG5Pygtb-jnMq_vgULJtS72aeZ1r7YvIDoTLI>"
SUPABASE_TABLE = "Indicator values"

CACHE_FILE = "indicator_cache.pkl"
