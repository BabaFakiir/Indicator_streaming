#!/usr/bin/env python3
"""
Fetch BANKNIFTY option tokens from Angel One instrument master.
Filters for a given expiry and strike range (CE + PE).

Usage:
  python fetch_banknifty_options.py
  python fetch_banknifty_options.py --expiry 30MAR2026
  python fetch_banknifty_options.py --expiry 30MAR2026 --min 59000 --max 63000
"""
import argparse
import json
import urllib.request
from typing import List, Tuple

URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"


def fetch_instruments() -> list:
    """Fetch and parse OpenAPIScripMaster.json."""
    with urllib.request.urlopen(URL, timeout=30) as resp:
        return json.loads(resp.read().decode())


def filter_banknifty_options(
    instruments: list,
    expiry: str,  # e.g. "30MAR2026"
    strike_min: int = 55000,
    strike_max: int = 60000,
    step: int = 100,
) -> List[Tuple[str, str]]:
    """
    Filter for BANKNIFTY options: OPTIDX, NFO, given expiry, strikes in range.
    Returns list of (token, symbol) tuples.
    """
    valid_strikes = set(range(strike_min, strike_max + 1, step))
    results = []

    for row in instruments:
        if row.get("name") != "BANKNIFTY":
            continue
        if row.get("instrumenttype") != "OPTIDX":
            continue
        if row.get("exch_seg") != "NFO":
            continue
        if row.get("expiry") != expiry:
            continue

        symbol = row.get("symbol", "")
        token = row.get("token", "")
        if not token:
            continue

        # Parse strike from symbol: BANKNIFTY30MAR2659500CE -> 59500
        try:
            # Format: BANKNIFTY{EXPIRY}{STRIKE}{CE|PE}
            tail = symbol.replace("BANKNIFTY", "").rstrip("CE").rstrip("PE")
            strike_str = tail[-5:]  # last 5 digits
            strike = int(strike_str)
        except (ValueError, IndexError):
            continue

        if strike in valid_strikes:
            results.append((token, symbol))

    return sorted(results, key=lambda x: (x[1][-2:], x[1]))  # PE first, then CE; by symbol


def main():
    parser = argparse.ArgumentParser(description="Fetch BANKNIFTY option tokens from Angel One")
    parser.add_argument("--expiry", default="30MAR2026", help="Expiry e.g. 30MAR2026")
    parser.add_argument("--min", type=int, default=55000, help="Min strike")
    parser.add_argument("--max", type=int, default=59500, help="Max strike")
    parser.add_argument("--step", type=int, default=100, help="Strike step")
    args = parser.parse_args()

    print("Fetching instrument master...")
    instruments = fetch_instruments()
    print(f"Total instruments: {len(instruments)}")

    tokens_symbols = filter_banknifty_options(
        instruments,
        expiry=args.expiry,
        strike_min=args.min,
        strike_max=args.max,
        step=args.step,
    )

    print(f"\nBANKNIFTY options for {args.expiry}, strikes {args.min}–{args.max} (CE + PE):\n")
    print(f"{'Token':<10} {'Symbol'}")
    print("-" * 50)
    for token, symbol in tokens_symbols:
        print(f"{token:<10} {symbol}")

    print(f"\n# Config.py format (SYMBOLS dict entries):\n")
    for token, symbol in tokens_symbols:
        print(f'    "{token}": "{symbol}",')

    print(f"\n# Count: {len(tokens_symbols)} options")


if __name__ == "__main__":
    main()
