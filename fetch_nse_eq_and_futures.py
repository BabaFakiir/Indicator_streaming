#!/usr/bin/env python3
"""
Fetch NSE equity (EQ) tokens and matching stock futures (FUTSTK) from Angel One
OpenAPIScripMaster.json — same source as fetch_banknifty_options.py.

NSE cash: rows with exch_seg NSE and symbol ending in -EQ (e.g. RELIANCE-EQ).
Futures: instrumenttype FUTSTK, exch_seg NFO, expiry matching the requested series
(e.g. RELIANCE28APR26FUT for expiry 28APR2026).

Usage:
  python fetch_nse_eq_and_futures.py --stocks RELIANCE TCS HDFCBANK
  python fetch_nse_eq_and_futures.py --expiry 28APR2026 --file stocks.txt
  python fetch_nse_eq_and_futures.py --json-out eq_fut_tokens.json
"""
from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.request
from typing import Any, Dict, List, Optional

URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# Sensible default basket if neither --stocks nor --file is given
DEFAULT_STOCKS = [
"RELIANCE", "HDFCBANK", "BHARTIARTL", "SBIN", "TCS", "ICICIBANK", "INFY", "BAJFINANCE", "LT",
"HINDUNILVR", "LICI", "SUNPHARMA", "MARUTI", "HCLTECH", "M&M", "AXISBANK", "ITC", "TITAN", "ONGC",
"KOTAKBANK", "NTPC", "ADANIPORTS", "ULTRACEMCO", "ADANIPOWER", "BEL", "DMART", "JSWSTEEL", "COALINDIA",
"POWERGRID", "VEDL", "BAJAJFINSV", "HAL", "BAJAJ-AUTO", "TATASTEEL", "NESTLEIND", "ETERNAL", "HINDZINC",
"ADANIENT","ASIANPAINT", "HINDALCO", "WIPRO", "IOC", "EICHERMOT", "SBILIFE", "GRASIM", "SHRIRAMFIN", "INDIGO",   
"TVSMOTOR", "DIVISLAB", "JIOFIN", "TECHM", "ADANIGREEN", "HYUNDAI", "VBL", "TORNTPHARM", "PFC", "UNIONBANK",
"BRITANNIA", "ABB", "PIDILITIND", "DLF", "BANKBARODA", "CUMMINSIND", "MUTHOOTFIN", "LTIM", "TRENT", "TATAPOWER",
"HDFCLIFE", "BPCL", "PNB", "IRFC", "SOLARINDS", "INDIANB", "BSE", "JINDALSTEL", "CHOLAFIN", "CANBK", "ADANIENSOL",
"HITACHI", "MOTHERSON", "INDUSTOWER", "TATAMOTORS", "SIEMENS", "CGPOWER", "APOLLOHOSP", "LUPIN", "POLYCAB",
"AMBUJACEM",
"TATACONSUM",
"GODREJCP",
"DRREDDY",
"HDFCAMC",
"HEROMOTOCO",
"BAJAJHLDNG",
"MARICO",
"CIPLA",
"BOSCHLTD",
"GMRINFRA",
"GAIL",
"IDEA",
"MAXHEALTH",
"MAZDOCK",
"UNITDSPR",
"WAAREEENER",
"ASHOKLEY",
"ZYDUSLIFE",
"BHEL",
"JSWENERGY",
"RECLTD",
"ICICIGI",
"SHREECEM",
"INDHOTEL",
"MANKIND",
"PERSISTENT",
"BHARATFORG",
"ABCAPITAL",
"OIL",
"AUROPHARMA",        
"SWIGGY",
"NHPC",
"HAVELLS",
"DABUR",
"NALCO",
"ICICIPRULI",
"SRF",
"NYKAA",
"LODHA",
"HINDPETRO",
"NMDC",
"TORNTPOWER",
"POLICYBZR",
"FEDERALBNK",    
"AUBANK",
"NAUKRI",
"PAYTM",
"SAIL",
"BANKINDIA",
"ALKEM",
"MCX",
"OFSS",
"SBICARD",
"INDUSINDBK",
"DIXON",
"LTF",
"FORTIS",
"UNOMINDA",
"GLENMARK",
"BIOCON",
"LAURUSLABS",
"YESBANK",
"SUZLON",
"OBEROIRLTY",
"PHOENIXLTD",
"RVNL",
"NAM-INDIA",
"APLAPOLLO",
"IDFCFIRSTB",
"PATANJALI",
"MFSL",
"VMART",
"UPL",
"COLPAL",
"TIINDIA",
"PRESTIGE",
"SUPREMEIND",
"GODREJPROP",
"BDL",
"PIIND",
"MPHASIS",
"ASTRAL",
"PREMIERENE",
"MOTILALOFS",
"VOLTAS",
"COFORGE",
"KALYANKJIL",
"KEI",
"PETRONET",
"360ONE",
"PAGEIND",
"COCHINSHIP",
"DALBHARAT",
"HUDCO",
"CONCOR",
"IREDA",
"DELHIVERY",
"BLUESTARCO",
"SONACOMS",
"GODFRYPHLP",
"JUBLFOOD",
"LICHSGFIN",
"FORCEMOT",
"TATAELXSI",
"EXIDEIND",
"CDSL",
"KAYNES",
"BANDHANBNK",
"NBCC",
"AMBER",
"TATATECH",
"ANGELONE",
"MANAPPURAM",
"NUVAMA",
"PNBHOUSING",
"KPITTECH",
"PPLPHARMA",
"RBLBANK",
"CAMS",
"KFINTECH",
"CROMPTON",
"INOXWIND",
"PGEL",
"SAMMAANCAP",
"IEX"
]


def fetch_instruments() -> List[Dict[str, Any]]:
    """Fetch and parse OpenAPIScripMaster.json."""
    try:
        import certifi

        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    req = urllib.request.Request(URL)
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        return json.loads(resp.read().decode())


def load_names_from_file(path: str) -> List[str]:
    names: List[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            names.append(s.upper())
    return names


def find_nse_eq_row(instruments: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    """Angel: NSE equity row uses symbol like RELIANCE-EQ on segment NSE."""
    name = name.upper()
    for row in instruments:
        if row.get("name") != name:
            continue
        if row.get("exch_seg") != "NSE":
            continue
        sym = row.get("symbol") or ""
        if sym.endswith("-EQ"):
            return row
    return None


def find_stock_future_row(
    instruments: List[Dict[str, Any]],
    name: str,
    expiry: str,
) -> Optional[Dict[str, Any]]:
    """
    Match stock future for expiry, e.g. expiry '28APR2026' -> RELIANCE28APR26FUT.
    """
    name = name.upper()
    expiry = expiry.upper()
    for row in instruments:
        if row.get("name") != name:
            continue
        if row.get("instrumenttype") != "FUTSTK":
            continue
        if row.get("exch_seg") != "NFO":
            continue
        if (row.get("expiry") or "").upper() != expiry:
            continue
        return row
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NSE EQ + stock future tokens from Angel instrument master"
    )
    parser.add_argument(
        "--expiry",
        default="28APR2026",
        help="Future expiry as in master JSON, e.g. 28APR2026",
    )
    parser.add_argument(
        "--stocks",
        nargs="*",
        metavar="NAME",
        help="Underlying names (e.g. RELIANCE TCS). If omitted, use --file or built-in defaults.",
    )
    parser.add_argument(
        "--file",
        "-f",
        help="Text file: one stock name per line (comments with # allowed)",
    )
    parser.add_argument(
        "--json-out",
        help="Write full matched rows (EQ + future) to this JSON file",
    )
    args = parser.parse_args()

    if args.file:
        names = load_names_from_file(args.file)
    elif args.stocks:
        names = [s.upper() for s in args.stocks]
    else:
        names = list(DEFAULT_STOCKS)

    if not names:
        print("No stock names to resolve.", file=sys.stderr)
        sys.exit(1)

    print("Fetching instrument master...")
    instruments = fetch_instruments()
    print(f"Total instruments: {len(instruments)}\n")
    print(f"Expiry filter (futures): {args.expiry}\n")

    rows_out: List[Dict[str, Any]] = []
    missing_eq: List[str] = []
    missing_fut: List[str] = []
    resolved: List[Dict[str, Any]] = []

    print(f"{'Name':<14} {'EQ token':<10} {'EQ symbol':<18} {'FUT token':<10} {'Future symbol'}")
    print("-" * 90)

    for name in names:
        eq = find_nse_eq_row(instruments, name)
        fut = find_stock_future_row(instruments, name, args.expiry)

        if not eq:
            missing_eq.append(name)
            eq_tok = eq_sym = "-"
        else:
            eq_tok = eq.get("token", "")
            eq_sym = eq.get("symbol", "")

        if not fut:
            missing_fut.append(name)
            fut_tok = fut_sym = "-"
        else:
            fut_tok = fut.get("token", "")
            fut_sym = fut.get("symbol", "")

        print(f"{name:<14} {eq_tok:<10} {eq_sym:<18} {fut_tok:<10} {fut_sym}")
        resolved.append({"name": name, "eq": eq, "fut": fut})

        if eq and fut:
            rows_out.append({"underlying": name, "nse_eq": eq, "future": fut})
        elif eq:
            rows_out.append({"underlying": name, "nse_eq": eq, "future": None})
        elif fut:
            rows_out.append({"underlying": name, "nse_eq": None, "future": fut})

    if missing_eq:
        print(f"\n# Missing NSE EQ row for: {', '.join(missing_eq)}", file=sys.stderr)
    if missing_fut:
        print(
            f"# No FUTSTK for expiry {args.expiry} for: {', '.join(missing_fut)}",
            file=sys.stderr,
        )

    print("\n# SYMBOLS-style (token -> label) for cash + future:")
    for r in resolved:
        eq, fut = r["eq"], r["fut"]
        if eq:
            label = (eq.get("symbol") or "").replace("-EQ", "")
            print(f'    "{eq.get("token")}": "{label}",')
        if fut:
            print(f'    "{fut.get("token")}": "{fut.get("symbol")}",')

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as jf:
            json.dump(
                {
                    "expiry": args.expiry,
                    "source_url": URL,
                    "instruments": rows_out,
                },
                jf,
                indent=2,
            )
        print(f"\nWrote {args.json_out}")


if __name__ == "__main__":
    main()
