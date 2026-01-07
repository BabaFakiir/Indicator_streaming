#!/usr/bin/env python3
"""
Quick script to check if all imports work
Run this before deploying to catch import errors
"""
import sys

print("Checking imports...")

try:
    print("  - Testing FastAPI...")
    from fastapi import FastAPI
    print("    ✓ FastAPI OK")
except Exception as e:
    print(f"    ✗ FastAPI failed: {e}")
    sys.exit(1)

try:
    print("  - Testing broadcaster...")
    from broadcaster import CLIENT_SUBSCRIPTIONS, set_main_loop
    print("    ✓ broadcaster OK")
except Exception as e:
    print(f"    ✗ broadcaster failed: {e}")
    sys.exit(1)

try:
    print("  - Testing config...")
    from config import SYMBOLS
    print(f"    ✓ config OK ({len(SYMBOLS)} symbols)")
except Exception as e:
    print(f"    ✗ config failed: {e}")
    sys.exit(1)

try:
    print("  - Testing app...")
    from app import app
    print("    ✓ app OK")
except Exception as e:
    print(f"    ✗ app failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

try:
    print("  - Testing main (this may take a moment)...")
    # Just check if it imports, don't run it
    import main
    print("    ✓ main imports OK")
except Exception as e:
    print(f"    ✗ main failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✅ All imports successful!")
print("You can now deploy to Koyeb.")

