import uvicorn
import os
import sys

if __name__ == "__main__":
    try:
        port = int(os.environ.get("PORT", 8000))
        # Use import string format (better for Koyeb and avoids import issues)
        uvicorn.run(
            "app:app",  # Import string format
            host="0.0.0.0",
            port=port,
            log_level="info"
        )
    except Exception as e:
        print(f"Fatal error starting server: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
