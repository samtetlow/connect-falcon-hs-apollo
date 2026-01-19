#!/usr/bin/env python3
"""
FalconHub Dashboard Launcher
Start the Falcon-HubSpot integration dashboard
"""

import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    """Launch the dashboard"""
    print("🦅 FalconHub Dashboard Launcher")
    print("=" * 40)
    print("🚀 Starting Falcon-HubSpot Integration Dashboard...")

    try:
        # Import and run the app
        from app import app
        import uvicorn

        print("✅ Dashboard loaded successfully")
        print("🌐 Access at: http://localhost:8080")
        print("📖 API Docs: http://localhost:8080/docs")
        print("🎨 Beautiful UI with real-time status monitoring")
        print()
        print("Press Ctrl+C to stop the server")
        print("-" * 40)

        uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=True)

    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error starting dashboard: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Make sure you're in the correct directory")
        print("2. Install dependencies: pip install fastapi uvicorn")
        print("3. Check if port 8003 is available")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
