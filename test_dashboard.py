#!/usr/bin/env python3
"""
Test script to diagnose dashboard issues
Run this to check if the dashboard is working properly
"""

import subprocess
import sys
import time
import requests

def check_server_running():
    """Check if server is running on port 8004"""
    try:
        result = subprocess.run(['lsof', '-i', ':8004'],
                              capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

def test_dashboard():
    """Test the dashboard endpoint"""
    try:
        response = requests.get('http://localhost:8004/', timeout=5)
        print(f"✅ Dashboard Status: {response.status_code}")

        if response.status_code == 200:
            content = response.text.lower()
            checks = [
                ('HTML structure', '<!doctype html>' in content),
                ('WrikeHub title', 'wrikehub' in content),
                ('CSS styles', 'var(--primary' in content),
                ('Status indicators', 'status-dot' in content),
            ]

            for check_name, passed in checks:
                status = '✅' if passed else '❌'
                print(f"   {status} {check_name}")

            if all(passed for _, passed in checks):
                print("🎉 Dashboard is working perfectly!")
                return True
            else:
                print("❌ Dashboard content has issues")
                return False
        else:
            print(f"❌ Bad response: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to dashboard: {e}")
        return False

def main():
    print("🦅 WrikeHub Dashboard Diagnostic")
    print("=" * 40)

    # Check if server is running
    if not check_server_running():
        print("❌ No server running on port 8003")
        print("🚀 Starting server...")

        # Start server in background
        try:
            server_process = subprocess.Popen([
                sys.executable, 'start_dashboard.py'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            print("⏳ Waiting for server to start...")
            time.sleep(3)

            if server_process.poll() is None:
                print("✅ Server started successfully")
            else:
                print("❌ Server failed to start")
                stdout, stderr = server_process.communicate()
                print(f"stdout: {stdout.decode()}")
                print(f"stderr: {stderr.decode()}")
                return 1

        except Exception as e:
            print(f"❌ Failed to start server: {e}")
            return 1

    # Test dashboard
    if test_dashboard():
        print("\n🌐 Dashboard URL: http://localhost:8004")
        print("📖 API Docs: http://localhost:8004/docs")
        return 0
    else:
        print("\n🔧 Troubleshooting:")
        print("1. Check that dashboard.html exists")
        print("2. Make sure port 8004 is not blocked by firewall")
        print("3. Try restarting the server")
        return 1

if __name__ == "__main__":
    sys.exit(main())
