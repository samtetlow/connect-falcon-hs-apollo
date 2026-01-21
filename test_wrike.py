#!/usr/bin/env python3
"""Test Wrike API connectivity and rate limits"""

import yaml
import requests
import time

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

wrike_token = config['wrike']['token']
folder_id = config['wrike']['folder_id']

headers = {
    "Authorization": f"Bearer {wrike_token}",
    "Content-Type": "application/json"
}

print("=" * 50)
print("WRIKE API TEST")
print("=" * 50)

# Test 1: Get folder info
print("\n1. Testing folder access...")
url = f"https://www.wrike.com/api/v4/folders/{folder_id}"
response = requests.get(url, headers=headers)
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    folder_data = response.json()
    folder_name = folder_data.get('data', [{}])[0].get('title', 'Unknown')
    print(f"   ✓ Folder: {folder_name}")
elif response.status_code == 429:
    print(f"   ✗ RATE LIMITED!")
    print(f"   Retry-After: {response.headers.get('Retry-After', 'N/A')}")
else:
    print(f"   ✗ Error: {response.text[:200]}")

# Test 2: Get tasks
print("\n2. Testing task retrieval...")
time.sleep(1)
url = f"https://www.wrike.com/api/v4/folders/{folder_id}/tasks?descendants=true&pageSize=1"
response = requests.get(url, headers=headers)
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    tasks = response.json().get('data', [])
    if tasks:
        task = tasks[0]
        print(f"   ✓ Found task: {task.get('title', 'Unknown')[:40]}...")
        print(f"   Task ID: {task.get('id')}")
    else:
        print("   ⚠ No tasks found in folder")
elif response.status_code == 429:
    print(f"   ✗ RATE LIMITED!")
else:
    print(f"   ✗ Error: {response.text[:200]}")

# Test 3: Rapid request test
print("\n3. Testing rate limits (5 requests with 1s delay)...")
rate_limited = 0
for i in range(5):
    time.sleep(1)
    url = f"https://www.wrike.com/api/v4/folders/{folder_id}"
    response = requests.get(url, headers=headers)
    status = "✓ OK" if response.status_code == 200 else "✗ RATE LIMITED" if response.status_code == 429 else f"? {response.status_code}"
    print(f"   Request {i+1}: {status}")
    if response.status_code == 429:
        rate_limited += 1

print("\n" + "=" * 50)
if rate_limited == 0:
    print("✅ RESULT: Wrike API working, no rate limits with 1s delay")
else:
    print(f"⚠ RESULT: {rate_limited}/5 requests were rate limited")
print("=" * 50)


