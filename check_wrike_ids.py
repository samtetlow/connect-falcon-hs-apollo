#!/usr/bin/env python3
"""Check if Wrike task IDs exist"""
import requests
import json

# Load config
with open('config.json') as f:
    config = json.load(f)

wrike_token = config['wrike']['api_token']
headers = {'Authorization': f'Bearer {wrike_token}'}

# Unique IDs to check
ids = [
    "MAAAAAEDI9Kb", "MAAAAAEDI9Kh", "MAAAAAEDI8HO", "MAAAAAEDI9KV",
    "MAAAAAEDIWBa", "MAAAAAEDI9Ki", "MAAAAAEDI8HD", "MAAAAAEDI9Kk",
    "MAAAAAEDIWBR", "MAAAAAEBQTAo", "MAAAAAEBQTAi", "MAAAAAEDI9KP",
    "MAAAAAEBQTAk", "MAAAAAEBQTAK", "MAAAAAEBQTAS", "MAAAAAECXqnZ"
]

print(f"Checking {len(ids)} unique Wrike task IDs...")
print("=" * 70)

found = []
not_found = []

for task_id in ids:
    try:
        resp = requests.get(f'https://www.wrike.com/api/v4/tasks/{task_id}', headers=headers)
        if resp.status_code == 200:
            data = resp.json().get('data', [])
            if data:
                task = data[0]
                title = task.get('title', 'Unknown')
                status = task.get('status', 'Unknown')
                found.append((task_id, title, status))
                print(f"✓ {task_id}: {title} [{status}]")
            else:
                not_found.append(task_id)
                print(f"✗ {task_id}: Empty response")
        else:
            not_found.append(task_id)
            print(f"✗ {task_id}: {resp.status_code} - {resp.text[:100]}")
    except Exception as e:
        not_found.append(task_id)
        print(f"✗ {task_id}: Error - {e}")

print("=" * 70)
print(f"\nSUMMARY:")
print(f"  Found: {len(found)}")
print(f"  Not Found: {len(not_found)}")

if not_found:
    print(f"\nNot Found IDs:")
    for nf in not_found:
        print(f"  - {nf}")


