#!/usr/bin/env python3
"""Check what these Wrike task IDs are."""
import requests
import json

with open('config.json') as f:
    config = json.load(f)

wrike_token = config['wrike']['api_token']
headers = {'Authorization': f'Bearer {wrike_token}'}

task_ids = ["MAAAAABpMxnb", "MAAAAABqliA4", "MAAAAABqrsKi"]

print("Checking Wrike Task IDs:")
print("=" * 60)

for task_id in task_ids:
    try:
        resp = requests.get(
            f'https://www.wrike.com/api/v4/tasks/{task_id}',
            headers=headers
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get('data'):
                task = data['data'][0]
                print(f"\n✓ {task_id}")
                print(f"  Title: {task.get('title', 'N/A')}")
                print(f"  Status: {task.get('status', 'N/A')}")
        else:
            print(f"\n✗ {task_id}: {resp.status_code} - {resp.text[:100]}")
    except Exception as e:
        print(f"\n✗ {task_id}: Error - {e}")


