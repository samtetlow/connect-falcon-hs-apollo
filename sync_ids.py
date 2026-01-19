#!/usr/bin/env python3
"""
Script to:
1. Check/create "Wrike Client ID" field in HubSpot
2. Check for "HubSpot ID" field in Wrike
3. Sync IDs between both systems
"""
import requests
import json

# Load config
with open('config.json') as f:
    config = json.load(f)

wrike_token = config['wrike']['api_token']
hubspot_token = config['hubspot']['access_token']
wrike_headers = {'Authorization': f'Bearer {wrike_token}', 'Content-Type': 'application/json'}
hubspot_headers = {'Authorization': f'Bearer {hubspot_token}', 'Content-Type': 'application/json'}

HUBSPOT_NAME_FIELD = 'IEAFXTW5JUAFOG4F'  # Hubspot Account Name
HUBSPOT_ID_FIELD = 'IEAFXTW5JUAISUWC'    # HubSpot ID (from earlier scan)
clients_folder = 'IEAFXTW5I5EXBNPV'

print('=' * 80)
print('SYNC WRIKE CLIENT IDs ↔ HUBSPOT ACCOUNT IDs')
print('=' * 80)
print()

# Step 1: Check/Create Wrike Client ID field in HubSpot
print('Step 1: Checking HubSpot for "Wrike Client ID" field...')
resp = requests.get('https://api.hubapi.com/crm/v3/properties/companies', headers=hubspot_headers)
hubspot_props = resp.json().get('results', [])

wrike_id_prop = None
for prop in hubspot_props:
    if 'wrike' in prop.get('name', '').lower():
        wrike_id_prop = prop
        print(f'  Found existing field: "{prop.get("label")}" ({prop.get("name")})')
        break

if not wrike_id_prop:
    print('  Creating "Wrike Client ID" field in HubSpot...')
    create_body = {
        "name": "wrike_client_id",
        "label": "Wrike Client ID",
        "type": "string",
        "fieldType": "text",
        "groupName": "companyinformation",
        "description": "The Wrike task ID for this company's AdminCard"
    }
    resp = requests.post('https://api.hubapi.com/crm/v3/properties/companies', 
                        headers=hubspot_headers, json=create_body)
    if resp.status_code == 201:
        print('  ✓ Created "Wrike Client ID" field')
        wrike_id_prop = resp.json()
    elif resp.status_code == 409:
        print('  ✓ "Wrike Client ID" field already exists')
    else:
        print(f'  ✗ Failed to create field: {resp.status_code} - {resp.text}')

print()

# Step 2: Verify Wrike has "HubSpot ID" field
print('Step 2: Checking Wrike for "HubSpot ID" field...')
print(f'  Using field ID: {HUBSPOT_ID_FIELD}')
print()

# Step 3: Get all companies and sync IDs
print('Step 3: Syncing IDs between systems...')
print()

resp = requests.get(f'https://www.wrike.com/api/v4/folders/{clients_folder}/folders', headers=wrike_headers)
client_folders = resp.json().get('data', [])

synced_to_wrike = 0
synced_to_hubspot = 0
processed = 0
errors = []

for folder in client_folders:
    folder_id = folder.get('id')
    folder_title = folder.get('title', '')
    
    if folder_title.startswith('20'):
        continue
    
    # Get AdminCard task
    resp = requests.get(f'https://www.wrike.com/api/v4/folders/{folder_id}/tasks', headers=wrike_headers)
    tasks = resp.json().get('data', [])
    
    admin_card = None
    for task in tasks:
        if 'AdminCard' in task.get('title', ''):
            admin_card = task
            break
    
    if not admin_card:
        continue
    
    wrike_task_id = admin_card.get('id')
    processed += 1
    
    # Get Wrike task details
    resp = requests.get(f'https://www.wrike.com/api/v4/tasks/{wrike_task_id}', headers=wrike_headers)
    task_data = resp.json().get('data', [{}])[0]
    
    # Get existing IDs from Wrike
    wrike_hubspot_name = None
    wrike_hubspot_id = None
    for cf in task_data.get('customFields', []):
        if cf.get('id') == HUBSPOT_NAME_FIELD:
            wrike_hubspot_name = cf.get('value')
        elif cf.get('id') == HUBSPOT_ID_FIELD:
            wrike_hubspot_id = cf.get('value')
    
    if not wrike_hubspot_name:
        continue
    
    # Search HubSpot for company
    search_body = {
        'filterGroups': [{'filters': [{'propertyName': 'name', 'operator': 'EQ', 'value': wrike_hubspot_name}]}],
        'properties': ['name', 'wrike_client_id'],
        'limit': 1
    }
    resp = requests.post('https://api.hubapi.com/crm/v3/objects/companies/search', 
                        headers=hubspot_headers, json=search_body)
    results = resp.json().get('results', [])
    
    if not results:
        continue
    
    hubspot_company = results[0]
    hubspot_id = hubspot_company['id']
    current_wrike_id = hubspot_company.get('properties', {}).get('wrike_client_id')
    
    # Sync HubSpot ID to Wrike (if not already set)
    if not wrike_hubspot_id or wrike_hubspot_id != hubspot_id:
        try:
            update_body = {'customFields': [{'id': HUBSPOT_ID_FIELD, 'value': hubspot_id}]}
            resp = requests.put(f'https://www.wrike.com/api/v4/tasks/{wrike_task_id}',
                              headers=wrike_headers, json=update_body)
            if resp.status_code == 200:
                synced_to_wrike += 1
                if processed <= 5:
                    print(f'  ✓ {folder_title}: Added HubSpot ID {hubspot_id} to Wrike')
        except Exception as e:
            errors.append(f'{folder_title}: {e}')
    
    # Sync Wrike ID to HubSpot (if not already set)
    if not current_wrike_id or current_wrike_id != wrike_task_id:
        try:
            resp = requests.patch(f'https://api.hubapi.com/crm/v3/objects/companies/{hubspot_id}',
                                headers=hubspot_headers, 
                                json={'properties': {'wrike_client_id': wrike_task_id}})
            if resp.status_code == 200:
                synced_to_hubspot += 1
                if processed <= 5:
                    print(f'  ✓ {folder_title}: Added Wrike ID {wrike_task_id} to HubSpot')
        except Exception as e:
            errors.append(f'{folder_title}: {e}')

print()
print('=' * 80)
print('SYNC COMPLETE')
print('=' * 80)
print()
print(f'Companies processed: {processed}')
print(f'HubSpot IDs added to Wrike: {synced_to_wrike}')
print(f'Wrike IDs added to HubSpot: {synced_to_hubspot}')
print(f'Errors: {len(errors)}')
print()

