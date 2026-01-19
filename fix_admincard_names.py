#!/usr/bin/env python3
"""
PERMANENT FIX: Remove "AdminCard_" prefix from all HubSpot company names
and update local database to have correct company names.
"""
import requests
import json
import sqlite3

# Load config
with open('config.json') as f:
    config = json.load(f)

hubspot_token = config['hubspot']['access_token']
headers = {
    'Authorization': f'Bearer {hubspot_token}',
    'Content-Type': 'application/json'
}

print("=" * 80)
print("PERMANENT FIX: Removing 'AdminCard_' prefix from HubSpot company names")
print("=" * 80)

# Step 1: Find all HubSpot companies with "AdminCard" in name
print("\n1. Searching HubSpot for companies with 'AdminCard' in name...")
search_body = {
    "filterGroups": [
        {
            "filters": [
                {
                    "propertyName": "name",
                    "operator": "CONTAINS_TOKEN",
                    "value": "AdminCard"
                }
            ]
        }
    ],
    "properties": ["name", "wrike_client_id"],
    "limit": 100
}

resp = requests.post(
    'https://api.hubspot.com/crm/v3/objects/companies/search',
    headers=headers,
    json=search_body
)

if resp.status_code != 200:
    print(f"   ERROR: {resp.status_code} - {resp.text}")
    exit(1)

companies = resp.json().get('results', [])
print(f"   Found {len(companies)} companies with 'AdminCard' in name")

if not companies:
    print("\n   No companies to fix!")
else:
    # Step 2: Update each company to remove AdminCard_ prefix
    print("\n2. Updating HubSpot company names...")
    updated = 0
    failed = 0
    
    for company in companies:
        hub_id = company['id']
        current_name = company.get('properties', {}).get('name', '')
        
        # Remove AdminCard prefix (handle different formats)
        new_name = current_name
        if new_name.startswith('AdminCard_'):
            new_name = new_name[len('AdminCard_'):]
        elif new_name.startswith('AdminCard '):
            new_name = new_name[len('AdminCard '):]
        elif new_name.startswith('AdminCard-'):
            new_name = new_name[len('AdminCard-'):]
        
        new_name = new_name.strip()
        
        if new_name == current_name:
            print(f"   - Skipping {current_name} (no change needed)")
            continue
        
        try:
            resp = requests.patch(
                f'https://api.hubspot.com/crm/v3/objects/companies/{hub_id}',
                headers=headers,
                json={'properties': {'name': new_name}}
            )
            if resp.status_code == 200:
                print(f"   ✓ Updated: '{current_name}' → '{new_name}'")
                updated += 1
            else:
                print(f"   ✗ Failed: {current_name} - {resp.status_code}")
                failed += 1
        except Exception as e:
            print(f"   ✗ Error: {current_name} - {e}")
            failed += 1
    
    print(f"\n   HubSpot updates complete: {updated} updated, {failed} failed")

# Step 3: Fix local database company names
print("\n3. Fixing local database company names...")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    
    # Get all companies with AdminCard in name
    cursor.execute("SELECT wrike_company_id, company_name FROM company_id_map WHERE company_name LIKE '%AdminCard%'")
    rows = cursor.fetchall()
    
    print(f"   Found {len(rows)} database records to fix")
    
    db_updated = 0
    for wrike_id, old_name in rows:
        new_name = old_name
        if new_name.startswith('AdminCard_'):
            new_name = new_name[len('AdminCard_'):]
        elif new_name.startswith('AdminCard '):
            new_name = new_name[len('AdminCard '):]
        elif new_name.startswith('AdminCard-'):
            new_name = new_name[len('AdminCard-'):]
        new_name = new_name.strip()
        
        if new_name != old_name:
            cursor.execute(
                "UPDATE company_id_map SET company_name = ? WHERE wrike_company_id = ?",
                (new_name, wrike_id)
            )
            db_updated += 1
    
    conn.commit()
    conn.close()
    print(f"   ✓ Updated {db_updated} database records")
    
except Exception as e:
    print(f"   ✗ Database error: {e}")

print("\n" + "=" * 80)
print("FIX COMPLETE")
print("=" * 80)


