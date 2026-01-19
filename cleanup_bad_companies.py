#!/usr/bin/env python3
"""
Script to delete HubSpot companies that were incorrectly created
from non-AdminCard Wrike tasks.
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

# These are the Wrike task IDs that were incorrectly synced as companies
bad_wrike_ids = list(set([
    "MAAAAAEDI9Kb", "MAAAAAEDI9Kh", "MAAAAAEDI8HO", "MAAAAAEDI9KV",
    "MAAAAAEDIWBa", "MAAAAAEDI9Ki", "MAAAAAEDI8HD", "MAAAAAEDI9Kk",
    "MAAAAAEDIWBR", "MAAAAAEBQTAo", "MAAAAAEBQTAi", "MAAAAAEDI9KP",
    "MAAAAAEBQTAk", "MAAAAAEBQTAK", "MAAAAAEBQTAS", "MAAAAAECXqnZ"
]))

# These are the task names from Wrike that were incorrectly synced
bad_company_names = [
    "00 Ancillary Documents Check List",
    "00 Institution Info",
    "01_Kickoff Call",
    "02_Follow up Email",
    "02_Post-Kickoff Emails",
    "04_Draft 1 Client Review",
    "08_RS Draft 1 Due [GE] and Sent to Client",
    "10_ARPA-H Summary Solution Deadline",
    "11_RS Draft 1 Due [GE] and Sent to Client",
    "12_RS Draft 1 Client Review Received",
    "14_Client review & sign-off for upload due",
    "18_Client review & sign-off for upload due",
    "19_Submission Upload Process",
    "21_GE Internal Submission Process",
    "AdminCard_Narwhal Medical"  # This one might be a real company - let's skip it
]

# Remove AdminCard from the list - those are valid
bad_company_names = [n for n in bad_company_names if not n.startswith("AdminCard")]

print("=" * 80)
print("CLEANUP: Deleting incorrectly created HubSpot companies")
print("=" * 80)

# First, let's check from the local database
print("\n1. Checking local database for bad mappings...")
found_in_db = []
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    
    for wrike_id in bad_wrike_ids:
        cursor.execute(
            "SELECT wrike_company_id, hubspot_company_id, company_name FROM company_id_map WHERE wrike_company_id = ?",
            (wrike_id,)
        )
        row = cursor.fetchone()
        if row:
            found_in_db.append({
                'wrike_id': row[0],
                'hubspot_id': row[1],
                'name': row[2]
            })
            print(f"  Found in DB: {row[2]} (HubSpot: {row[1]}, Wrike: {row[0]})")
    
    conn.close()
except Exception as e:
    print(f"  Database error: {e}")

print(f"\n  Found {len(found_in_db)} records in local database")

# Now search HubSpot by company names
print("\n2. Searching HubSpot by company names...")
found_companies = []

for name in bad_company_names:
    try:
        search_body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "name",
                            "operator": "EQ",
                            "value": name
                        }
                    ]
                }
            ],
            "properties": ["name", "wrike_client_id"],
            "limit": 10
        }
        
        resp = requests.post(
            'https://api.hubspot.com/crm/v3/objects/companies/search',
            headers=headers,
            json=search_body
        )
        
        if resp.status_code == 200:
            results = resp.json().get('results', [])
            for company in results:
                company_id = company['id']
                company_name = company.get('properties', {}).get('name', 'Unknown')
                wrike_id = company.get('properties', {}).get('wrike_client_id', '')
                found_companies.append({
                    'id': company_id,
                    'name': company_name,
                    'wrike_id': wrike_id
                })
                print(f"  ✓ Found: {company_name} (ID: {company_id})")
        else:
            print(f"  ✗ Search for '{name}' failed: {resp.status_code}")
            
    except Exception as e:
        print(f"  ✗ Error searching for '{name}': {e}")

# Also check HubSpot IDs from the database
if found_in_db:
    print("\n3. Verifying HubSpot IDs from database...")
    for record in found_in_db:
        if record['hubspot_id'] and record not in [c for c in found_companies if c.get('id') == record['hubspot_id']]:
            try:
                resp = requests.get(
                    f"https://api.hubspot.com/crm/v3/objects/companies/{record['hubspot_id']}",
                    headers=headers,
                    params={'properties': 'name,wrike_client_id'}
                )
                if resp.status_code == 200:
                    company = resp.json()
                    found_companies.append({
                        'id': record['hubspot_id'],
                        'name': company.get('properties', {}).get('name', record['name']),
                        'wrike_id': record['wrike_id']
                    })
                    print(f"  ✓ Verified: {record['name']} (ID: {record['hubspot_id']})")
                elif resp.status_code == 404:
                    print(f"  ✗ Not in HubSpot: {record['name']} (ID: {record['hubspot_id']})")
            except Exception as e:
                print(f"  ✗ Error: {e}")

# Deduplicate
seen_ids = set()
unique_companies = []
for c in found_companies:
    if c['id'] not in seen_ids:
        seen_ids.add(c['id'])
        unique_companies.append(c)
found_companies = unique_companies

print(f"\n{'=' * 80}")
print(f"SEARCH RESULTS:")
print(f"  Companies found in HubSpot: {len(found_companies)}")
print(f"{'=' * 80}\n")

if not found_companies and not found_in_db:
    print("No companies to delete. Exiting.")
    exit(0)

# Show what will be deleted
print("Companies to be DELETED from HubSpot:")
for c in found_companies:
    print(f"  - {c['name']} (ID: {c['id']})")

print(f"\n⚠️  This will permanently delete {len(found_companies)} companies from HubSpot!")
print("Proceeding with deletion...\n")

# Delete companies from HubSpot
deleted = 0
failed = 0

for company in found_companies:
    try:
        resp = requests.delete(
            f"https://api.hubspot.com/crm/v3/objects/companies/{company['id']}",
            headers=headers
        )
        if resp.status_code == 204:
            print(f"  ✓ Deleted: {company['name']} (ID: {company['id']})")
            deleted += 1
        else:
            print(f"  ✗ Failed to delete {company['name']}: {resp.status_code} - {resp.text}")
            failed += 1
    except Exception as e:
        print(f"  ✗ Error deleting {company['name']}: {e}")
        failed += 1

# Clean up local database
print("\nCleaning up local database mappings...")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    
    deleted_from_db = 0
    for wrike_id in bad_wrike_ids:
        cursor.execute("DELETE FROM company_id_map WHERE wrike_company_id = ?", (wrike_id,))
        if cursor.rowcount > 0:
            deleted_from_db += cursor.rowcount
    
    conn.commit()
    conn.close()
    print(f"  ✓ Removed {deleted_from_db} mappings from local database")
except Exception as e:
    print(f"  ✗ Database cleanup error: {e}")

print(f"\n{'=' * 80}")
print("CLEANUP COMPLETE")
print(f"{'=' * 80}")
print(f"  HubSpot companies deleted: {deleted}")
print(f"  Failed deletions: {failed}")
