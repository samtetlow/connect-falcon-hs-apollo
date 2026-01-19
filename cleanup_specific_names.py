#!/usr/bin/env python3
"""
Script to delete specific company names from HubSpot.
"""
import requests
import json

# Load config
with open('config.json') as f:
    config = json.load(f)

hubspot_token = config['hubspot']['access_token']
headers = {
    'Authorization': f'Bearer {hubspot_token}',
    'Content-Type': 'application/json'
}

# Specific company names to delete (deduplicated)
bad_names = list(set([
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
    "AdminCard_Narwhal Medical"
]))

print("=" * 80)
print("CLEANUP: Searching and deleting specific company names from HubSpot")
print("=" * 80)
print(f"\nSearching for {len(bad_names)} unique company names...\n")

found_companies = []
not_found = []

for name in bad_names:
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
            "properties": ["name"],
            "limit": 100
        }
        
        resp = requests.post(
            'https://api.hubspot.com/crm/v3/objects/companies/search',
            headers=headers,
            json=search_body
        )
        
        if resp.status_code == 200:
            results = resp.json().get('results', [])
            if results:
                for company in results:
                    found_companies.append({
                        'id': company['id'],
                        'name': company.get('properties', {}).get('name', name)
                    })
                    print(f"  ✓ Found: {name} (ID: {company['id']})")
            else:
                not_found.append(name)
                print(f"  - Not found: {name}")
        else:
            print(f"  ✗ Search error for '{name}': {resp.status_code}")
            
    except Exception as e:
        print(f"  ✗ Error: {e}")

print(f"\n{'=' * 80}")
print(f"SEARCH RESULTS:")
print(f"  Companies found: {len(found_companies)}")
print(f"  Not found: {len(not_found)}")
print(f"{'=' * 80}\n")

if not found_companies:
    print("No companies to delete. All clean!")
    exit(0)

# Delete found companies
print(f"Deleting {len(found_companies)} companies from HubSpot...\n")
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
        elif resp.status_code == 404:
            print(f"  - Already gone: {company['name']} (ID: {company['id']})")
        else:
            print(f"  ✗ Failed: {company['name']} - {resp.status_code}")
            failed += 1
    except Exception as e:
        print(f"  ✗ Error: {e}")
        failed += 1

# Also clean from local database
import sqlite3
print("\nCleaning local database...")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    for name in bad_names:
        cursor.execute("DELETE FROM company_id_map WHERE company_name = ?", (name,))
    conn.commit()
    conn.close()
    print("  ✓ Local database cleaned")
except Exception as e:
    print(f"  ✗ Database error: {e}")

print(f"\n{'=' * 80}")
print("CLEANUP COMPLETE")
print(f"{'=' * 80}")
print(f"  HubSpot companies deleted: {deleted}")
print(f"  Failed: {failed}")
print(f"  Not found (already clean): {len(not_found)}")


