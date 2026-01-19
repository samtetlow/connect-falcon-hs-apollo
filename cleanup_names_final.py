#!/usr/bin/env python3
"""
Script to delete HubSpot companies by name.
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

# Company names to delete (deduplicated)
bad_names = list(set([
    "20_Client Sign-off on Submission",
    "15_RS + CP Draft 2 in Review [Client + ER]",
    "17_Final Draft QC Due",
    "16_Final Draft Due [GE]",
    "12_RS Draft 1 Client Review Received",
    "18_Client review & sign-off for upload due",
    "19_Submission Upload Process",
    "11_RS Draft 1 Due [GE] and Sent to Client",
    "21_GE Internal Submission Process",
    "14_RS Draft 2 [GE]",
    "AdminCard_Palmos Labs",
    "AdminCard_Narwhal Medical",
    "AdminCard_Stepan",
    "04_Draft 1 Client Review",
    "05_Draft 2 [GE]",
    "06_Draft 2 Client Review",
    "08_Client Sign-off on Final Draft",
    "02_Follow up Email",
    "07_Final Draft [GE]",
    "03_Draft 1 [GE]",
    "09_Submission",
    "01_Kickoff Call",
    "00 Ancillary Documents Check List",
    "11_RS Draft 2 in Review [Client + External Review]",
    "15_Submission Upload Process",
    "13_Final Draft QC Due",
    "00 Institution Info",
    "02_Post-Kickoff Emails",
    "08_RS Draft 1 Due [GE] and Sent to Client",
    "18_GE Submission Deadline",
    "00 Request for External Review",
    "14_Client review & sign-off for upload due",
    "09_RS Draft 1 Client Review Received",
    "12_Final Draft Due [GE]",
    "10_RS Draft 2 [GE]",
    "17_GE Internal Submission Process",
    "16_Client Sign-off on Submission",
    "10_ARPA-H Summary Solution Deadline",
    "09_Submission (Client required)",
    "05_SAs Finalized",
    "04_Client SAs Review received",
    "06_SAs in External Review",
    "07_Program Officer Process",
    "03_Specific Aims (SAs) Draft due [GE]",
    "AdminCard_VyaHealth"
]))

print("=" * 80)
print("CLEANUP: Searching and deleting companies by name from HubSpot")
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
            print(f"  - Already gone: {company['name']}")
        else:
            print(f"  ✗ Failed: {company['name']} - {resp.status_code}")
            failed += 1
    except Exception as e:
        print(f"  ✗ Error: {e}")
        failed += 1

# Also clean from local database
print("\nCleaning local database...")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    deleted_from_db = 0
    for name in bad_names:
        cursor.execute("DELETE FROM company_id_map WHERE company_name = ?", (name,))
        deleted_from_db += cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  ✓ Removed {deleted_from_db} records from local database")
except Exception as e:
    print(f"  ✗ Database error: {e}")

print(f"\n{'=' * 80}")
print("CLEANUP COMPLETE")
print(f"{'=' * 80}")
print(f"  HubSpot companies deleted: {deleted}")
print(f"  Failed: {failed}")
print(f"  Not found (already clean): {len(not_found)}")


