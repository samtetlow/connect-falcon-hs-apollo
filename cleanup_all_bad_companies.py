#!/usr/bin/env python3
"""
Script to delete ALL HubSpot companies that were incorrectly created
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

print("=" * 80)
print("CLEANUP: Deleting ALL incorrectly created HubSpot companies")
print("=" * 80)

# Get all non-AdminCard records from database
print("\n1. Finding non-AdminCard records in database...")
bad_records = []
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT wrike_company_id, hubspot_company_id, company_name 
        FROM company_id_map 
        WHERE company_name NOT LIKE 'AdminCard%'
    """)
    rows = cursor.fetchall()
    
    for row in rows:
        bad_records.append({
            'wrike_id': row[0],
            'hubspot_id': row[1],
            'name': row[2]
        })
        print(f"  ✗ BAD: {row[2]} (HubSpot: {row[1]})")
    
    conn.close()
except Exception as e:
    print(f"  Database error: {e}")

print(f"\n  Found {len(bad_records)} incorrect records")

if not bad_records:
    print("\nNo bad records to delete. Exiting.")
    exit(0)

# Delete from HubSpot
print(f"\n2. Deleting {len(bad_records)} companies from HubSpot...")
deleted = 0
failed = 0
not_found = 0

for record in bad_records:
    if not record['hubspot_id']:
        continue
    try:
        resp = requests.delete(
            f"https://api.hubspot.com/crm/v3/objects/companies/{record['hubspot_id']}",
            headers=headers
        )
        if resp.status_code == 204:
            print(f"  ✓ Deleted: {record['name']} (ID: {record['hubspot_id']})")
            deleted += 1
        elif resp.status_code == 404:
            print(f"  - Not found: {record['name']} (ID: {record['hubspot_id']})")
            not_found += 1
        else:
            print(f"  ✗ Failed: {record['name']} - {resp.status_code}")
            failed += 1
    except Exception as e:
        print(f"  ✗ Error: {record['name']} - {e}")
        failed += 1

# Clean up local database
print("\n3. Cleaning up local database...")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM company_id_map WHERE company_name NOT LIKE 'AdminCard%'")
    deleted_from_db = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"  ✓ Removed {deleted_from_db} records from local database")
except Exception as e:
    print(f"  ✗ Database cleanup error: {e}")

# Show remaining records
print("\n4. Remaining valid records:")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    cursor.execute("SELECT company_name FROM company_id_map LIMIT 10")
    rows = cursor.fetchall()
    for row in rows:
        print(f"  ✓ {row[0]}")
    cursor.execute("SELECT COUNT(*) FROM company_id_map")
    total = cursor.fetchone()[0]
    print(f"  ... Total: {total}")
    conn.close()
except:
    pass

print(f"\n{'=' * 80}")
print("CLEANUP COMPLETE")
print(f"{'=' * 80}")
print(f"  HubSpot companies deleted: {deleted}")
print(f"  Already gone from HubSpot: {not_found}")
print(f"  Failed deletions: {failed}")


