#!/usr/bin/env python3
"""
Script to delete HubSpot companies by their Wrike Client IDs.
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

# Wrike Client IDs to delete (deduplicated)
bad_wrike_ids = list(set([
    "MAAAAAEBQTA3", "MAAAAAEBQTAa", "MAAAAAEBQTAc", "MAAAAAEBQTAg",
    "MAAAAAEBQTAi", "MAAAAAEBQTAk", "MAAAAAEBQTAK", "MAAAAAEBQTAo",
    "MAAAAAEBQTAS", "MAAAAAEBQTAz", "MAAAAAECoRAZ", "MAAAAAECXqnZ",
    "MAAAAAEDI-nu", "MAAAAAEDI8HD", "MAAAAAEDI8HE", "MAAAAAEDI8HG",
    "MAAAAAEDI8HH", "MAAAAAEDI8HI", "MAAAAAEDI8HK", "MAAAAAEDI8HL",
    "MAAAAAEDI8HM", "MAAAAAEDI8HO", "MAAAAAEDI9Kb", "MAAAAAEDI9Kd",
    "MAAAAAEDI9Kf", "MAAAAAEDI9Kg", "MAAAAAEDI9Kh", "MAAAAAEDI9Ki",
    "MAAAAAEDI9Kk", "MAAAAAEDI9Kl", "MAAAAAEDI9Kn", "MAAAAAEDI9KP",
    "MAAAAAEDI9KQ", "MAAAAAEDI9KS", "MAAAAAEDI9KT", "MAAAAAEDI9KU",
    "MAAAAAEDI9KV", "MAAAAAEDI9KY", "MAAAAAEDIWBa", "MAAAAAEDIWBR",
    "MAAAAAEDJNVa", "MAAAAAEDJNVb", "MAAAAAEDJNVc", "MAAAAAEDJNVd",
    "MAAAAAEDJNVe", "MAAAAAEDJNVf", "MAAAAAEDJNVg", "MAAAAAEDJNVX",
    "MAAAAAEDJNVY", "MAAAAAEDJNVZ", "MAAAAAEDKWV-", "MAAAAAEDKWV1",
    "MAAAAAEDKWV3", "MAAAAAEDKWV8", "MAAAAAEDKWVv", "MAAAAAEDKWWC",
    "MAAAAAEDKWWd", "MAAAAAEDKWWE", "MAAAAAEDKWWg", "MAAAAAEDKWWJ",
    "MAAAAAEDKWWM", "MAAAAAEDKWWP", "MAAAAAEDKWWU", "MAAAAAEDKWWW",
    "MAAAAAEDKWWY", "MAAAAAEDQ-ht", "MAAAAAEDQnp0", "MAAAAAEDQnp8",
    "MAAAAAEDQnqa", "MAAAAAEDQnqe", "MAAAAAEDQnqI", "MAAAAAEDQnqj",
    "MAAAAAEDQnqN", "MAAAAAEDQnqR", "MAAAAAEDQnqr", "MAAAAAEDQnqU",
    "MAAAAAEDQqhc", "MAAAAAEDQqhg", "MAAAAAEDQqhj", "MAAAAAEDQqhZ"
]))

print("=" * 80)
print("CLEANUP: Deleting HubSpot companies by Wrike Client ID")
print("=" * 80)
print(f"\nSearching for {len(bad_wrike_ids)} unique Wrike IDs in HubSpot...\n")

# First, get ALL companies from HubSpot with wrike_client_id property
print("Fetching all companies from HubSpot...")
all_companies = []
after = None

while True:
    params = {
        'limit': 100,
        'properties': 'name,wrike_client_id'
    }
    if after:
        params['after'] = after
    
    resp = requests.get(
        'https://api.hubspot.com/crm/v3/objects/companies',
        headers=headers,
        params=params
    )
    
    if resp.status_code == 200:
        data = resp.json()
        results = data.get('results', [])
        all_companies.extend(results)
        
        paging = data.get('paging', {})
        next_page = paging.get('next', {})
        after = next_page.get('after')
        
        if not after:
            break
    else:
        print(f"  Error fetching companies: {resp.status_code}")
        break

print(f"  Found {len(all_companies)} total companies in HubSpot\n")

# Find companies with matching wrike_client_ids
found_companies = []
for company in all_companies:
    wrike_id = company.get('properties', {}).get('wrike_client_id', '')
    if wrike_id in bad_wrike_ids:
        found_companies.append({
            'id': company['id'],
            'name': company.get('properties', {}).get('name', 'Unknown'),
            'wrike_id': wrike_id
        })
        print(f"  ✓ Found: {company.get('properties', {}).get('name', 'Unknown')} (Wrike: {wrike_id})")

print(f"\n{'=' * 80}")
print(f"SEARCH RESULTS:")
print(f"  Companies to delete: {len(found_companies)}")
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
            print(f"  ✓ Deleted: {company['name']} (ID: {company['id']}, Wrike: {company['wrike_id']})")
            deleted += 1
        elif resp.status_code == 404:
            print(f"  - Already gone: {company['name']}")
        else:
            print(f"  ✗ Failed: {company['name']} - {resp.status_code}")
            failed += 1
    except Exception as e:
        print(f"  ✗ Error: {e}")
        failed += 1

# Clean local database
print("\nCleaning local database...")
try:
    conn = sqlite3.connect('sync.db')
    cursor = conn.cursor()
    deleted_from_db = 0
    for wrike_id in bad_wrike_ids:
        cursor.execute("DELETE FROM company_id_map WHERE wrike_company_id = ?", (wrike_id,))
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


