#!/usr/bin/env python3
"""
Map Wrike Client IDs to HubSpot companies.
This script will:
1. Get all AdminCard tasks from Wrike
2. For each task, find the matching HubSpot company by name
3. Update the HubSpot company's wrike_client_id field with the Wrike task ID
"""

import yaml
import requests
import time
import re

# Load config
print("Loading configuration...")

# Load config.json for API tokens
import json
with open('config.json', 'r') as f:
    config_json = json.load(f)

# Load config.yaml for folder IDs
with open('config.yaml', 'r') as f:
    config_yaml = yaml.safe_load(f)

# Wrike setup
wrike_token = config_json.get('wrike', {}).get('api_token')
folder_id = config_yaml['wrike']['companies_folder_id']
wrike_headers = {
    "Authorization": f"Bearer {wrike_token}",
    "Content-Type": "application/json"
}

# HubSpot setup
hubspot_token = config_json.get('hubspot', {}).get('access_token') or config_json.get('hubspot', {}).get('api_key')
hubspot_headers = {
    "Authorization": f"Bearer {hubspot_token}",
    "Content-Type": "application/json"
}

print(f"   Wrike token: {'✓ Configured' if wrike_token else '✗ Missing'}")
print(f"   HubSpot token: {'✓ Configured' if hubspot_token else '✗ Missing'}")
print(f"   Folder ID: {folder_id}")

def clean_company_name(name):
    """Remove AdminCard_ prefix and clean the name for matching"""
    # Remove various AdminCard prefixes
    cleaned = re.sub(r'^AdminCard[_\s-]+', '', name, flags=re.IGNORECASE)
    return cleaned.strip()

def get_all_wrike_tasks():
    """Get all tasks from Wrike folder"""
    print("\n📥 Fetching all Wrike tasks...")
    all_tasks = []
    next_page = None
    
    while True:
        url = f"https://www.wrike.com/api/v4/folders/{folder_id}/tasks?descendants=true&pageSize=100"
        if next_page:
            url += f"&nextPageToken={next_page}"
        
        time.sleep(1)  # Respect rate limits
        response = requests.get(url, headers=wrike_headers)
        
        if response.status_code != 200:
            print(f"   Error fetching tasks: {response.status_code}")
            break
            
        data = response.json()
        tasks = data.get('data', [])
        all_tasks.extend(tasks)
        
        next_page = data.get('nextPageToken')
        print(f"   Fetched {len(all_tasks)} tasks so far...")
        
        if not next_page:
            break
    
    # Filter to AdminCard tasks only
    admin_tasks = [t for t in all_tasks if 'AdminCard' in t.get('title', '')]
    print(f"   ✓ Found {len(admin_tasks)} AdminCard tasks out of {len(all_tasks)} total")
    return admin_tasks

def search_hubspot_company(company_name):
    """Search for a HubSpot company by name"""
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    
    # Try exact match first
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "name",
                "operator": "EQ",
                "value": company_name
            }]
        }],
        "properties": ["name", "wrike_client_id"],
        "limit": 1
    }
    
    time.sleep(0.15)  # Respect rate limits
    response = requests.post(url, headers=hubspot_headers, json=payload)
    
    if response.status_code == 200:
        results = response.json().get('results', [])
        if results:
            return results[0]
    
    # Try contains match if exact match fails
    payload["filterGroups"][0]["filters"][0]["operator"] = "CONTAINS_TOKEN"
    
    time.sleep(0.15)
    response = requests.post(url, headers=hubspot_headers, json=payload)
    
    if response.status_code == 200:
        results = response.json().get('results', [])
        if results:
            return results[0]
    
    return None

def update_hubspot_wrike_id(hubspot_id, wrike_task_id):
    """Update the wrike_client_id field in HubSpot"""
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{hubspot_id}"
    
    payload = {
        "properties": {
            "wrike_client_id": wrike_task_id
        }
    }
    
    time.sleep(0.15)  # Respect rate limits
    response = requests.patch(url, headers=hubspot_headers, json=payload)
    
    return response.status_code == 200

def main():
    print("=" * 60)
    print("WRIKE CLIENT ID → HUBSPOT MAPPING")
    print("=" * 60)
    
    # Get all Wrike tasks
    wrike_tasks = get_all_wrike_tasks()
    
    if not wrike_tasks:
        print("No AdminCard tasks found in Wrike!")
        return
    
    # Process each task
    print("\n🔄 Mapping Wrike IDs to HubSpot companies...")
    
    stats = {
        "processed": 0,
        "mapped": 0,
        "already_mapped": 0,
        "not_found": 0,
        "failed": 0
    }
    
    not_found_companies = []
    
    for i, task in enumerate(wrike_tasks, 1):
        wrike_task_id = task.get('id')
        wrike_title = task.get('title', '')
        company_name = clean_company_name(wrike_title)
        
        stats["processed"] += 1
        
        # Progress indicator
        if i % 10 == 0 or i == len(wrike_tasks):
            print(f"   Processing {i}/{len(wrike_tasks)}...")
        
        # Search for HubSpot company
        hubspot_company = search_hubspot_company(company_name)
        
        if not hubspot_company:
            stats["not_found"] += 1
            not_found_companies.append(company_name)
            continue
        
        hubspot_id = hubspot_company.get('id')
        existing_wrike_id = hubspot_company.get('properties', {}).get('wrike_client_id')
        
        # Check if already mapped
        if existing_wrike_id == wrike_task_id:
            stats["already_mapped"] += 1
            continue
        
        # Update HubSpot with Wrike ID
        if update_hubspot_wrike_id(hubspot_id, wrike_task_id):
            stats["mapped"] += 1
            print(f"   ✓ Mapped: {company_name} → {wrike_task_id}")
        else:
            stats["failed"] += 1
            print(f"   ✗ Failed: {company_name}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 MAPPING SUMMARY")
    print("=" * 60)
    print(f"   Total Processed: {stats['processed']}")
    print(f"   ✓ Newly Mapped: {stats['mapped']}")
    print(f"   ✓ Already Mapped: {stats['already_mapped']}")
    print(f"   ⚠ Not Found in HubSpot: {stats['not_found']}")
    print(f"   ✗ Failed: {stats['failed']}")
    
    if not_found_companies and len(not_found_companies) <= 20:
        print("\n   Companies not found in HubSpot:")
        for name in not_found_companies[:20]:
            print(f"      - {name}")
        if len(not_found_companies) > 20:
            print(f"      ... and {len(not_found_companies) - 20} more")
    
    print("\n" + "=" * 60)
    print("✅ MAPPING COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()

