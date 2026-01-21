#!/usr/bin/env python3
"""Check HubSpot for companies with AdminCard in name."""
import requests
import json

with open('config.json') as f:
    config = json.load(f)

hubspot_token = config['hubspot']['access_token']
headers = {
    'Authorization': f'Bearer {hubspot_token}',
    'Content-Type': 'application/json'
}

# Search for companies with AdminCard in name
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

if resp.status_code == 200:
    results = resp.json().get('results', [])
    print(f"Found {len(results)} HubSpot companies with 'AdminCard' in name:")
    print("=" * 80)
    for company in results:
        name = company.get('properties', {}).get('name', 'N/A')
        wrike_id = company.get('properties', {}).get('wrike_client_id', 'N/A')
        hub_id = company.get('id')
        print(f"  ID: {hub_id}")
        print(f"  Name: {name}")
        print(f"  Wrike ID: {wrike_id}")
        print("-" * 40)
else:
    print(f"Error: {resp.status_code} - {resp.text}")



