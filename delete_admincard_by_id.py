#!/usr/bin/env python3
"""
Delete specific HubSpot companies by ID.
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

# Company IDs to check and delete
companies_to_delete = [
    ("AdminCard_100X Bio", "49218623299"),
    ("AdminCard_Absci Bio", "49283119543"),
    ("AdminCard_Ace Vision Group Inc", "49198151435"),
    ("AdminCard_Adjuvant Genomics", "49253236857"),
    ("AdminCard_AgonOx", "49199724582"),
    ("AdminCard_Aisa Pharma", "49213496859"),
    ("AdminCard_Ashvattha Therapeutics Inc.", "49275517704"),
    ("AdminCard_AtaiLifeScience", "49253700527"),
    ("AdminCard_Black Fur", "49079040919"),
    ("AdminCard_Bullfrog AI Holdings INC", "49307959911"),
    ("AdminCard_Capelli Bio", "49149032756"),
    ("AdminCard_CellFe Inc.", "49250412712"),
    ("AdminCard_Ceria Therapeutics", "49317353308"),
    ("AdminCard_Cogentis Therapeutics, Inc.", "49297607023"),
    ("AdminCard_CorNav", "49116532613"),
    ("AdminCard_Endiatx", "49216147771"),
    ("AdminCard_Geneius Biotechnology Inc.", "49080439440"),
    ("AdminCard_Hexagon Bio, Inc", "49296201872"),
    ("AdminCard_Ichor Sciences", "49297301355"),
    ("AdminCard_Immune Age", "49303747474"),
    ("AdminCard_IVSonance Biomedical Inc.", "49231643522"),
    ("AdminCard_Katz Diagnostics Inc", "49296201873"),
    ("AdminCard_LIMBER Prosthetics and Orthotics", "49197687709"),
    ("AdminCard_Olio Labs Incorporated", "49190442073"),
    ("AdminCard_Oncotab, INC", "49242820927"),
    ("AdminCard_PatchClamp MedTech, Inc. - University of Washington", "49210717282"),
    ("AdminCard_Platelet Biogenesis, Inc", "49286209958"),
    ("AdminCard_Protuoso, Inc", "49091693190"),
    ("AdminCard_Pulsar", "49241740852"),
    ("AdminCard_Purgo Scientific", "49246222514"),
    ("AdminCard_Qurasense", "49287294041"),
    ("AdminCard_RHNanoPharma", "49259012660"),
    ("AdminCard_Savanna (formerly NovoGlia)", "49077957815"),
    ("AdminCard_Sikaram Labs", "49286209956"),
    ("AdminCard_Survey Genomics Inc.", "49333547269"),
    ("AdminCard_TearSolutions Inc.", "49294663004"),
    ("AdminCard_Tetracore Inc", "49065441433"),
    ("AdminCard_TFC Therapeutics Inc.", "49104311044"),
    ("AdminCard_UNandUP LLC", "49293271189"),
    ("AdminCard_Uvantis Therapeutics", "49200333186"),
    ("AdminCard_Vacunax Inc.", "49333543240"),
    ("AdminCard_VitaWave Tech", "49080439439"),
]

print("=" * 80)
print("Checking and deleting AdminCard companies from HubSpot")
print("=" * 80)
print(f"\nChecking {len(companies_to_delete)} company IDs...\n")

found = 0
deleted = 0
not_found = 0
failed = 0

for name, company_id in companies_to_delete:
    try:
        # Check if company exists
        resp = requests.get(
            f'https://api.hubspot.com/crm/v3/objects/companies/{company_id}',
            headers=headers,
            params={'properties': 'name'}
        )
        
        if resp.status_code == 200:
            actual_name = resp.json().get('properties', {}).get('name', 'Unknown')
            print(f"✓ Found: {actual_name} (ID: {company_id})")
            found += 1
            
            # Delete the company
            del_resp = requests.delete(
                f'https://api.hubspot.com/crm/v3/objects/companies/{company_id}',
                headers=headers
            )
            
            if del_resp.status_code == 204:
                print(f"  → Deleted successfully")
                deleted += 1
            else:
                print(f"  → Failed to delete: {del_resp.status_code}")
                failed += 1
                
        elif resp.status_code == 404:
            print(f"- Not found: {name} (ID: {company_id})")
            not_found += 1
        else:
            print(f"✗ Error checking {name}: {resp.status_code}")
            failed += 1
            
    except Exception as e:
        print(f"✗ Error: {name} - {e}")
        failed += 1

print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")
print(f"  Companies found in HubSpot: {found}")
print(f"  Successfully deleted: {deleted}")
print(f"  Not found (already deleted): {not_found}")
print(f"  Failed: {failed}")


