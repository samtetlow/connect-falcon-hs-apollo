"""
HubSpot Integration Module
Handles all HubSpot API interactions including reading deals, contacts, and updating fields.
Uses requests only to avoid library dependency issues.
"""

import json
import logging
import time
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class HubSpotClient:
    """HubSpot API integration using direct requests"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize HubSpot client with configuration

        Args:
            config: Configuration dictionary with api_key or access_token
        """
        self.access_token = config.get("access_token", "")
        self.api_key = config.get("api_key", "")  # Legacy API key (deprecated)
        self.base_url = "https://api.hubapi.com"
        self.session = requests.Session()
        
        # Use Bearer token if access_token provided, otherwise try legacy hapikey
        if self.access_token:
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
            self.auth_method = "bearer"
            logger.info("HubSpot client initialized with Bearer token")
        elif self.api_key:
            self.session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
            self.auth_method = "hapikey"
            logger.info("HubSpot client initialized with legacy API key (deprecated)")
        else:
            self.auth_method = None
            logger.warning("HubSpot client initialized without API credentials")

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make request with basic error handling"""
        url = f"{self.base_url}{path}"
        
        # Add legacy hapikey param if using that auth method
        if self.auth_method == "hapikey" and self.api_key:
            params = kwargs.get("params", {})
            if isinstance(params, dict):
                params["hapikey"] = self.api_key
            kwargs["params"] = params
        
        try:
            response = self.session.request(method, url, timeout=30, **kwargs)
            if response.status_code == 429:
                logger.warning("HubSpot rate limit hit")
                time.sleep(1)
                return self._request(method, path, **kwargs)
            
            if response.status_code >= 400:
                logger.error(f"HubSpot API error {response.status_code}: {response.text}")
            
            return response
        except Exception as e:
            logger.error(f"HubSpot request exception: {e}")
            raise

    # ==================== DEALS ====================

    def get_deal(self, deal_id: str, properties: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get a deal by ID"""
        params = {"properties": properties or []}
        response = self._request("GET", f"/crm/v3/objects/deals/{deal_id}", params=params)
        response.raise_for_status()
        return response.json()

    def update_deal(self, deal_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Update deal properties"""
        response = self._request("PATCH", f"/crm/v3/objects/deals/{deal_id}", json={"properties": properties})
        response.raise_for_status()
        return response.json()

    def create_deal(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new deal"""
        response = self._request("POST", "/crm/v3/objects/deals", json={"properties": properties})
        response.raise_for_status()
        return response.json()

    def associate_deal_to_company(self, deal_id: str, company_id: str) -> bool:
        """Associate a deal with a company"""
        # Definition: deal (0-3) to company (0-2)
        # Association type ID 3 for deal to company
        path = f"/crm/v3/associations/deals/companies/batch/create"
        body = {
            "inputs": [
                {
                    "from": {"id": deal_id},
                    "to": {"id": company_id},
                    "type": "deal_to_company"
                }
            ]
        }
        response = self._request("POST", path, json=body)
        return response.status_code < 400

    # ==================== CONTACTS ====================

    def find_contact_by_email(self, email: str, properties: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Find contact by email"""
        body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": email
                        }
                    ]
                }
            ],
            "properties": properties or ["firstname", "lastname", "email"]
        }
        response = self._request("POST", "/crm/v3/objects/contacts/search", json=body)
        if response.status_code == 200:
            results = response.json().get("results", [])
            return results[0] if results else None
        return None

    def create_contact(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new contact"""
        response = self._request("POST", "/crm/v3/objects/contacts", json={"properties": properties})
        response.raise_for_status()
        return response.json()

    # ==================== COMPANIES ====================

    def find_company_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find company by name"""
        body = {
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
            "properties": ["name", "domain"]
        }
        response = self._request("POST", "/crm/v3/objects/companies/search", json=body)
        if response.status_code == 200:
            results = response.json().get("results", [])
            return results[0] if results else None
        return None

    # ==================== UTILITY ====================

    def test_connection(self) -> bool:
        """Test connection by fetching a single property"""
        if not self.api_key:
            return False
        response = self._request("GET", "/crm/v3/properties/deals", params={"limit": 1})
        return response.status_code == 200
