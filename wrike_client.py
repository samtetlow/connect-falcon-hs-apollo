"""
Wrike Integration Module
Handles all Wrike API interactions including managing folders, projects, and tasks.
"""

import json
import logging
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class WrikeClient:
    """Wrike API integration for workflow management"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Wrike client with configuration

        Args:
            config: Configuration dictionary with api_token, client_id, client_secret, base_url, timeout
        """
        self.api_token = config.get("api_token", "")
        self.client_id = config.get("client_id", "")
        self.client_secret = config.get("client_secret", "")
        self.base_url = config.get("base_url", "https://www.wrike.com/api/v4").rstrip('/')
        self.timeout = config.get("timeout", 30)

        # Priority: Permanent Access Token > OAuth App Credentials
        auth_header = f"bearer {self.api_token}" if self.api_token else ""
        
        self.headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json"
        }

        # Test connection on init if token exists
        if self.api_token:
            try:
                self.get_user_info()
                logger.info("Wrike client initialized with token")
            except Exception as e:
                logger.warning(f"Initial Wrike connection attempt failed: {e}")
        elif self.client_id and self.client_secret:
            logger.info("Wrike client configured with OAuth credentials (Token required for API calls)")
        else:
            logger.warning("Wrike client initialized without authentication credentials")

    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Internal method to handle API requests
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=data,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Wrike API error ({method} {endpoint}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise Exception(f"Wrike API request failed: {str(e)}")

    def get_user_info(self) -> Dict[str, Any]:
        """Get current user info to verify connection"""
        return self._request("GET", "account")

    # ==================== FOLDERS & PROJECTS ====================

    def search_folders(self, name: str, permalink: bool = False) -> List[Dict[str, Any]]:
        """
        Search for folders or projects by name
        """
        # Using the folder list endpoint
        response = self._request("GET", "folders")
        return response.get('data', [])

    def create_folder(self, title: str, parent_id: Optional[str] = None,
                     description: Optional[str] = None, project: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create a folder (or project)

        Args:
            title: Name of the folder
            parent_id: ID of parent folder/space (creates in root if None)
            description: Optional description
            project: Dict defining project settings if this is a project (e.g. {"status": "Green", ...})
        """
        endpoint = f"folders/{parent_id}/folders" if parent_id else "folders"

        data = {
            "title": title,
            "description": description
        }

        if project:
            data["project"] = project

        response = self._request("POST", endpoint, data=data)
        return response['data'][0]

    def get_folders(self, folder_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get folders by IDs or all folders if no IDs specified

        Args:
            folder_ids: List of folder IDs to retrieve

        Returns:
            List of folder data
        """
        if folder_ids:
            # Get specific folders
            ids_param = ",".join(folder_ids)
            response = self._request("GET", f"folders/{ids_param}")
            return response.get('data', [])
        else:
            # Get all folders (may be paginated in real usage)
            response = self._request("GET", "folders")
            return response.get('data', [])

    # ==================== TASKS ====================

    def create_task(self, title: str, folder_id: str, description: Optional[str] = None,
                    assignees: Optional[List[str]] = None, status: str = "Active",
                    dates: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Create a task in a specific folder

        Args:
            title: Task title
            folder_id: ID of the folder/project to add task to
            description: Task description
            assignees: List of contact IDs to assign
            status: Task status (Active, Completed, Deferred, Cancelled)
            dates: Dict with 'start' and 'due' dates (YYYY-MM-DD)
        """
        endpoint = f"folders/{folder_id}/tasks"

        data = {
            "title": title,
            "description": description,
            "status": status
        }

        if assignees:
            data["responsibles"] = assignees

        if dates:
            data["dates"] = dates

        response = self._request("POST", endpoint, data=data)
        return response['data'][0]

    def get_tasks(self, folder_id: Optional[str] = None, task_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get tasks from a folder or by specific IDs

        Args:
            folder_id: Folder ID to get tasks from
            task_ids: List of specific task IDs to retrieve

        Returns:
            List of task data
        """
        if task_ids:
            # Get specific tasks
            ids_param = ",".join(task_ids)
            response = self._request("GET", f"tasks/{ids_param}")
            return response.get('data', [])
        elif folder_id:
            # Get tasks from folder
            response = self._request("GET", f"folders/{folder_id}/tasks")
            return response.get('data', [])
        else:
            # Get all tasks (may need pagination)
            response = self._request("GET", "tasks")
            return response.get('data', [])

    def update_task(self, task_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a task

        Args:
            task_id: Task ID to update
            updates: Fields to update

        Returns:
            Updated task data
        """
        response = self._request("PUT", f"tasks/{task_id}", data=updates)
        return response['data'][0]

    # ==================== UTILITY METHODS ====================

    def test_connection(self) -> bool:
        """
        Test connection to Wrike service

        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.get_user_info()
            return True
        except Exception as e:
            logger.error(f"Wrike connection test failed: {str(e)}")
            return False

    def get_workflow_steps(self) -> List[str]:
        """
        Get the standard workflow steps (can be customized based on your process)

        Returns:
            List of workflow step names
        """
        return [
            "Initial Contact",
            "Qualification",
            "Proposal",
            "Contract Signed",
            "Project Start",
            "Milestone Completion",
            "Project Close"
        ]
