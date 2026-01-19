"""
Falcon-HubSpot Sync Engine
Production-ready integration for syncing data between Falcon and HubSpot
Following the Manual Protocol for Falcon <-> HubSpot Communication
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
import logging

import requests
import yaml
from dateutil import parser as dtparser

# Import our clients
from wrike_client import WrikeClient
from hubspot_client import HubSpotClient

# --- Enhanced Logging ---
def setup_logging():
    """Configure comprehensive logging"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('logs/falcon_hubspot_sync.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# --- Sync Diagnostics System ---
class SyncDiagnostics:
    """Tracks sync issues and generates diagnostic reports with suggested fixes"""
    
    ISSUE_CATEGORIES = {
        "HUBSPOT_PROPERTY_MISSING": {
            "description": "HubSpot custom property does not exist",
            "suggested_fix": "Create the custom property in HubSpot Settings > Properties > Company Properties"
        },
        "HUBSPOT_COMPANY_NOT_FOUND": {
            "description": "HubSpot company record not found (404)",
            "suggested_fix": "The company may have been deleted from HubSpot. Remove stale mapping from local database."
        },
        "WRIKE_TASK_NOT_FOUND": {
            "description": "Wrike task/company not found",
            "suggested_fix": "The Wrike task may have been deleted. Remove stale mapping from local database."
        },
        "HUBSPOT_SEARCH_FAILED": {
            "description": "HubSpot search API returned 400 Bad Request",
            "suggested_fix": "Verify 'wrike_task_id' custom property exists in HubSpot with correct internal name."
        },
        "MISSING_REQUIRED_FIELD": {
            "description": "Required field is empty or missing",
            "suggested_fix": "Populate the required field in the source system before syncing."
        },
        "ID_MAPPING_MISSING": {
            "description": "No ID mapping exists between Wrike and HubSpot",
            "suggested_fix": "Set the Wrike Client ID in HubSpot or HubSpot Account ID in Wrike to establish link."
        },
        "STALE_DATABASE_RECORD": {
            "description": "Local database has reference to deleted external record",
            "suggested_fix": "Run database cleanup to remove orphaned mappings."
        },
        "API_RATE_LIMIT": {
            "description": "API rate limit exceeded",
            "suggested_fix": "Reduce sync frequency or implement request throttling."
        },
        "FIELD_VALUE_MISMATCH": {
            "description": "Field value format doesn't match expected type",
            "suggested_fix": "Check field type in source system matches destination (e.g., dropdown vs text)."
        }
    }
    
    def __init__(self):
        self.issues = []
        self.skipped_records = []
        self.field_sync_status = {}
        self.summary = {
            "total_records_processed": 0,
            "successful_syncs": 0,
            "failed_syncs": 0,
            "skipped_syncs": 0
        }
    
    def record_issue(self, category: str, record_id: str, record_name: str, 
                     field_name: str = None, details: str = None):
        """Record a sync issue"""
        issue = {
            "category": category,
            "record_id": record_id,
            "record_name": record_name,
            "field_name": field_name,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.issues.append(issue)
        
        # Track field-level issues
        if field_name:
            if field_name not in self.field_sync_status:
                self.field_sync_status[field_name] = {"success": 0, "failed": 0, "issues": []}
            self.field_sync_status[field_name]["failed"] += 1
            self.field_sync_status[field_name]["issues"].append(category)
    
    def record_success(self, field_name: str = None):
        """Record a successful sync"""
        self.summary["successful_syncs"] += 1
        if field_name:
            if field_name not in self.field_sync_status:
                self.field_sync_status[field_name] = {"success": 0, "failed": 0, "issues": []}
            self.field_sync_status[field_name]["success"] += 1
    
    def record_skip(self, record_id: str, record_name: str, reason: str):
        """Record a skipped record"""
        self.skipped_records.append({
            "record_id": record_id,
            "record_name": record_name,
            "reason": reason
        })
        self.summary["skipped_syncs"] += 1
    
    def increment_processed(self):
        """Increment processed count"""
        self.summary["total_records_processed"] += 1
    
    def increment_failed(self):
        """Increment failed count"""
        self.summary["failed_syncs"] += 1
    
    def generate_report(self) -> List[str]:
        """Generate diagnostic report lines for terminal display"""
        lines = []
        
        lines.append("")
        lines.append("═" * 70)
        lines.append("📊 SYNC DIAGNOSTIC REPORT")
        lines.append("═" * 70)
        lines.append("")
        
        # Summary
        lines.append("┌─ SUMMARY")
        lines.append(f"│  Total Records Processed: {self.summary['total_records_processed']}")
        lines.append(f"│  Successful Syncs: {self.summary['successful_syncs']}")
        lines.append(f"│  Failed Syncs: {self.summary['failed_syncs']}")
        lines.append(f"│  Skipped Records: {self.summary['skipped_syncs']}")
        lines.append("└─")
        lines.append("")
        
        # Issues by Category
        if self.issues:
            issue_counts = {}
            for issue in self.issues:
                cat = issue["category"]
                issue_counts[cat] = issue_counts.get(cat, 0) + 1
            
            lines.append("┌─ ISSUES BY CATEGORY")
            for cat, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
                cat_info = self.ISSUE_CATEGORIES.get(cat, {"description": cat})
                lines.append(f"│")
                lines.append(f"│  ⚠️  {cat} ({count} occurrences)")
                lines.append(f"│     Description: {cat_info.get('description', 'Unknown')}")
                lines.append(f"│     🔧 Suggested Fix: {cat_info.get('suggested_fix', 'No fix available')}")
            lines.append("└─")
            lines.append("")
        
        # Fields that failed to sync
        failed_fields = {k: v for k, v in self.field_sync_status.items() if v["failed"] > 0}
        if failed_fields:
            lines.append("┌─ FIELDS WITH SYNC FAILURES")
            for field, stats in sorted(failed_fields.items(), key=lambda x: -x[1]["failed"]):
                success_rate = (stats["success"] / (stats["success"] + stats["failed"])) * 100 if (stats["success"] + stats["failed"]) > 0 else 0
                lines.append(f"│")
                lines.append(f"│  📋 {field}")
                lines.append(f"│     Success: {stats['success']}, Failed: {stats['failed']} ({success_rate:.1f}% success rate)")
                unique_issues = list(set(stats["issues"]))
                for issue_type in unique_issues[:3]:  # Show top 3 issue types
                    lines.append(f"│     └─ Issue: {issue_type}")
            lines.append("└─")
            lines.append("")
        
        # Sample of skipped records
        if self.skipped_records:
            lines.append("┌─ SKIPPED RECORDS (sample)")
            for skip in self.skipped_records[:10]:  # Show first 10
                lines.append(f"│  • {skip['record_name'][:40]}...")
                lines.append(f"│    Reason: {skip['reason']}")
            if len(self.skipped_records) > 10:
                lines.append(f"│  ... and {len(self.skipped_records) - 10} more")
            lines.append("└─")
            lines.append("")
        
        # Recommendations
        lines.append("┌─ 🔧 RECOMMENDED ACTIONS")
        recommendations = self._generate_recommendations()
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                lines.append(f"│  {i}. {rec}")
        else:
            lines.append("│  ✅ No critical issues detected")
        lines.append("└─")
        lines.append("")
        lines.append("═" * 70)
        
        return lines
    
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on issues"""
        recommendations = []
        
        issue_counts = {}
        for issue in self.issues:
            cat = issue["category"]
            issue_counts[cat] = issue_counts.get(cat, 0) + 1
        
        # Priority-ordered recommendations
        if issue_counts.get("HUBSPOT_SEARCH_FAILED", 0) > 0:
            recommendations.append(
                "CREATE 'wrike_task_id' custom property in HubSpot: "
                "Settings > Properties > Company > Create Property (Text field)"
            )
        
        if issue_counts.get("HUBSPOT_COMPANY_NOT_FOUND", 0) > 5:
            recommendations.append(
                f"CLEAN UP local database: {issue_counts['HUBSPOT_COMPANY_NOT_FOUND']} records reference "
                "deleted HubSpot companies. Run: DELETE FROM company_id_map WHERE hubspot_company_id NOT IN (valid IDs)"
            )
        
        if issue_counts.get("ID_MAPPING_MISSING", 0) > 0:
            recommendations.append(
                "ESTABLISH ID LINKS: For companies to sync, set 'Wrike Client ID' in HubSpot "
                "or 'HubSpot Account ID' in Wrike to create the mapping"
            )
        
        if issue_counts.get("MISSING_REQUIRED_FIELD", 0) > 0:
            recommendations.append(
                "POPULATE REQUIRED FIELDS: Some records are missing required data. "
                "Check email, name, and ID fields in source systems"
            )
        
        if self.summary["skipped_syncs"] > self.summary["successful_syncs"]:
            recommendations.append(
                "HIGH SKIP RATE: More records are being skipped than synced. "
                "Review skip reasons above and ensure data is properly linked"
            )
        
        return recommendations


# Global diagnostics instance (reset each sync)
_sync_diagnostics = None

def get_diagnostics() -> SyncDiagnostics:
    """Get or create diagnostics instance"""
    global _sync_diagnostics
    if _sync_diagnostics is None:
        _sync_diagnostics = SyncDiagnostics()
    return _sync_diagnostics

def reset_diagnostics():
    """Reset diagnostics for new sync"""
    global _sync_diagnostics
    _sync_diagnostics = SyncDiagnostics()
    return _sync_diagnostics


# --- Database Models ---
class EnhancedDB:
    """Enhanced database with SQLAlchemy-like models and SQLite backend"""

    def __init__(self, path: str):
        self.path = path
        # Allow cross-thread access for streaming sync
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize all database tables"""
        cur = self.conn.cursor()

        # Company ID mapping table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS company_id_map (
                wrike_company_id TEXT PRIMARY KEY,
                hubspot_company_id TEXT UNIQUE,
                company_name TEXT,
                sync_status TEXT DEFAULT 'active',
                last_synced TEXT,
                updated_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            )
        """)

        # Sync state tracking
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_state (
                k TEXT PRIMARY KEY,
                v TEXT
            )
        """)

        # Sync logs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                source_system TEXT NOT NULL,
                target_system TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                status TEXT NOT NULL,
                message TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Reconciliation issues
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_issue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                entity_type TEXT,
                entity_id TEXT,
                issue_type TEXT,
                detail TEXT,
                resolved INTEGER DEFAULT 0
            )
        """)

        # Contact mappings
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contact_mappings (
                wrike_contact_id TEXT PRIMARY KEY,
                hubspot_contact_id TEXT UNIQUE,
                email TEXT,
                company_id TEXT,
                last_synced TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES company_id_map(wrike_company_id)
            )
        """)

        # Sync activities (one record per sync operation)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT DEFAULT 'running',
                companies_processed INTEGER DEFAULT 0,
                contacts_processed INTEGER DEFAULT 0,
                changes_made INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                summary TEXT
            )
        """)

        # Detailed sync changes (one record per field change)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_activity_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_id INTEGER NOT NULL,
                company_name TEXT,
                wrike_company_id TEXT,
                hubspot_company_id TEXT,
                entity_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                system_changed TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                changed INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (activity_id) REFERENCES sync_activities(id)
            )
        """)

        self.conn.commit()

    def log_sync_operation(self, operation: str, source: str, target: str,
                          entity_type: str, entity_id: str, status: str, message: str = ""):
        """Log sync operations"""
        self.conn.execute("""
            INSERT INTO sync_logs(operation, source_system, target_system,
                                 entity_type, entity_id, status, message)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        """, (operation, source, target, entity_type, entity_id, status, message))
        self.conn.commit()

    def start_activity(self, activity_type: str) -> int:
        """Start a new sync activity and return its ID"""
        cur = self.conn.execute("""
            INSERT INTO sync_activities(activity_type, started_at, status)
            VALUES(?, ?, 'running')
        """, (activity_type, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
        return cur.lastrowid

    def complete_activity(self, activity_id: int, companies: int = 0, contacts: int = 0,
                         changes: int = 0, errors: int = 0, summary: str = ""):
        """Mark activity as complete with summary stats"""
        self.conn.execute("""
            UPDATE sync_activities 
            SET completed_at = ?, status = 'completed',
                companies_processed = ?, contacts_processed = ?,
                changes_made = ?, errors = ?, summary = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), companies, contacts, 
              changes, errors, summary, activity_id))
        self.conn.commit()

    def fail_activity(self, activity_id: int, error_message: str):
        """Mark activity as failed"""
        self.conn.execute("""
            UPDATE sync_activities 
            SET completed_at = ?, status = 'failed', summary = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), error_message, activity_id))
        self.conn.commit()

    def record_change(self, activity_id: int, company_name: str, wrike_id: str, 
                     hubspot_id: str, entity_type: str, field_name: str, 
                     system_changed: str, old_value: str, new_value: str, changed: bool = True):
        """Record a single field change/comparison"""
        self.conn.execute("""
            INSERT INTO sync_activity_changes(
                activity_id, company_name, wrike_company_id, hubspot_company_id,
                entity_type, field_name, system_changed, old_value, new_value, changed
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (activity_id, company_name, wrike_id, hubspot_id, entity_type,
              field_name, system_changed, str(old_value) if old_value else "", 
              str(new_value) if new_value else "", 1 if changed else 0))
        self.conn.commit()

    def list_activities(self, limit: int = 50) -> List[Dict]:
        """List recent sync activities"""
        rows = self.conn.execute("""
            SELECT id, activity_type, started_at, completed_at, status,
                   companies_processed, contacts_processed, changes_made, errors, summary
            FROM sync_activities
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [{
            "id": r[0], "activity_type": r[1], "started_at": r[2],
            "completed_at": r[3], "status": r[4], "companies_processed": r[5],
            "contacts_processed": r[6], "changes_made": r[7], "errors": r[8],
            "summary": r[9]
        } for r in rows]

    def get_activity_changes(self, activity_id: int) -> List[Dict]:
        """Get all changes for a specific activity"""
        rows = self.conn.execute("""
            SELECT id, company_name, wrike_company_id, hubspot_company_id,
                   entity_type, field_name, system_changed, old_value, new_value, changed
            FROM sync_activity_changes
            WHERE activity_id = ?
            ORDER BY company_name, field_name
        """, (activity_id,)).fetchall()
        return [{
            "id": r[0], "company_name": r[1], "wrike_company_id": r[2],
            "hubspot_company_id": r[3], "entity_type": r[4], "field_name": r[5],
            "system_changed": r[6], "old_value": r[7], "new_value": r[8],
            "changed": bool(r[9])
        } for r in rows]

    def export_activity_report(self, activity_id: int, path: str = None) -> str:
        """Export activity changes to CSV"""
        if path is None:
            path = f"activity_report_{activity_id}.csv"
        
        changes = self.get_activity_changes(activity_id)
        activity = self.conn.execute(
            "SELECT activity_type, started_at FROM sync_activities WHERE id = ?",
            (activity_id,)
        ).fetchone()
        
        headers = ["Company Name", "Wrike Company ID", "HubSpot Company ID",
                   "Entity Type", "Field Name", "System Changed",
                   "Original Value", "New Value", "Changed (Y/N)"]
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if activity:
                writer.writerow([f"Activity Report: {activity[0]}"])
                writer.writerow([f"Started: {activity[1]}"])
                writer.writerow([])
            writer.writerow(headers)
            for c in changes:
                writer.writerow([
                    c["company_name"], c["wrike_company_id"], c["hubspot_company_id"],
                    c["entity_type"], c["field_name"], c["system_changed"],
                    c["old_value"], c["new_value"], "Y" if c["changed"] else "N"
                ])
        return path

    def get_state(self, k: str) -> Optional[str]:
        row = self.conn.execute("SELECT v FROM sync_state WHERE k = ?", (k,)).fetchone()
        return row[0] if row else None

    def set_state(self, k: str, v: str) -> None:
        self.conn.execute(
            "INSERT INTO sync_state(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (k, v),
        )
        self.conn.commit()

    def get_hubspot_company_id(self, wrike_company_id: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT hubspot_company_id FROM company_id_map WHERE wrike_company_id = ? AND sync_status = 'active'",
            (wrike_company_id,),
        ).fetchone()
        return row[0] if row else None

    def get_wrike_company_id_by_hubspot(self, hubspot_company_id: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT wrike_company_id FROM company_id_map WHERE hubspot_company_id = ? AND sync_status = 'active'",
            (hubspot_company_id,),
        ).fetchone()
        return row[0] if row else None

    def upsert_company_mapping(self, wrike_company_id: str, hubspot_company_id: str,
                              company_name: str = "", notes: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO company_id_map(wrike_company_id, hubspot_company_id, company_name,
                                      updated_at, last_synced, notes)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(wrike_company_id) DO UPDATE SET
                hubspot_company_id=excluded.hubspot_company_id,
                company_name=excluded.company_name,
                updated_at=excluded.updated_at,
                last_synced=excluded.last_synced,
                notes=excluded.notes,
                sync_status='active'
            """,
            (wrike_company_id, hubspot_company_id, company_name, now, now, notes),
        )
        self.conn.commit()

    def add_issue(self, source: str, entity_type: str, entity_id: str,
                 issue_type: str, detail: str) -> None:
        self.conn.execute(
            """
            INSERT INTO reconciliation_issue(created_at, source, entity_type,
                                           entity_id, issue_type, detail, resolved)
            VALUES(?, ?, ?, ?, ?, ?, 0)
            """,
            (datetime.now(timezone.utc).isoformat(), source, entity_type,
             entity_id, issue_type, detail),
        )
        self.conn.commit()
        logger.warning(f"Reconciliation issue: {issue_type} - {detail}")

    def list_unresolved_issues(self) -> List[Tuple]:
        rows = self.conn.execute(
            """
            SELECT id, created_at, source, entity_type, entity_id, issue_type, detail
            FROM reconciliation_issue
            WHERE resolved = 0
            ORDER BY id ASC
            """
        ).fetchall()
        return rows

    def get_company_mappings_report(self) -> Dict:
        """Generate company mappings report"""
        rows = self.conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN sync_status = 'active' THEN 1 ELSE 0 END) as active "
            "FROM company_id_map"
        ).fetchone()

        recent = self.conn.execute(
            "SELECT company_name, wrike_company_id, hubspot_company_id, last_synced "
            "FROM company_id_map "
            "WHERE sync_status = 'active' "
            "ORDER BY last_synced DESC LIMIT 10"
        ).fetchall()

        return {
            "total_mappings": rows[0] if rows else 0,
            "active_mappings": rows[1] if rows else 0,
            "recent_syncs": [
                {
                    "name": r[0],
                    "wrike_id": r[1],
                    "hubspot_id": r[2],
                    "last_synced": r[3]
                } for r in recent
            ]
        }

# --- Data Models ---
@dataclass
class Company:
    """Company data model"""
    name: str
    wrike_id: Optional[str] = None
    hubspot_id: Optional[str] = None
    account_status: Optional[str] = None
    affinity_score: Optional[float] = None
    account_tier: Optional[str] = None
    properties: Optional[Dict] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

@dataclass
class Contact:
    """Contact data model"""
    first_name: str
    last_name: str
    email: str
    wrike_id: Optional[str] = None
    hubspot_id: Optional[str] = None
    # NOTE: Title field does NOT exist in Wrike
    phone: Optional[str] = None
    mobile: Optional[str] = None
    address1: Optional[str] = None
    address2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    company_id: Optional[str] = None
    properties: Optional[Dict] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

# --- Configuration Management ---
@dataclass
class Config:
    """Enhanced configuration with validation"""
    raw: Dict[str, Any]

    @property
    def hubspot(self) -> Dict[str, Any]:
        return self.raw["hubspot"]

    @property
    def wrike(self) -> Dict[str, Any]:
        return self.raw["wrike"]

    @property
    def sync(self) -> Dict[str, Any]:
        return self.raw.get("sync", {})

    @property
    def environment(self) -> str:
        return self.raw.get("environment", "development")

    def validate(self) -> List[str]:
        """Validate configuration"""
        errors = []

        # Check required sections
        for section in ["hubspot", "wrike"]:
            if section not in self.raw:
                errors.append(f"Missing section: {section}")

        # Check required properties
        if "hubspot" in self.raw:
            hub = self.hubspot
            if "company_properties" not in hub:
                errors.append("Missing hubspot.company_properties")
            if "contact_properties" not in hub:
                errors.append("Missing hubspot.contact_properties")

        if "wrike" in self.raw:
            wrk = self.wrike
            for folder in ["companies_folder_id", "contacts_folder_id"]:
                if folder not in wrk:
                    errors.append(f"Missing wrike.{folder}")
            for cf in ["company_custom_fields", "contact_custom_fields"]:
                if cf not in wrk:
                    errors.append(f"Missing wrike.{cf}")

        return errors

def load_config(path: str) -> Config:
    """Load and validate configuration"""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config YAML must be a mapping at the top level.")

    config = Config(raw=raw)
    errors = config.validate()

    if errors:
        error_msg = "Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Configuration loaded from {path}")
    return config

# --- Enhanced HubSpot Client ---
class EnhancedHubSpotClient:
    """Enhanced HubSpot client with better error handling"""

    def __init__(self, token: str, base_url: str = "https://api.hubapi.com", use_hapikey: bool = False):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.use_hapikey = use_hapikey  # True for legacy API key auth
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        if not use_hapikey:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def _request_with_retry(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make request with exponential backoff retry and throttling"""
        max_retries = 10  # Increased for better reliability
        backoff_factor = 1.5
        
        # Throttle: HubSpot allows ~100 requests per 10 seconds = 10/second
        # Using 150ms delay gives us ~6.7 requests/second (safe margin under 10/sec)
        time.sleep(0.15)
        
        # Add legacy hapikey param if using that auth method
        if self.use_hapikey and self.token:
            params = kwargs.get("params", {})
            if params is None:
                params = {}
            params["hapikey"] = self.token
            kwargs["params"] = params

        for attempt in range(max_retries):
            try:
                # Extra delay on retries
                if attempt > 0:
                    time.sleep(0.2)
                
                response = self.session.request(method, f"{self.base_url}{path}",
                                              timeout=60, **kwargs)  # Increased timeout to 60s

                if response.status_code == 429:  # Rate limited
                    # Check for Retry-After header from HubSpot
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = min(backoff_factor ** attempt, 30)  # Cap at 30 seconds
                    logger.warning(f"Rate limited. Waiting {wait_time:.1f} seconds (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue

                if response.status_code >= 400:
                    logger.error(f"HubSpot error {response.status_code}: {response.text}")
                    if response.status_code >= 500 and attempt < max_retries - 1:
                        wait_time = min(backoff_factor ** attempt, 15)
                        time.sleep(wait_time)
                        continue
                    response.raise_for_status()

                return response

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
                wait_time = min(backoff_factor ** attempt, 15)
                time.sleep(wait_time)

        raise Exception("Max retries exceeded")

    # Core HubSpot API methods
    def list_properties(self, object_type: str) -> List[Dict[str, Any]]:
        response = self._request_with_retry("GET", f"/crm/v3/properties/{object_type}")
        data = response.json()
        return data.get("results", [])

    def search_objects(self, object_type: str, filter_groups: List[Dict[str, Any]],
                      properties: List[str], sorts: List[str],
                      after: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        body = {
            "filterGroups": filter_groups,
            "properties": properties,
            "sorts": sorts,
            "limit": limit,
        }
        if after is not None:
            body["after"] = after

        response = self._request_with_retry("POST", f"/crm/v3/objects/{object_type}/search",
                                          json=body)
        return response.json()

    def get_object(self, object_type: str, object_id: str, properties: List[str]) -> Dict[str, Any]:
        params = {"properties": properties}
        response = self._request_with_retry("GET",
                                          f"/crm/v3/objects/{object_type}/{object_id}",
                                          params=params)
        return response.json()

    def create_object(self, object_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        response = self._request_with_retry("POST", f"/crm/v3/objects/{object_type}",
                                          json={"properties": properties})
        return response.json()

    def update_object(self, object_type: str, object_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        response = self._request_with_retry("PATCH",
                                          f"/crm/v3/objects/{object_type}/{object_id}",
                                          json={"properties": properties})
        return response.json()

    def find_contact_by_email(self, email: str, properties: List[str]) -> Optional[Dict[str, Any]]:
        email = email.strip()
        if not email:
            return None

        filters = [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}]
        res = self.search_objects("contacts", filters, properties=properties,
                                sorts=["hs_lastmodifieddate"], limit=2)
        results = res.get("results", [])
        return results[0] if results else None

    def associate_contact_to_company(self, contact_id: str, company_id: str) -> None:
        """Create contact-company association"""
        path = f"/crm/v4/objects/contact/{contact_id}/associations/default/company/{company_id}"
        self._request_with_retry("PUT", path)

# --- Enhanced Wrike Client ---
class EnhancedWrikeClient:
    """Enhanced Wrike client with better error handling"""

    def __init__(self, token: str, base_url: str = "https://www.wrike.com/api/v4"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

    def _request_with_retry(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make request with exponential backoff retry and throttling"""
        max_retries = 10  # Increased for better reliability
        backoff_factor = 1.5
        
        # Throttle: Wrike allows ~100 requests per minute = 1.67/second
        # Using 1000ms (1 second) delay gives us 1 request/second (safe margin)
        time.sleep(1.0)

        for attempt in range(max_retries):
            try:
                # Extra delay on retries
                if attempt > 0:
                    time.sleep(0.2)
                
                response = self.session.request(method, f"{self.base_url}{path}",
                                              timeout=60, **kwargs)  # Increased timeout to 60s

                if response.status_code == 429:  # Rate limited
                    # Check for Retry-After header
                    retry_after = response.headers.get('Retry-After') or response.headers.get('X-Rate-Limit-Reset')
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = min(backoff_factor ** attempt, 30)  # Cap at 30 seconds
                    logger.warning(f"Wrike rate limited. Waiting {wait_time:.1f} seconds (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue

                if response.status_code >= 400:
                    logger.error(f"Wrike error {response.status_code}: {response.text}")
                    if response.status_code >= 500 and attempt < max_retries - 1:
                        wait_time = min(backoff_factor ** attempt, 15)
                        time.sleep(wait_time)
                        continue
                    response.raise_for_status()

                return response

            except requests.exceptions.RequestException as e:
                logger.error(f"Wrike request failed (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
                wait_time = min(backoff_factor ** attempt, 15)
                time.sleep(wait_time)

        raise Exception("Max retries exceeded")

    def get_folder_tasks(self, folder_id: str, page_size: int = 100) -> List[Dict[str, Any]]:
        """Get tasks from a specific folder"""
        params = {"pageSize": str(page_size)}
        response = self._request_with_retry("GET", f"/folders/{folder_id}/tasks", params=params)
        return response.json().get("data", [])

    # Core Wrike API methods
    def list_custom_fields(self) -> List[Dict[str, Any]]:
        response = self._request_with_retry("GET", "/customfields")
        data = response.json()
        return data.get("data", [])

    def query_tasks_in_folder_updated_between(self, folder_id: str,
                                            start_dt: datetime, end_dt: datetime,
                                            descendants: bool = True, page_size: int = 100) -> Iterable[Dict[str, Any]]:
        next_token = None
        # Format dates in Wrike's expected format: YYYY-MM-DDTHH:MM:SSZ (no microseconds, Z suffix)
        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        updated_date = json.dumps({"start": start_str, "end": end_str})

        while True:
            params = {
                "descendants": "true" if descendants else "false",
                "pageSize": str(page_size),
                "updatedDate": updated_date,
            }
            if next_token:
                params["nextPageToken"] = next_token

            response = self._request_with_retry("GET", f"/folders/{folder_id}/tasks",
                                              params=params)
            data = response.json()

            for task in data.get("data", []):
                yield task

            next_token = data.get("nextPageToken")
            if not next_token:
                break

    def query_tasks_in_folder_by_custom_field(self, folder_id: str, custom_field_id: str,
                                            value: str, descendants: bool = True,
                                            page_size: int = 100) -> List[Dict[str, Any]]:
        cf = json.dumps({"id": custom_field_id, "value": value})
        params = {
            "descendants": "true" if descendants else "false",
            "pageSize": str(page_size),
            "customField": cf,
        }
        response = self._request_with_retry("GET", f"/folders/{folder_id}/tasks",
                                          params=params)
        data = response.json()
        return data.get("data", [])

    def create_task(self, folder_id: str, title: str, custom_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        data = {
            "title": title,
            "customFields": json.dumps(custom_fields),
        }
        response = self._request_with_retry("POST", f"/folders/{folder_id}/tasks", data=data)
        items = response.json().get("data", [])
        if not items:
            raise Exception("Wrike create task returned no data.")
        return items[0]

    def update_task(self, task_id: str, title: Optional[str] = None,
                   custom_fields: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Update a Wrike task. Title is optional - if not provided, task title won't be changed."""
        data = {}
        if title is not None:
            data["title"] = title
        if custom_fields is not None:
            data["customFields"] = json.dumps(custom_fields)

        response = self._request_with_retry("PUT", f"/tasks/{task_id}", data=data)
        items = response.json().get("data", [])
        if not items:
            raise Exception("Wrike update task returned no data.")
        return items[0]

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Get specific task by ID"""
        response = self._request_with_retry("GET", f"/tasks/{task_id}")
        items = response.json().get("data", [])
        if not items:
            raise Exception(f"Task {task_id} not found")
        return items[0]

# --- Utility Functions ---
def wrike_cf_get(task: Dict[str, Any], cf_id: str) -> Optional[str]:
    """Get custom field value from Wrike task"""
    for cf in task.get("customFields", []) or []:
        if cf.get("id") == cf_id:
            v = cf.get("value")
            if v is None:
                return None
            return str(v)
    return None

def wrike_cf_set(custom_fields: List[Dict[str, Any]], cf_id: str,
                value: Optional[str]) -> List[Dict[str, Any]]:
    """Set custom field value for Wrike task"""
    value = None if value is None else str(value)
    out = [dict(x) for x in (custom_fields or []) if x.get("id") != cf_id]
    if value is not None and value.strip() != "":
        out.append({"id": cf_id, "value": value})
    return out

def safe_str(x: Any) -> str:
    """Safely convert to string"""
    if x is None:
        return ""
    return str(x).strip()

def clean_company_name_for_hubspot(name: str) -> str:
    """
    IMPORTANT: Remove 'AdminCard' prefix from company names before sending to HubSpot.
    AdminCard_ is a Wrike convention and should NEVER appear in HubSpot.
    """
    if not name:
        return ""
    # Handle different AdminCard formats
    if name.startswith('AdminCard_'):
        name = name[len('AdminCard_'):]
    elif name.startswith('AdminCard '):
        name = name[len('AdminCard '):]
    elif name.startswith('AdminCard-'):
        name = name[len('AdminCard-'):]
    return name.strip()

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

# --- Enhanced Sync Engine ---
class EnhancedSyncEngine:
    """Enhanced sync engine with all integrated features"""

    def __init__(self, cfg: Config, db: EnhancedDB, hub: EnhancedHubSpotClient,
                 wrk: EnhancedWrikeClient):
        self.cfg = cfg
        self.db = db
        self.hub = hub
        self.wrk = wrk
        self.hp_company_props = cfg.hubspot["company_properties"]
        self.hp_contact_props = cfg.hubspot["contact_properties"]
        self.w_company_cf = cfg.wrike["company_custom_fields"]
        self.w_contact_cf = cfg.wrike["contact_custom_fields"]
        self.sync_opts = cfg.sync
        self.tier_to_priority = self.sync_opts.get("tier_to_priority", {})
        self.priority_to_tier = self.sync_opts.get("priority_to_tier", {})

        # Environment-specific settings
        self.is_production = cfg.environment == "production"

    def verify(self) -> Dict[str, Any]:
        """Verify all configurations and return report"""
        logger.info("Verifying system configuration...")

        report = {
            "timestamp": utc_now().isoformat(),
            "hubspot": {"status": "pending", "missing_properties": []},
            "wrike": {"status": "pending", "missing_fields": []},
            "mappings": {"status": "pending"},
            "environment": self.cfg.environment,
            "notes": []
        }

        try:
            # Verify HubSpot properties
            logger.info("Verifying HubSpot properties...")
            company_props = {p.get("name") for p in self.hub.list_properties("companies")}
            contact_props = {p.get("name") for p in self.hub.list_properties("contacts")}

            required_company = set(self.hp_company_props.values())
            required_contact = set(self.hp_contact_props.values())

            missing_company = [p for p in required_company if p not in company_props]
            missing_contact = [p for p in required_contact if p not in contact_props]

            if missing_company or missing_contact:
                report["hubspot"]["status"] = "failed"
                report["hubspot"]["missing_properties"] = missing_company + missing_contact
                report["notes"].append("Missing HubSpot properties")
            else:
                report["hubspot"]["status"] = "passed"

            # Verify Wrike custom fields
            logger.info("Verifying Wrike custom fields...")
            wrike_fields = {f.get("id") for f in self.wrk.list_custom_fields()}
            required_wrike = set(self.w_company_cf.values()) | set(self.w_contact_cf.values())
            missing_wrike = [cf for cf in required_wrike if cf not in wrike_fields]

            if missing_wrike:
                report["wrike"]["status"] = "failed"
                report["wrike"]["missing_fields"] = missing_wrike
                report["notes"].append("Missing Wrike custom fields")
            else:
                report["wrike"]["status"] = "passed"

            # Verify field mappings
            logger.info("Verifying field mappings...")
            mapping_errors = []

            # Check tier mappings
            for tier in ["Tier 1", "Tier 2", "Tier 3"]:
                if tier not in self.tier_to_priority:
                    mapping_errors.append(f"Missing tier mapping: {tier}")

            if mapping_errors:
                report["mappings"]["status"] = "failed"
                report["mappings"]["errors"] = mapping_errors
                report["notes"].append("Missing tier/priority mappings")
            else:
                report["mappings"]["status"] = "passed"

            # Overall status
            all_passed = all([
                report["hubspot"]["status"] == "passed",
                report["wrike"]["status"] == "passed",
                report["mappings"]["status"] == "passed"
            ])

            report["overall_status"] = "PASSED" if all_passed else "FAILED"

            if all_passed:
                logger.info("Verification PASSED")
            else:
                logger.error("Verification FAILED")
                logger.error(f"Report: {json.dumps(report, indent=2)}")

            return report

        except Exception as e:
            logger.error(f"Verification failed with error: {e}")
            report["overall_status"] = "ERROR"
            report["error"] = str(e)
            return report

    def generate_mapping_report(self) -> Dict[str, Any]:
        """Generate comprehensive mapping report"""
        report = {
            "timestamp": utc_now().isoformat(),
            "company_field_mappings": self.hp_company_props,
            "contact_field_mappings": self.hp_contact_props,
            "wrike_company_fields": self.w_company_cf,
            "wrike_contact_fields": self.w_contact_cf,
            "tier_mapping": self.tier_to_priority,
            "priority_mapping": self.priority_to_tier,
            "database_stats": self.db.get_company_mappings_report(),
            "sync_settings": {
                "polling_interval": self.sync_opts.get("polling_interval_seconds"),
                "environment": self.cfg.environment
            }
        }
        return report

    def test_sync(self) -> Dict[str, Any]:
        """Perform a limited test sync to verify connectivity and mappings"""
        logger.info("Starting test sync...")
        results = {
            "status": "success",
            "falcon_connectivity": "pending",
            "hubspot_connectivity": "pending",
            "sample_data": {
                "falcon_company": None,
                "hubspot_company": None
            },
            "errors": []
        }

        # 0. Validate config early for clearer diagnostics
        try:
            cfg_errors = self.cfg.validate()
            if cfg_errors:
                results["status"] = "warning"
                results["errors"].append("Configuration issues found: " + "; ".join(cfg_errors))
        except Exception as e:
            results["status"] = "warning"
            results["errors"].append(f"Configuration validation error: {str(e)}")

        # 1. Test Falcon Connectivity & Sample Data
        try:
            companies_folder = self.cfg.wrike["companies_folder_id"]
            # Fetch just 1 task from companies folder
            if hasattr(self.wrk, "get_folder_tasks"):
                tasks = self.wrk.get_folder_tasks(companies_folder, page_size=1)
            else:
                # Fallback for older client implementations
                start = utc_now() - timedelta(days=7)
                end = utc_now()
                tasks = list(self.wrk.query_tasks_in_folder_updated_between(
                    companies_folder, start, end, descendants=True, page_size=1
                ))
            if tasks:
                results["sample_data"]["falcon_company"] = tasks[0].get("title")
                results["falcon_connectivity"] = "connected"
            else:
                results["falcon_connectivity"] = "connected (but folder empty)"
        except Exception as e:
            results["falcon_connectivity"] = "failed"
            message = str(e)
            if "400" in message and "folders" in message:
                message = ("Falcon error: Invalid Falcon folder ID. "
                           "Check config.yaml -> wrike.companies_folder_id.")
            else:
                message = f"Falcon error: {message}"
            results["errors"].append(message)
            results["status"] = "partial_failure"

        # 2. Test HubSpot Connectivity & Sample Data
        try:
            # Search for any company in HubSpot
            filters = [] # No filters = get any
            search_res = self.hub.search_objects("companies", filters, properties=["name"], sorts=[], limit=1)
            if search_res.get("results"):
                results["sample_data"]["hubspot_company"] = search_res["results"][0]["properties"].get("name")
                results["hubspot_connectivity"] = "connected"
            else:
                results["hubspot_connectivity"] = "connected (but CRM empty)"
        except Exception as e:
            results["hubspot_connectivity"] = "failed"
            message = str(e)
            if "401" in message or "Unauthorized" in message:
                message = ("HubSpot error: Unauthorized. "
                           "Check HubSpot API key in config.json/config.yaml.")
            else:
                message = f"HubSpot error: {message}"
            results["errors"].append(message)
            results["status"] = "failed" if results["status"] == "partial_failure" else "partial_failure"

        return results

    def sync_once(self) -> Dict[str, Any]:
        """Run one complete sync cycle"""
        logger.info("Starting sync cycle...")
        
        # Reset diagnostics for this sync
        diagnostics = reset_diagnostics()

        results = {
            "start_time": utc_now().isoformat(),
            "companies_to_hubspot": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "contacts_to_hubspot": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
            "companies_to_wrike": {"processed": 0, "updated": 0, "failed": 0},
            "contacts_to_wrike": {"processed": 0, "updated": 0, "failed": 0},
            "issues_found": 0,
            "end_time": None,
            "duration_seconds": None
        }

        start_time = utc_now()
        
        # Start activity tracking
        activity_id = self.db.start_activity("full_sync")
        results["activity_id"] = activity_id
        total_changes = 0
        total_companies = 0
        total_contacts = 0
        total_errors = 0

        try:
            # 1. Wrike -> HubSpot (Companies)
            logger.info("Syncing companies from Wrike to HubSpot...")
            company_results = self.sync_wrike_to_hubspot_companies(activity_id)
            results["companies_to_hubspot"].update(company_results)
            total_companies += company_results.get("processed", 0)
            total_changes += company_results.get("updated", 0) + company_results.get("created", 0)

            # 2. Wrike -> HubSpot (Contacts)
            logger.info("Syncing contacts from Wrike to HubSpot...")
            contact_results = self.sync_wrike_to_hubspot_contacts(activity_id)
            results["contacts_to_hubspot"].update(contact_results)
            total_contacts += contact_results.get("processed", 0)
            total_changes += contact_results.get("updated", 0) + contact_results.get("created", 0)

            # 3. HubSpot -> Wrike (Companies)
            logger.info("Syncing companies from HubSpot to Wrike...")
            hubspot_company_results = self.sync_hubspot_to_wrike_companies(activity_id)
            results["companies_to_wrike"].update(hubspot_company_results)
            total_changes += hubspot_company_results.get("updated", 0)

            # 4. HubSpot -> Wrike (Contacts)
            if self.sync_opts.get("sync_contacts_hubspot_to_wrike", False):
                logger.info("Syncing contacts from HubSpot to Wrike...")
                hubspot_contact_results = self.sync_hubspot_to_wrike_contacts(activity_id)
                results["contacts_to_wrike"].update(hubspot_contact_results)
                total_changes += hubspot_contact_results.get("updated", 0)

            # 5. Sync HubSpot Company Names to Wrike (auto-update "Hubspot Account Name" field)
            logger.info("Syncing HubSpot company names to Wrike...")
            name_sync_results = self.sync_hubspot_company_names_to_wrike(activity_id)
            results["company_names_synced"] = name_sync_results
            total_changes += name_sync_results.get("updated", 0)

            # 6. Sync HubSpot/Wrike IDs bidirectionally
            logger.info("Syncing HubSpot Account IDs and Wrike Client IDs...")
            id_sync_results = self.sync_company_ids_bidirectional(activity_id)
            results["company_ids_synced"] = id_sync_results
            total_changes += id_sync_results.get("wrike_ids_updated", 0) + id_sync_results.get("hubspot_ids_updated", 0)

            # Count issues
            issues = self.db.list_unresolved_issues()
            results["issues_found"] = len(issues)

            # Calculate duration
            end_time = utc_now()
            results["end_time"] = end_time.isoformat()
            results["duration_seconds"] = (end_time - start_time).total_seconds()

            logger.info(f"Sync completed in {results['duration_seconds']:.2f} seconds")
            
            # Complete activity tracking
            self.db.complete_activity(
                activity_id, 
                companies=total_companies, 
                contacts=total_contacts,
                changes=total_changes, 
                errors=total_errors,
                summary=f"Companies: {total_companies}, Contacts: {total_contacts}, Changes: {total_changes}"
            )
            
            # Log sync summary
            self.db.log_sync_operation(
                operation="full_sync",
                source="middleware",
                target="bidirectional",
                entity_type="all",
                entity_id=None,
                status="success",
                message=f"Processed {results['companies_to_hubspot']['processed']} companies, "
                       f"{results['contacts_to_hubspot']['processed']} contacts"
            )

            # Generate and log diagnostic report
            diagnostics = get_diagnostics()
            diagnostics.summary["total_records_processed"] = total_companies + total_contacts
            diagnostics.summary["successful_syncs"] = total_changes
            diagnostics.summary["failed_syncs"] = total_errors
            
            report_lines = diagnostics.generate_report()
            for line in report_lines:
                if "⚠️" in line or "Issue:" in line:
                    logger.warning(line)
                elif "🔧" in line or "Suggested Fix:" in line:
                    logger.info(line)
                elif "═" in line or "┌" in line or "└" in line or "│" in line:
                    logger.info(line)
                else:
                    logger.info(line)
            
            # Store diagnostic report in results
            results["diagnostic_report"] = report_lines

            return results

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            self.db.fail_activity(activity_id, str(e))
            self.db.log_sync_operation(
                operation="full_sync",
                source="middleware",
                target="bidirectional",
                entity_type="all",
                entity_id=None,
                status="failed",
                message=str(e)
            )
            raise

    def sync_wrike_to_hubspot_companies(self, activity_id: int = None) -> Dict[str, int]:
        """Sync companies from Wrike to HubSpot"""
        companies_folder = self.cfg.wrike["companies_folder_id"]
        state_key = "wrike_to_hubspot_last_run"
        last = self.db.get_state(state_key)
        start = dtparser.parse(last) if last else (utc_now() - timedelta(days=7))
        end = utc_now()

        results = {"processed": 0, "created": 0, "updated": 0, "failed": 0, "sync_details": []}

        for task in self.wrk.query_tasks_in_folder_updated_between(
            companies_folder, start, end, descendants=True
        ):
            try:
                wrike_task_id = safe_str(task.get("id"))
                wrike_task_title = safe_str(task.get("title", ""))
                
                # Skip non-AdminCard tasks - only AdminCard tasks represent companies
                if "AdminCard" not in wrike_task_title:
                    continue
                
                results["processed"] += 1

                hubspot_company_id = self.db.get_hubspot_company_id(wrike_task_id)

                # Extract company name from AdminCard title (e.g., "AdminCard_100X Bio" -> "100X Bio")
                # Use helper function to ensure AdminCard is NEVER in HubSpot
                company_name = clean_company_name_for_hubspot(wrike_task_title)
                if not company_name:
                    continue

                # Get custom field values from Wrike
                account_status = wrike_cf_get(task, self.w_company_cf["account_status"])
                affinity_score = wrike_cf_get(task, self.w_company_cf["affinity_score"])
                account_tier = wrike_cf_get(task, self.w_company_cf["account_tier"])

                # Map Tier to Priority
                account_priority = self.tier_to_priority.get(account_tier, account_tier)

                # Log source data
                logger.info(f"┌─ SYNC: {company_name}")
                logger.info(f"│  Wrike Task ID: {wrike_task_id}")
                logger.info(f"│  SOURCE (Falcon):")
                logger.info(f"│    Account Status: {account_status or '(empty)'}")
                logger.info(f"│    Affinity Score: {affinity_score or '(empty)'}")
                logger.info(f"│    Tier: {account_tier or '(empty)'} → Priority: {account_priority or '(empty)'}")

                # Prepare HubSpot properties - ALWAYS include wrike_task_id
                wrike_task_id_prop = self.hp_company_props.get("wrike_task_id", "wrike_task_id")
                company_props = {
                    self.hp_company_props["name"]: company_name,
                    self.hp_company_props["account_status"]: account_status,
                    self.hp_company_props["affinity_score"]: affinity_score,
                    self.hp_company_props["account_priority"]: account_priority,
                    wrike_task_id_prop: wrike_task_id,  # Always set Wrike Client ID
                }
                logger.info(f"│    Wrike Client ID: {wrike_task_id} → HubSpot")

                sync_detail = {
                    "company": company_name,
                    "wrike_id": wrike_task_id,
                    "source_values": {
                        "account_status": account_status,
                        "affinity_score": affinity_score,
                        "tier": account_tier,
                        "priority": account_priority
                    }
                }

                if hubspot_company_id:
                    # Get current HubSpot values for comparison
                    try:
                        current = self.hub.get_object("companies", hubspot_company_id, 
                            list(self.hp_company_props.values()))
                        current_props = current.get("properties", {})
                        
                        logger.info(f"│  TARGET (HubSpot) - BEFORE:")
                        logger.info(f"│    Account Status: {current_props.get(self.hp_company_props['account_status']) or '(empty)'}")
                        logger.info(f"│    Affinity Score: {current_props.get(self.hp_company_props['affinity_score']) or '(empty)'}")
                        logger.info(f"│    Priority: {current_props.get(self.hp_company_props['account_priority']) or '(empty)'}")
                        
                        sync_detail["hubspot_id"] = hubspot_company_id
                        sync_detail["before_values"] = {
                            "account_status": current_props.get(self.hp_company_props['account_status']),
                            "affinity_score": current_props.get(self.hp_company_props['affinity_score']),
                            "priority": current_props.get(self.hp_company_props['account_priority'])
                        }
                    except:
                        pass
                    
                    # Update existing HubSpot company
                    self.hub.update_object("companies", hubspot_company_id, company_props)
                    results["updated"] += 1
                    sync_detail["action"] = "updated"
                    logger.info(f"│  ✓ UPDATED HubSpot ID: {hubspot_company_id}")
                else:
                    # No mapping exists - search HubSpot by Wrike Client ID (NOT by name)
                    wrike_task_id_prop = self.hp_company_props.get("wrike_task_id", "wrike_task_id")
                    filters = [{"filters": [{"propertyName": wrike_task_id_prop, "operator": "EQ", "value": wrike_task_id}]}]
                    
                    try:
                        search_res = self.hub.search_objects("companies", filters, 
                            properties=list(self.hp_company_props.values()), sorts=[], limit=1)
                    except Exception as search_err:
                        # Search failed - property may not exist in HubSpot
                        if "400" in str(search_err):
                            logger.warning(f"│  ⚠ Search failed for {company_name}: wrike_task_id property may not exist in HubSpot")
                            results["skipped"] = results.get("skipped", 0) + 1
                            sync_detail["action"] = "skipped_search_failed"
                            # Record diagnostic
                            diag = get_diagnostics()
                            diag.record_issue("HUBSPOT_SEARCH_FAILED", wrike_task_id, company_name, 
                                "wrike_task_id", "HubSpot search API returned 400 - property may not exist")
                            diag.record_skip(wrike_task_id, company_name, "HubSpot search failed (400)")
                            continue
                        raise
                    
                    if search_res.get("results"):
                        # Found by Wrike Client ID - this is the correct match
                        hubspot_company_id = search_res["results"][0]["id"]
                        current_props = search_res["results"][0].get("properties", {})
                        
                        logger.info(f"│  Found HubSpot company by Wrike ID (ID: {hubspot_company_id})")
                        logger.info(f"│  TARGET (HubSpot) - BEFORE:")
                        logger.info(f"│    Account Status: {current_props.get(self.hp_company_props['account_status']) or '(empty)'}")
                        logger.info(f"│    Affinity Score: {current_props.get(self.hp_company_props['affinity_score']) or '(empty)'}")
                        logger.info(f"│    Priority: {current_props.get(self.hp_company_props['account_priority']) or '(empty)'}")
                        
                        sync_detail["hubspot_id"] = hubspot_company_id
                        sync_detail["before_values"] = {
                            "account_status": current_props.get(self.hp_company_props['account_status']),
                            "affinity_score": current_props.get(self.hp_company_props['affinity_score']),
                            "priority": current_props.get(self.hp_company_props['account_priority'])
                        }
                        
                        self.hub.update_object("companies", hubspot_company_id, company_props)
                        results["updated"] += 1
                        sync_detail["action"] = "matched_by_wrike_id"
                        logger.info(f"│  ✓ MATCHED BY WRIKE ID & UPDATED")
                    else:
                        # No match found - CREATE the company in HubSpot with wrike_task_id set
                        logger.info(f"│  Creating new HubSpot company for: {company_name}")
                        logger.info(f"│    Setting Wrike Client ID: {wrike_task_id}")
                        
                        new_company = self.hub.create_object("companies", company_props)
                        hubspot_company_id = new_company.get("id")
                        
                        results["created"] += 1
                        sync_detail["action"] = "created"
                        sync_detail["hubspot_id"] = hubspot_company_id
                        logger.info(f"│  ✓ CREATED HubSpot company ID: {hubspot_company_id}")
                    
                    if hubspot_company_id:
                        self.db.upsert_company_mapping(wrike_task_id, hubspot_company_id, company_name)

                logger.info(f"└─ COMPLETE: {company_name}")
                results["sync_details"].append(sync_detail)
                
                # Record changes to activity log if tracking
                if activity_id and hubspot_company_id:
                    before = sync_detail.get("before_values", {})
                    for field_name, new_val in [
                        ("Account Status", account_status),
                        ("Affinity Score", affinity_score),
                        ("Priority", account_priority)
                    ]:
                        old_val = before.get(field_name.lower().replace(" ", "_"), "")
                        changed = str(old_val or "") != str(new_val or "")
                        self.db.record_change(activity_id, company_name, wrike_task_id, hubspot_company_id,
                            "company", field_name, "HubSpot", old_val or "", new_val or "", changed)

            except Exception as e:
                logger.error(f"Failed to sync Wrike company {wrike_task_id}: {e}")
                results["failed"] += 1
                self.db.add_issue("wrike", "company", wrike_task_id, "sync_error", str(e))
                # Record diagnostic
                diag = get_diagnostics()
                error_msg = str(e)
                if "404" in error_msg:
                    diag.record_issue("HUBSPOT_COMPANY_NOT_FOUND", wrike_task_id, company_name, 
                        None, f"HubSpot company not found: {error_msg}")
                else:
                    diag.record_issue("FIELD_VALUE_MISMATCH", wrike_task_id, company_name, 
                        None, f"Sync error: {error_msg}")
                diag.increment_failed()

        # Update sync state
        self.db.set_state(state_key, end.isoformat())
        return results

    def sync_wrike_to_hubspot_contacts(self, activity_id: int = None) -> Dict[str, int]:
        """Sync contacts from Wrike to HubSpot"""
        contacts_folder = self.cfg.wrike["contacts_folder_id"]
        state_key = "wrike_to_hubspot_contacts_last_run"
        last = self.db.get_state(state_key)
        start = dtparser.parse(last) if last else (utc_now() - timedelta(days=7))
        end = utc_now()

        results = {"processed": 0, "created": 0, "updated": 0, "failed": 0, "sync_details": []}

        for task in self.wrk.query_tasks_in_folder_updated_between(
            contacts_folder, start, end, descendants=True
        ):
            try:
                wrike_task_id = safe_str(task.get("id"))
                results["processed"] += 1

                # Extract data using custom field mappings
                email = wrike_cf_get(task, self.w_contact_cf["email"])
                if not email:
                    logger.warning(f"Contact task {wrike_task_id} missing email, skipping")
                    continue

                firstname = wrike_cf_get(task, self.w_contact_cf["first_name"])
                lastname = wrike_cf_get(task, self.w_contact_cf["last_name"])
                # NOTE: Title field does NOT exist in Wrike - do not sync
                phone = wrike_cf_get(task, self.w_contact_cf["phone"])
                mobile = wrike_cf_get(task, self.w_contact_cf["mobile"])
                address1 = wrike_cf_get(task, self.w_contact_cf["address1"])
                address2 = wrike_cf_get(task, self.w_contact_cf["address2"])
                city = wrike_cf_get(task, self.w_contact_cf["city"])
                state = wrike_cf_get(task, self.w_contact_cf["state"])
                country = wrike_cf_get(task, self.w_contact_cf["country"])
                
                # Fallback: If firstname/lastname fields are empty, try to parse from Wrike task title
                if not firstname and not lastname:
                    title_parts = task.get("title", "").split(" ", 1)
                    firstname = title_parts[0] if len(title_parts) > 0 else ""
                    lastname = title_parts[1] if len(title_parts) > 1 else ""

                # Log source data
                logger.info(f"┌─ SYNC CONTACT: {firstname} {lastname}")
                logger.info(f"│  Wrike Task ID: {wrike_task_id}")
                logger.info(f"│  SOURCE (Falcon):")
                logger.info(f"│    Email: {email}")
                logger.info(f"│    Name: {firstname or '(empty)'} {lastname or '(empty)'}")
                logger.info(f"│    Phone: {phone or '(empty)'} | Mobile: {mobile or '(empty)'}")
                logger.info(f"│    Address: {address1 or '(empty)'}, {city or '(empty)'}, {state or '(empty)'}")

                # NOTE: Title/jobtitle is NOT synced - field does not exist in Wrike
                contact_props = {
                    self.hp_contact_props["firstname"]: firstname,
                    self.hp_contact_props["lastname"]: lastname,
                    self.hp_contact_props["email"]: email,
                    self.hp_contact_props["phone"]: phone,
                    self.hp_contact_props["mobilephone"]: mobile,
                    self.hp_contact_props["address"]: address1,
                    self.hp_contact_props["address2"]: address2,
                    self.hp_contact_props["city"]: city,
                    self.hp_contact_props["state"]: state,
                    self.hp_contact_props["country"]: country,
                }

                sync_detail = {
                    "contact": f"{firstname} {lastname}",
                    "email": email,
                    "wrike_id": wrike_task_id,
                    "source_values": contact_props.copy()
                }

                # Find HubSpot contact by email
                hub_contact = self.hub.find_contact_by_email(email, 
                    properties=list(self.hp_contact_props.values()))
                
                if hub_contact:
                    hub_id = hub_contact["id"]
                    current_props = hub_contact.get("properties", {})
                    
                    logger.info(f"│  TARGET (HubSpot) - BEFORE (ID: {hub_id}):")
                    logger.info(f"│    Name: {current_props.get('firstname', '(empty)')} {current_props.get('lastname', '(empty)')}")
                    logger.info(f"│    Phone: {current_props.get('phone', '(empty)')} | Mobile: {current_props.get('mobilephone', '(empty)')}")
                    
                    sync_detail["hubspot_id"] = hub_id
                    sync_detail["before_values"] = current_props
                    
                    # Update HubSpot contact
                    self.hub.update_object("contacts", hub_id, contact_props)
                    results["updated"] += 1
                    sync_detail["action"] = "updated"
                    logger.info(f"│  ✓ UPDATED HubSpot contact ID: {hub_id}")
                else:
                    # Create HubSpot contact
                    response = self.hub.create_object("contacts", contact_props)
                    hub_id = response.get("id")
                    results["created"] += 1
                    sync_detail["hubspot_id"] = hub_id
                    sync_detail["action"] = "created"
                    logger.info(f"│  ✓ CREATED new HubSpot contact (ID: {hub_id})")

                logger.info(f"└─ COMPLETE: {firstname} {lastname} ({email})")
                results["sync_details"].append(sync_detail)

            except Exception as e:
                logger.error(f"Failed to sync Wrike contact {wrike_task_id}: {e}")
                results["failed"] += 1
                self.db.add_issue("wrike", "contact", wrike_task_id, "sync_error", str(e))

        self.db.set_state(state_key, end.isoformat())
        return results

    def sync_hubspot_to_wrike_companies(self, activity_id: int = None) -> Dict[str, int]:
        """Sync companies from HubSpot to Wrike (Bidirectional)"""
        state_key = "hubspot_to_wrike_last_run"
        last = self.db.get_state(state_key)
        start = dtparser.parse(last) if last else (utc_now() - timedelta(days=1))
        end = utc_now()

        results = {"processed": 0, "updated": 0, "failed": 0}
        
        # Search for updated companies in HubSpot
        filters = [{"filters": [{"propertyName": "hs_lastmodifieddate", "operator": "GTE", "value": to_epoch_ms(start)}]}]
        search_results = self.hub.search_objects("companies", filters, properties=list(self.hp_company_props.values()), sorts=["hs_lastmodifieddate"], limit=100)

        for hub_company in search_results.get("results", []):
            try:
                hub_id = hub_company["id"]
                results["processed"] += 1
                
                wrike_id = self.db.get_wrike_company_id_by_hubspot(hub_id)
                if not wrike_id:
                    continue 

                props = hub_company["properties"]
                
                # Extract values from HubSpot
                name = props.get(self.hp_company_props["name"])
                status = props.get(self.hp_company_props["account_status"])
                affinity = props.get(self.hp_company_props["affinity_score"])
                priority = props.get(self.hp_company_props["account_priority"])
                
                # Map priority back to tier
                tier = self.priority_to_tier.get(priority, priority)

                # Prepare Wrike custom fields
                custom_fields = []
                custom_fields = wrike_cf_set(custom_fields, self.w_company_cf["account_status"], status)
                custom_fields = wrike_cf_set(custom_fields, self.w_company_cf["affinity_score"], affinity)
                custom_fields = wrike_cf_set(custom_fields, self.w_company_cf["account_tier"], tier)

                # Update Wrike task - DO NOT update title to preserve "AdminCard_" prefix
                # Only update custom fields, not the task title
                self.wrk.update_task(wrike_id, custom_fields=custom_fields)
                results["updated"] += 1

            except Exception as e:
                logger.error(f"Failed to sync HubSpot company {hub_id} to Wrike: {e}")
                results["failed"] += 1

        self.db.set_state(state_key, end.isoformat())
        return results

    def sync_hubspot_to_wrike_contacts(self, activity_id: int = None) -> Dict[str, int]:
        """Sync contacts from HubSpot to Wrike (Bidirectional)"""
        state_key = "hubspot_to_wrike_contacts_last_run"
        last = self.db.get_state(state_key)
        start = dtparser.parse(last) if last else (utc_now() - timedelta(days=1))
        end = utc_now()

        results = {"processed": 0, "updated": 0, "failed": 0}
        
        # HubSpot search for modified contacts
        filters = [{"filters": [{"propertyName": "lastmodifieddate", "operator": "GTE", "value": to_epoch_ms(start)}]}]
        search_results = self.hub.search_objects("contacts", filters, properties=list(self.hp_contact_props.values()), sorts=["lastmodifieddate"], limit=100)

        for hub_contact in search_results.get("results", []):
            try:
                hub_id = hub_contact["id"]
                results["processed"] += 1
                
                email = hub_contact["properties"].get("email")
                if not email: continue
                
                wrike_tasks = self.wrk.query_tasks_in_folder_by_custom_field(
                    self.cfg.wrike["contacts_folder_id"],
                    self.w_contact_cf["email"],
                    email
                )
                
                if not wrike_tasks:
                    continue 
                
                wrike_id = wrike_tasks[0]["id"]
                props = hub_contact["properties"]
                
                # Prepare Wrike custom fields
                cf = []
                cf = wrike_cf_set(cf, self.w_contact_cf["first_name"], props.get("firstname"))
                cf = wrike_cf_set(cf, self.w_contact_cf["last_name"], props.get("lastname"))
                # NOTE: Title field does NOT exist in Wrike - do not sync
                cf = wrike_cf_set(cf, self.w_contact_cf["phone"], props.get("phone"))
                cf = wrike_cf_set(cf, self.w_contact_cf["mobile"], props.get("mobilephone"))
                cf = wrike_cf_set(cf, self.w_contact_cf["address1"], props.get("address"))
                cf = wrike_cf_set(cf, self.w_contact_cf["city"], props.get("city"))
                
                self.wrk.update_task(wrike_id, title=f"{props.get('firstname')} {props.get('lastname')}", custom_fields=cf)
                results["updated"] += 1

            except Exception as e:
                logger.error(f"Failed to sync HubSpot contact {hub_id} to Wrike: {e}")
                results["failed"] += 1

        self.db.set_state(state_key, end.isoformat())
        return results

    def sync_hubspot_company_names_to_wrike(self, activity_id: int = None) -> Dict[str, Any]:
        """
        Sync HubSpot company names to Wrike 'Hubspot Account Name' field.
        If the actual HubSpot company name differs from Wrike's field, update Wrike.
        """
        results = {
            "processed": 0, 
            "updated": 0, 
            "matched": 0, 
            "failed": 0, 
            "not_found_in_hubspot": 0,
            "updates": []
        }
        
        companies_folder = self.cfg.wrike["companies_folder_id"]
        hubspot_name_field_id = self.w_company_cf.get("hubspot_account_name")
        
        if not hubspot_name_field_id:
            logger.warning("hubspot_account_name field ID not configured in config.yaml")
            return results
        
        logger.info("┌─ SYNC: HubSpot Company Names → Wrike")
        logger.info(f"│  Field ID: {hubspot_name_field_id}")
        
        # Get all tasks in the companies folder with descendants
        for task in self.wrk.query_tasks_in_folder_updated_between(
            companies_folder, 
            utc_now() - timedelta(days=365),  # Check all companies from last year
            utc_now(), 
            descendants=True
        ):
            try:
                wrike_task_id = safe_str(task.get("id"))
                wrike_task_title = safe_str(task.get("title", ""))
                
                # Skip non-AdminCard tasks
                if "AdminCard" not in wrike_task_title:
                    continue
                
                results["processed"] += 1
                
                # Get HubSpot Account ID from Wrike (this is the proper way to match)
                wrike_hubspot_id = wrike_cf_get(task, self.w_company_cf.get("hubspot_account_id", ""))
                wrike_hubspot_name = wrike_cf_get(task, hubspot_name_field_id)
                
                # Try to find HubSpot company by ID first (preferred method)
                hubspot_company = None
                hub_id = None
                actual_hubspot_name = None
                
                if wrike_hubspot_id:
                    # Best case: We have the HubSpot ID stored in Wrike
                    try:
                        hubspot_company = self.hub.get_object("companies", wrike_hubspot_id, ["name"])
                        hub_id = wrike_hubspot_id
                        actual_hubspot_name = hubspot_company.get("properties", {}).get("name", "")
                    except:
                        pass
                
                if not hubspot_company:
                    # Fallback: Search by Wrike Client ID in HubSpot
                    wrike_task_id_prop = self.hp_company_props.get("wrike_task_id", "wrike_task_id")
                    filters = [{"filters": [{"propertyName": wrike_task_id_prop, "operator": "EQ", "value": wrike_task_id}]}]
                    try:
                        search_res = self.hub.search_objects("companies", filters, properties=["name"], sorts=[], limit=1)
                        
                        if search_res.get("results"):
                            hub_id = search_res["results"][0]["id"]
                            actual_hubspot_name = search_res["results"][0]["properties"].get("name", "")
                    except Exception as search_err:
                        # Search failed - property may not exist in HubSpot
                        if "400" in str(search_err):
                            logger.warning(f"│  Skipping {wrike_task_id}: HubSpot search failed (property may not exist)")
                            results["not_found_in_hubspot"] += 1
                            # Record diagnostic
                            diag = get_diagnostics()
                            diag.record_issue("HUBSPOT_SEARCH_FAILED", wrike_task_id, wrike_task_title, 
                                "wrike_task_id", "HubSpot search API returned 400 - property may not exist")
                            diag.record_skip(wrike_task_id, wrike_task_title, "HubSpot search failed (400)")
                            continue
                        raise
                
                if not actual_hubspot_name:
                    # No match found by ID - skip (don't match by name)
                    results["not_found_in_hubspot"] += 1
                    # Record diagnostic
                    diag = get_diagnostics()
                    diag.record_issue("ID_MAPPING_MISSING", wrike_task_id, wrike_task_title, 
                        "hubspot_account_name", "No HubSpot company found to get name from")
                    diag.record_skip(wrike_task_id, wrike_task_title, "No HubSpot match by ID")
                    continue
                
                # Check if Wrike's HubSpot Account Name needs updating
                name_changed = actual_hubspot_name.strip() != (wrike_hubspot_name or "").strip()
                
                if name_changed:
                    # Names differ - update Wrike
                    custom_fields = wrike_cf_set([], hubspot_name_field_id, actual_hubspot_name)
                    self.wrk.update_task(wrike_task_id, custom_fields=custom_fields)
                    
                    results["updated"] += 1
                    results["updates"].append({
                        "wrike_task": wrike_task_title,
                        "old_name": wrike_hubspot_name,
                        "new_name": actual_hubspot_name,
                        "hubspot_id": hub_id
                    })
                    
                    logger.info(f"│  ✓ UPDATED: {wrike_task_title}")
                    logger.info(f"│    Old: {wrike_hubspot_name}")
                    logger.info(f"│    New: {actual_hubspot_name}")
                else:
                    results["matched"] += 1
                
                # Record the change (or match) if tracking activity
                if activity_id:
                    self.db.record_change(activity_id, company_name, wrike_task_id, hub_id,
                        "company", "HubSpot Account Name", "Wrike",
                        wrike_hubspot_name, actual_hubspot_name, name_changed)
                
            except Exception as e:
                logger.error(f"│  ✗ Failed to sync company name for {wrike_task_id}: {e}")
                results["failed"] += 1
                # Record diagnostic
                diag = get_diagnostics()
                error_msg = str(e)
                if "404" in error_msg:
                    diag.record_issue("HUBSPOT_COMPANY_NOT_FOUND", wrike_task_id, wrike_task_title, 
                        "hubspot_account_name", f"Company not found: {error_msg}")
                else:
                    diag.record_issue("FIELD_VALUE_MISMATCH", wrike_task_id, wrike_task_title, 
                        "hubspot_account_name", f"Error: {error_msg}")
                diag.increment_failed()
        
        logger.info(f"│")
        logger.info(f"│  Summary: {results['processed']} processed, {results['updated']} updated, {results['matched']} matched")
        logger.info(f"└─ COMPLETE: HubSpot Company Names Sync")
        
        return results

    def sync_company_ids_bidirectional(self, activity_id: int = None) -> Dict[str, Any]:
        """
        Sync HubSpot Account IDs and Wrike Client IDs bidirectionally.
        - Updates Wrike "HubSpot Account ID" field with the HubSpot company ID
        - Updates HubSpot "Wrike Client ID" field with the Wrike task ID
        """
        results = {
            "processed": 0,
            "wrike_ids_updated": 0,
            "hubspot_ids_updated": 0,
            "already_synced": 0,
            "failed": 0,
            "updates": []
        }
        
        companies_folder = self.cfg.wrike["companies_folder_id"]
        
        # Get field IDs from config
        hubspot_account_id_field = self.w_company_cf.get("hubspot_account_id")
        hubspot_name_field = self.w_company_cf.get("hubspot_account_name")
        wrike_task_id_prop = self.hp_company_props.get("wrike_task_id", "wrike_task_id")
        
        if not hubspot_account_id_field:
            logger.warning("hubspot_account_id field ID not configured in config.yaml")
            return results
        
        logger.info("┌─ SYNC: HubSpot/Wrike IDs Bidirectional")
        logger.info(f"│  Wrike Field (HubSpot ID): {hubspot_account_id_field}")
        logger.info(f"│  HubSpot Field (Wrike ID): {wrike_task_id_prop}")
        
        # Get all tasks in the companies folder
        for task in self.wrk.query_tasks_in_folder_updated_between(
            companies_folder,
            utc_now() - timedelta(days=365),
            utc_now(),
            descendants=True
        ):
            try:
                wrike_task_id = safe_str(task.get("id"))
                wrike_task_title = safe_str(task.get("title", ""))
                
                # Skip non-AdminCard tasks
                if "AdminCard" not in wrike_task_title:
                    continue
                
                results["processed"] += 1
                
                # Get current values from Wrike
                wrike_hubspot_name = wrike_cf_get(task, hubspot_name_field)
                wrike_hubspot_id = wrike_cf_get(task, hubspot_account_id_field)
                
                # Find HubSpot company by ID (preferred) or by Wrike Client ID
                hubspot_company = None
                hubspot_id = None
                current_wrike_id_in_hubspot = None
                
                # Method 1: Use HubSpot ID stored in Wrike
                if wrike_hubspot_id:
                    try:
                        hubspot_company = self.hub.get_object("companies", wrike_hubspot_id, ["name", wrike_task_id_prop])
                        hubspot_id = wrike_hubspot_id
                        current_wrike_id_in_hubspot = hubspot_company.get("properties", {}).get(wrike_task_id_prop)
                    except:
                        pass
                
                # Method 2: Search HubSpot by Wrike Client ID
                if not hubspot_company:
                    filters = [{"filters": [{"propertyName": wrike_task_id_prop, "operator": "EQ", "value": wrike_task_id}]}]
                    search_res = self.hub.search_objects(
                        "companies", filters, 
                        properties=["name", wrike_task_id_prop], 
                        sorts=[], limit=1
                    )
                    
                    if search_res.get("results"):
                        hubspot_company = search_res["results"][0]
                        hubspot_id = hubspot_company["id"]
                        current_wrike_id_in_hubspot = hubspot_company.get("properties", {}).get(wrike_task_id_prop)
                
                if not hubspot_company:
                    # No match found by ID - skip
                    continue
                
                update_detail = {
                    "company": clean_company_name_for_hubspot(wrike_task_title),
                    "wrike_task_id": wrike_task_id,
                    "hubspot_id": hubspot_id,
                    "changes": []
                }
                
                company_name = clean_company_name_for_hubspot(wrike_task_title)
                
                # Check and update Wrike's HubSpot Account ID
                wrike_changed = not wrike_hubspot_id or str(wrike_hubspot_id) != str(hubspot_id)
                if wrike_changed:
                    try:
                        custom_fields = wrike_cf_set([], hubspot_account_id_field, hubspot_id)
                        self.wrk.update_task(wrike_task_id, custom_fields=custom_fields)
                        results["wrike_ids_updated"] += 1
                        update_detail["changes"].append(f"Wrike HubSpot ID: {wrike_hubspot_id or '(empty)'} → {hubspot_id}")
                    except Exception as e:
                        logger.error(f"│  ✗ Failed to update Wrike HubSpot ID for {wrike_task_title}: {e}")
                        results["failed"] += 1
                
                # Record change for Wrike HubSpot ID
                if activity_id:
                    self.db.record_change(activity_id, company_name, wrike_task_id, hubspot_id,
                        "company", "HubSpot Account ID", "Wrike",
                        wrike_hubspot_id or "", hubspot_id, wrike_changed)
                
                # Check and update HubSpot's Wrike Client ID
                hubspot_changed = not current_wrike_id_in_hubspot or current_wrike_id_in_hubspot != wrike_task_id
                if hubspot_changed:
                    try:
                        self.hub.update_object("companies", hubspot_id, {wrike_task_id_prop: wrike_task_id})
                        results["hubspot_ids_updated"] += 1
                        update_detail["changes"].append(f"HubSpot Wrike ID: {current_wrike_id_in_hubspot or '(empty)'} → {wrike_task_id}")
                    except Exception as e:
                        logger.error(f"│  ✗ Failed to update HubSpot Wrike ID for {wrike_task_title}: {e}")
                        results["failed"] += 1
                
                # Record change for HubSpot Wrike ID
                if activity_id:
                    self.db.record_change(activity_id, company_name, wrike_task_id, hubspot_id,
                        "company", "Wrike Client ID", "HubSpot",
                        current_wrike_id_in_hubspot or "", wrike_task_id, hubspot_changed)
                
                if update_detail["changes"]:
                    results["updates"].append(update_detail)
                    logger.info(f"│  ✓ {company_name}: {', '.join(update_detail['changes'])}")
                else:
                    results["already_synced"] += 1
                
            except Exception as e:
                logger.error(f"│  ✗ Failed to sync IDs for {wrike_task_id}: {e}")
                results["failed"] += 1
                # Record diagnostic
                diag = get_diagnostics()
                error_msg = str(e)
                if "404" in error_msg:
                    diag.record_issue("HUBSPOT_COMPANY_NOT_FOUND", wrike_task_id, wrike_task_title, 
                        "hubspot_account_id", f"Company not found during ID sync: {error_msg}")
                else:
                    diag.record_issue("FIELD_VALUE_MISMATCH", wrike_task_id, wrike_task_title, 
                        "hubspot_account_id", f"ID sync error: {error_msg}")
                diag.increment_failed()
        
        logger.info(f"│")
        logger.info(f"│  Summary: {results['processed']} processed")
        logger.info(f"│    Wrike HubSpot IDs updated: {results['wrike_ids_updated']}")
        logger.info(f"│    HubSpot Wrike IDs updated: {results['hubspot_ids_updated']}")
        logger.info(f"│    Already in sync: {results['already_synced']}")
        logger.info(f"└─ COMPLETE: ID Bidirectional Sync")
        
        return results

    def export_reconciliation_report(self, path: str = "reconciliation_report.csv") -> str:
        """Export unresolved issues to CSV"""
        issues = self.db.list_unresolved_issues()
        headers = ["ID", "Created At", "Source", "Entity Type", "Entity ID", "Issue Type", "Detail"]
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(issues)
            
        logger.info(f"Reconciliation report exported to {path}")
        return path
