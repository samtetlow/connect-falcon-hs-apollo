"""
FalconHub Database Abstraction Layer
Supports both SQLite (local development) and PostgreSQL (Vercel/production)
"""

import os
import json
import csv
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Check if we're on Vercel (PostgreSQL) or local (SQLite)
IS_VERCEL = os.environ.get('VERCEL') == '1' or os.environ.get('POSTGRES_URL') is not None
DATABASE_URL = os.environ.get('POSTGRES_URL') or os.environ.get('DATABASE_URL')


class DatabaseInterface(ABC):
    """Abstract base class for database operations"""
    
    @abstractmethod
    def execute(self, query: str, params: tuple = ()) -> Any:
        pass
    
    @abstractmethod
    def fetchone(self, query: str, params: tuple = ()) -> Optional[Tuple]:
        pass
    
    @abstractmethod
    def fetchall(self, query: str, params: tuple = ()) -> List[Tuple]:
        pass
    
    @abstractmethod
    def commit(self) -> None:
        pass


class SQLiteDatabase(DatabaseInterface):
    """SQLite implementation for local development"""
    
    def __init__(self, path: str = "sync.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()
    
    def execute(self, query: str, params: tuple = ()) -> Any:
        cur = self.conn.execute(query, params)
        return cur
    
    def fetchone(self, query: str, params: tuple = ()) -> Optional[Tuple]:
        return self.conn.execute(query, params).fetchone()
    
    def fetchall(self, query: str, params: tuple = ()) -> List[Tuple]:
        return self.conn.execute(query, params).fetchall()
    
    def commit(self) -> None:
        self.conn.commit()
    
    def get_last_insert_id(self, cursor) -> int:
        return cursor.lastrowid
    
    def _init_schema(self) -> None:
        """Initialize SQLite schema"""
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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Sync activities
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

        # Sync activity changes
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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Change tracking
        cur.execute("""
            CREATE TABLE IF NOT EXISTS change_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                record_id TEXT NOT NULL,
                record_name TEXT,
                change_type TEXT,
                detected_at TEXT NOT NULL,
                synced_at TEXT,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                UNIQUE(source, record_id, detected_at)
            )
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_tracking_status 
            ON change_tracking(status)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_tracking_source_record 
            ON change_tracking(source, record_id)
        """)

        # Reconciliation reports
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT NOT NULL,
                wrike_total INTEGER DEFAULT 0,
                hubspot_total INTEGER DEFAULT 0,
                matched INTEGER DEFAULT 0,
                wrike_only INTEGER DEFAULT 0,
                hubspot_only INTEGER DEFAULT 0,
                mismatched INTEGER DEFAULT 0,
                auto_fixed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'completed',
                details TEXT
            )
        """)

        self.conn.commit()


class PostgreSQLDatabase(DatabaseInterface):
    """PostgreSQL implementation for Vercel/production"""
    
    def __init__(self, database_url: str = None):
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        self.database_url = database_url or DATABASE_URL
        if not self.database_url:
            raise ValueError("No PostgreSQL database URL provided. Set POSTGRES_URL environment variable.")
        
        self.conn = psycopg2.connect(self.database_url)
        self.conn.autocommit = False
        self._init_schema()
    
    def execute(self, query: str, params: tuple = ()) -> Any:
        # Convert SQLite-style ? placeholders to PostgreSQL-style %s
        query = self._convert_placeholders(query)
        cur = self.conn.cursor()
        cur.execute(query, params)
        return cur
    
    def fetchone(self, query: str, params: tuple = ()) -> Optional[Tuple]:
        query = self._convert_placeholders(query)
        cur = self.conn.cursor()
        cur.execute(query, params)
        return cur.fetchone()
    
    def fetchall(self, query: str, params: tuple = ()) -> List[Tuple]:
        query = self._convert_placeholders(query)
        cur = self.conn.cursor()
        cur.execute(query, params)
        return cur.fetchall()
    
    def commit(self) -> None:
        self.conn.commit()
    
    def get_last_insert_id(self, cursor) -> int:
        # PostgreSQL returns the ID via RETURNING clause
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _convert_placeholders(self, query: str) -> str:
        """Convert SQLite ? placeholders to PostgreSQL %s"""
        return query.replace('?', '%s')
    
    def _init_schema(self) -> None:
        """Initialize PostgreSQL schema"""
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
                id SERIAL PRIMARY KEY,
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
                id SERIAL PRIMARY KEY,
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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Sync activities
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_activities (
                id SERIAL PRIMARY KEY,
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

        # Sync activity changes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_activity_changes (
                id SERIAL PRIMARY KEY,
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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Change tracking
        cur.execute("""
            CREATE TABLE IF NOT EXISTS change_tracking (
                id SERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                record_id TEXT NOT NULL,
                record_name TEXT,
                change_type TEXT,
                detected_at TEXT NOT NULL,
                synced_at TEXT,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                UNIQUE(source, record_id, detected_at)
            )
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_tracking_status 
            ON change_tracking(status)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_tracking_source_record 
            ON change_tracking(source, record_id)
        """)

        # Reconciliation reports
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_reports (
                id SERIAL PRIMARY KEY,
                run_at TEXT NOT NULL,
                wrike_total INTEGER DEFAULT 0,
                hubspot_total INTEGER DEFAULT 0,
                matched INTEGER DEFAULT 0,
                wrike_only INTEGER DEFAULT 0,
                hubspot_only INTEGER DEFAULT 0,
                mismatched INTEGER DEFAULT 0,
                auto_fixed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'completed',
                details TEXT
            )
        """)

        self.conn.commit()


class EnhancedDB:
    """
    Enhanced database wrapper that works with both SQLite and PostgreSQL.
    Automatically selects the appropriate backend based on environment.
    """

    def __init__(self, path: str = "sync.db"):
        self.is_postgres = IS_VERCEL and DATABASE_URL
        
        if self.is_postgres:
            logger.info("🐘 Using PostgreSQL database (Vercel mode)")
            self._db = PostgreSQLDatabase(DATABASE_URL)
        else:
            logger.info(f"📁 Using SQLite database: {path}")
            self._db = SQLiteDatabase(path)
            self.path = path

    @property
    def conn(self):
        """For backward compatibility with direct conn access"""
        return self._db.conn

    def execute(self, query: str, params: tuple = ()) -> Any:
        return self._db.execute(query, params)

    def fetchone(self, query: str, params: tuple = ()) -> Optional[Tuple]:
        return self._db.fetchone(query, params)

    def fetchall(self, query: str, params: tuple = ()) -> List[Tuple]:
        return self._db.fetchall(query, params)

    def commit(self) -> None:
        self._db.commit()

    # ============================================
    # SYNC OPERATION METHODS
    # ============================================

    def log_sync_operation(self, operation: str, source: str, target: str,
                          entity_type: str, entity_id: str, status: str, message: str = ""):
        """Log sync operations"""
        self.execute("""
            INSERT INTO sync_logs(operation, source_system, target_system,
                                 entity_type, entity_id, status, message)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        """, (operation, source, target, entity_type, entity_id, status, message))
        self.commit()

    def start_activity(self, activity_type: str) -> int:
        """Start a new sync activity and return its ID"""
        now = datetime.now(timezone.utc).isoformat()
        
        if self.is_postgres:
            cur = self.execute("""
                INSERT INTO sync_activities(activity_type, started_at, status)
                VALUES(%s, %s, 'running')
                RETURNING id
            """, (activity_type, now))
            self.commit()
            return self._db.get_last_insert_id(cur)
        else:
            cur = self.execute("""
                INSERT INTO sync_activities(activity_type, started_at, status)
                VALUES(?, ?, 'running')
            """, (activity_type, now))
            self.commit()
            return cur.lastrowid

    def complete_activity(self, activity_id: int, companies: int = 0, contacts: int = 0,
                         changes: int = 0, errors: int = 0, summary: str = ""):
        """Mark activity as complete with summary stats"""
        self.execute("""
            UPDATE sync_activities 
            SET completed_at = ?, status = 'completed',
                companies_processed = ?, contacts_processed = ?,
                changes_made = ?, errors = ?, summary = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), companies, contacts, 
              changes, errors, summary, activity_id))
        self.commit()

    def fail_activity(self, activity_id: int, error_message: str):
        """Mark activity as failed"""
        self.execute("""
            UPDATE sync_activities 
            SET completed_at = ?, status = 'failed', summary = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), error_message, activity_id))
        self.commit()

    def record_change(self, activity_id: int, company_name: str, wrike_id: str, 
                     hubspot_id: str, entity_type: str, field_name: str, 
                     system_changed: str, old_value: str, new_value: str, changed: bool = True):
        """Record a single field change/comparison"""
        self.execute("""
            INSERT INTO sync_activity_changes(
                activity_id, company_name, wrike_company_id, hubspot_company_id,
                entity_type, field_name, system_changed, old_value, new_value, changed
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (activity_id, company_name, wrike_id, hubspot_id, entity_type,
              field_name, system_changed, str(old_value) if old_value else "", 
              str(new_value) if new_value else "", 1 if changed else 0))
        self.commit()

    def list_activities(self, limit: int = 50) -> List[Dict]:
        """List recent sync activities"""
        rows = self.fetchall("""
            SELECT id, activity_type, started_at, completed_at, status,
                   companies_processed, contacts_processed, changes_made, errors, summary
            FROM sync_activities
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return [{
            "id": r[0], "activity_type": r[1], "started_at": r[2],
            "completed_at": r[3], "status": r[4], "companies_processed": r[5],
            "contacts_processed": r[6], "changes_made": r[7], "errors": r[8],
            "summary": r[9]
        } for r in rows]

    def get_activity_changes(self, activity_id: int) -> List[Dict]:
        """Get all changes for a specific activity"""
        rows = self.fetchall("""
            SELECT id, company_name, wrike_company_id, hubspot_company_id,
                   entity_type, field_name, system_changed, old_value, new_value, changed
            FROM sync_activity_changes
            WHERE activity_id = ?
            ORDER BY company_name, field_name
        """, (activity_id,))
        return [{
            "id": r[0], "company_name": r[1], "wrike_company_id": r[2],
            "hubspot_company_id": r[3], "entity_type": r[4], "field_name": r[5],
            "system_changed": r[6], "old_value": r[7], "new_value": r[8],
            "changed": bool(r[9])
        } for r in rows]

    def export_activity_report(self, activity_id: int, path: str = None) -> str:
        """Export activity changes to CSV"""
        if path is None:
            path = f"/tmp/activity_report_{activity_id}.csv"
        
        changes = self.get_activity_changes(activity_id)
        activity = self.fetchone(
            "SELECT activity_type, started_at FROM sync_activities WHERE id = ?",
            (activity_id,)
        )
        
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

    # ============================================
    # STATE MANAGEMENT
    # ============================================

    def get_state(self, k: str) -> Optional[str]:
        row = self.fetchone("SELECT v FROM sync_state WHERE k = ?", (k,))
        return row[0] if row else None

    def set_state(self, k: str, v: str) -> None:
        if self.is_postgres:
            self.execute("""
                INSERT INTO sync_state(k, v) VALUES(%s, %s)
                ON CONFLICT(k) DO UPDATE SET v = EXCLUDED.v
            """, (k, v))
        else:
            self.execute("""
                INSERT INTO sync_state(k, v) VALUES(?, ?)
                ON CONFLICT(k) DO UPDATE SET v=excluded.v
            """, (k, v))
        self.commit()

    # ============================================
    # COMPANY MAPPING METHODS
    # ============================================

    def get_hubspot_company_id(self, wrike_company_id: str) -> Optional[str]:
        row = self.fetchone(
            "SELECT hubspot_company_id FROM company_id_map WHERE wrike_company_id = ? AND sync_status = 'active'",
            (wrike_company_id,)
        )
        return row[0] if row else None

    def get_wrike_company_id_by_hubspot(self, hubspot_company_id: str) -> Optional[str]:
        row = self.fetchone(
            "SELECT wrike_company_id FROM company_id_map WHERE hubspot_company_id = ? AND sync_status = 'active'",
            (hubspot_company_id,)
        )
        return row[0] if row else None

    def upsert_company_mapping(self, wrike_company_id: str, hubspot_company_id: str,
                              company_name: str = "", notes: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        if self.is_postgres:
            self.execute("""
                INSERT INTO company_id_map(wrike_company_id, hubspot_company_id, company_name,
                                          updated_at, last_synced, notes)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT(wrike_company_id) DO UPDATE SET
                    hubspot_company_id = EXCLUDED.hubspot_company_id,
                    company_name = EXCLUDED.company_name,
                    updated_at = EXCLUDED.updated_at,
                    last_synced = EXCLUDED.last_synced,
                    notes = EXCLUDED.notes,
                    sync_status = 'active'
            """, (wrike_company_id, hubspot_company_id, company_name, now, now, notes))
        else:
            self.execute("""
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
            """, (wrike_company_id, hubspot_company_id, company_name, now, now, notes))
        self.commit()

    # ============================================
    # ISSUE TRACKING
    # ============================================

    def add_issue(self, source: str, entity_type: str, entity_id: str,
                 issue_type: str, detail: str) -> None:
        self.execute("""
            INSERT INTO reconciliation_issue(created_at, source, entity_type,
                                           entity_id, issue_type, detail, resolved)
            VALUES(?, ?, ?, ?, ?, ?, 0)
        """, (datetime.now(timezone.utc).isoformat(), source, entity_type,
             entity_id, issue_type, detail))
        self.commit()
        logger.warning(f"Reconciliation issue: {issue_type} - {detail}")

    def list_unresolved_issues(self) -> List[Tuple]:
        return self.fetchall("""
            SELECT id, created_at, source, entity_type, entity_id, issue_type, detail
            FROM reconciliation_issue
            WHERE resolved = 0
            ORDER BY id ASC
        """)

    def get_company_mappings_report(self) -> Dict:
        """Generate company mappings report"""
        rows = self.fetchone(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN sync_status = 'active' THEN 1 ELSE 0 END) as active "
            "FROM company_id_map"
        )

        recent = self.fetchall(
            "SELECT company_name, wrike_company_id, hubspot_company_id, last_synced "
            "FROM company_id_map "
            "WHERE sync_status = 'active' "
            "ORDER BY last_synced DESC LIMIT 10"
        )

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

    # ============================================
    # CHANGE TRACKING METHODS (Event-Driven Sync)
    # ============================================

    def track_change(self, source: str, record_id: str, record_name: str = "",
                    change_type: str = "update") -> int:
        """Record a detected change for later processing"""
        now = datetime.now(timezone.utc).isoformat()
        try:
            if self.is_postgres:
                cur = self.execute("""
                    INSERT INTO change_tracking(source, record_id, record_name, change_type, detected_at, status)
                    VALUES(%s, %s, %s, %s, %s, 'pending')
                    ON CONFLICT DO NOTHING
                    RETURNING id
                """, (source, record_id, record_name, change_type, now))
                self.commit()
                result = cur.fetchone()
                return result[0] if result else 0
            else:
                cur = self.execute("""
                    INSERT OR IGNORE INTO change_tracking(source, record_id, record_name, change_type, detected_at, status)
                    VALUES(?, ?, ?, ?, ?, 'pending')
                """, (source, record_id, record_name, change_type, now))
                self.commit()
                return cur.lastrowid
        except Exception:
            return 0

    def get_pending_changes(self, limit: int = 100) -> List[Dict]:
        """Get pending changes that need to be synced"""
        rows = self.fetchall("""
            SELECT id, source, record_id, record_name, change_type, detected_at
            FROM change_tracking
            WHERE status = 'pending'
            ORDER BY detected_at ASC
            LIMIT ?
        """, (limit,))
        
        return [{
            "id": r[0], "source": r[1], "record_id": r[2],
            "record_name": r[3], "change_type": r[4], "detected_at": r[5]
        } for r in rows]

    def mark_change_synced(self, change_id: int) -> None:
        """Mark a change as successfully synced"""
        now = datetime.now(timezone.utc).isoformat()
        self.execute("""
            UPDATE change_tracking
            SET status = 'synced', synced_at = ?
            WHERE id = ?
        """, (now, change_id))
        self.commit()

    def mark_change_failed(self, change_id: int, error: str) -> None:
        """Mark a change as failed with error message"""
        now = datetime.now(timezone.utc).isoformat()
        self.execute("""
            UPDATE change_tracking
            SET status = 'failed', synced_at = ?, error_message = ?
            WHERE id = ?
        """, (now, error, change_id))
        self.commit()

    def get_change_stats(self) -> Dict:
        """Get statistics about change tracking"""
        rows = self.fetchall("""
            SELECT status, COUNT(*) as count
            FROM change_tracking
            GROUP BY status
        """)
        
        stats = {"pending": 0, "synced": 0, "failed": 0}
        for r in rows:
            stats[r[0]] = r[1]
        
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        recent = self.fetchone("""
            SELECT COUNT(*) FROM change_tracking
            WHERE detected_at > ?
        """, (yesterday,))
        
        stats["changes_last_24h"] = recent[0] if recent else 0
        return stats

    def cleanup_old_changes(self, days: int = 30) -> int:
        """Remove old synced/failed changes"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cur = self.execute("""
            DELETE FROM change_tracking
            WHERE status IN ('synced', 'failed') AND detected_at < ?
        """, (cutoff,))
        self.commit()
        return cur.rowcount

    # ============================================
    # RECONCILIATION REPORT METHODS
    # ============================================

    def save_reconciliation_report(self, report: Dict) -> int:
        """Save a reconciliation report"""
        if self.is_postgres:
            cur = self.execute("""
                INSERT INTO reconciliation_reports(
                    run_at, wrike_total, hubspot_total, matched,
                    wrike_only, hubspot_only, mismatched, auto_fixed,
                    status, details
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                datetime.now(timezone.utc).isoformat(),
                report.get("wrike_total", 0),
                report.get("hubspot_total", 0),
                report.get("matched", 0),
                report.get("wrike_only", 0),
                report.get("hubspot_only", 0),
                report.get("mismatched", 0),
                report.get("auto_fixed", 0),
                report.get("status", "completed"),
                json.dumps(report.get("details", {}))
            ))
            self.commit()
            return self._db.get_last_insert_id(cur)
        else:
            cur = self.execute("""
                INSERT INTO reconciliation_reports(
                    run_at, wrike_total, hubspot_total, matched,
                    wrike_only, hubspot_only, mismatched, auto_fixed,
                    status, details
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(timezone.utc).isoformat(),
                report.get("wrike_total", 0),
                report.get("hubspot_total", 0),
                report.get("matched", 0),
                report.get("wrike_only", 0),
                report.get("hubspot_only", 0),
                report.get("mismatched", 0),
                report.get("auto_fixed", 0),
                report.get("status", "completed"),
                json.dumps(report.get("details", {}))
            ))
            self.commit()
            return cur.lastrowid

    def get_last_reconciliation_report(self) -> Optional[Dict]:
        """Get the most recent reconciliation report"""
        row = self.fetchone("""
            SELECT id, run_at, wrike_total, hubspot_total, matched,
                   wrike_only, hubspot_only, mismatched, auto_fixed,
                   status, details
            FROM reconciliation_reports
            ORDER BY id DESC
            LIMIT 1
        """)
        
        if not row:
            return None
        
        return {
            "id": row[0], "run_at": row[1], "wrike_total": row[2],
            "hubspot_total": row[3], "matched": row[4],
            "wrike_only": row[5], "hubspot_only": row[6],
            "mismatched": row[7], "auto_fixed": row[8],
            "status": row[9], "details": json.loads(row[10]) if row[10] else {}
        }

    def list_reconciliation_reports(self, limit: int = 10) -> List[Dict]:
        """List recent reconciliation reports"""
        rows = self.fetchall("""
            SELECT id, run_at, wrike_total, hubspot_total, matched,
                   wrike_only, hubspot_only, mismatched, auto_fixed, status
            FROM reconciliation_reports
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        
        return [{
            "id": r[0], "run_at": r[1], "wrike_total": r[2],
            "hubspot_total": r[3], "matched": r[4],
            "wrike_only": r[5], "hubspot_only": r[6],
            "mismatched": r[7], "auto_fixed": r[8], "status": r[9]
        } for r in rows]

