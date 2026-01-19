"""
FalconHub - Connect Falcon-HubSpot
Clean FastAPI application for Falcon and HubSpot integration
With Event-Driven Synchronization Support
"""

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from datetime import datetime, timezone
import json
import logging
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
from wrike_client import WrikeClient
from hubspot_client import HubSpotClient

# APScheduler for background job scheduling
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False
    logging.warning("APScheduler not installed. Background sync disabled.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from sync_engine import EnhancedSyncEngine, EnhancedDB, EnhancedHubSpotClient, EnhancedWrikeClient, load_config as load_sync_config
    SYNC_ENGINE_AVAILABLE = True
except ImportError as e:
    SYNC_ENGINE_AVAILABLE = False
    logger.warning(f"Sync Engine not fully available: {e}")

# Global clients
wrike_client = None
hubspot_client = None
sync_engine = None
db = None
scheduler = None  # APScheduler instance
sync_config = None  # Configuration loaded from config.yaml

# Scheduler state tracking
scheduler_state = {
    "enabled": False,
    "change_detection_interval_minutes": 2,
    "reconciliation_hour": 0,
    "last_change_detection": None,
    "last_reconciliation": None,
    "next_change_detection": None,
    "next_reconciliation": None,
    "change_detections_run": 0,
    "reconciliations_run": 0,
    "webhooks_enabled": False
}

def load_config():
    """Load configuration from config.json"""
    config_path = Path(__file__).parent / "config.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    return {}

async def run_change_detection():
    """Background job: Detect changes in Wrike and HubSpot and sync them"""
    global sync_engine, scheduler_state
    if not sync_engine:
        logger.warning("Change detection skipped: Sync engine not available")
        return
    
    try:
        logger.info("🔄 Running change detection...")
        scheduler_state["last_change_detection"] = datetime.now(timezone.utc).isoformat()
        scheduler_state["change_detections_run"] += 1
        
        # Use the incremental sync methods
        results = await asyncio.get_event_loop().run_in_executor(
            None, sync_engine.detect_and_sync_changes
        )
        
        logger.info(f"✓ Change detection complete: {results}")
        return results
    except Exception as e:
        logger.error(f"Change detection failed: {e}")
        return {"error": str(e)}

async def run_daily_reconciliation():
    """Background job: Daily full reconciliation"""
    global sync_engine, scheduler_state
    if not sync_engine:
        logger.warning("Daily reconciliation skipped: Sync engine not available")
        return
    
    try:
        logger.info("📊 Running daily reconciliation...")
        scheduler_state["last_reconciliation"] = datetime.now(timezone.utc).isoformat()
        scheduler_state["reconciliations_run"] += 1
        
        # Run full reconciliation
        results = await asyncio.get_event_loop().run_in_executor(
            None, sync_engine.daily_reconciliation
        )
        
        logger.info(f"✓ Daily reconciliation complete: {results}")
        return results
    except Exception as e:
        logger.error(f"Daily reconciliation failed: {e}")
        return {"error": str(e)}

def update_scheduler_next_runs():
    """Update next run times in scheduler state"""
    global scheduler, scheduler_state
    if scheduler and scheduler.running:
        change_job = scheduler.get_job('change_detection')
        reconcile_job = scheduler.get_job('daily_reconcile')
        
        if change_job and change_job.next_run_time:
            scheduler_state["next_change_detection"] = change_job.next_run_time.isoformat()
        if reconcile_job and reconcile_job.next_run_time:
            scheduler_state["next_reconciliation"] = reconcile_job.next_run_time.isoformat()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown"""
    global wrike_client, hubspot_client, sync_engine, db, scheduler, sync_config, scheduler_state

    # Startup
    config = load_config()

    if "wrike" in config and config["wrike"].get("api_token"):
        wrike_client = WrikeClient(config["wrike"])
        logger.info("✓ Falcon client initialized")
    
    if "hubspot" in config and (config["hubspot"].get("api_key") or config["hubspot"].get("access_token")):
        hubspot_client = HubSpotClient(config["hubspot"])
        logger.info("✓ HubSpot client initialized")

    # Initialize Enhanced Sync Engine if config.yaml exists
    sync_config_path = Path(__file__).parent / "config.yaml"
    if SYNC_ENGINE_AVAILABLE and sync_config_path.exists():
        try:
            db = EnhancedDB("sync.db")
            cfg = load_sync_config(str(sync_config_path))
            sync_config = cfg  # Store for scheduler access
            
            # Determine HubSpot token and auth method
            # Prefer access_token (Bearer) over legacy api_key (hapikey)
            hub_access_token = (
                cfg.hubspot.get("access_token")
                or config.get("hubspot", {}).get("access_token")
            )
            hub_api_key = (
                cfg.hubspot.get("api_key")
                or config.get("hubspot", {}).get("api_key")
            )
            
            # Use access_token with Bearer auth, or api_key with legacy hapikey auth
            if hub_access_token:
                hub_token = hub_access_token
                use_hapikey = False
            elif hub_api_key:
                hub_token = hub_api_key
                use_hapikey = True
            else:
                hub_token = None
                use_hapikey = False
            
            wrk_token = cfg.wrike.get("api_token") or (config.get("wrike", {}).get("api_token"))
            
            if hub_token and wrk_token:
                hub_enhanced = EnhancedHubSpotClient(hub_token, use_hapikey=use_hapikey)
                wrk_enhanced = EnhancedWrikeClient(wrk_token)
                sync_engine = EnhancedSyncEngine(cfg, db, hub_enhanced, wrk_enhanced)
                logger.info(f"✓ Enhanced Sync Engine initialized (HubSpot auth: {'hapikey' if use_hapikey else 'Bearer'})")
            else:
                logger.warning("Missing API tokens for Enhanced Sync Engine")
        except Exception as e:
            logger.error(f"Failed to initialize Sync Engine: {e}")

    # Initialize APScheduler for background jobs
    if SCHEDULER_AVAILABLE and sync_engine:
        try:
            scheduler = AsyncIOScheduler()
            
            # Get scheduler settings from config
            sync_settings = cfg.sync if sync_config else {}
            change_interval = sync_settings.get("change_detection_interval_minutes", 2)
            reconcile_hour = sync_settings.get("reconciliation_hour", 0)
            webhooks_enabled = sync_settings.get("webhooks_enabled", False)
            
            # Update scheduler state
            scheduler_state["enabled"] = True
            scheduler_state["change_detection_interval_minutes"] = change_interval
            scheduler_state["reconciliation_hour"] = reconcile_hour
            scheduler_state["webhooks_enabled"] = webhooks_enabled
            
            # Add change detection job (runs every N minutes)
            scheduler.add_job(
                run_change_detection,
                IntervalTrigger(minutes=change_interval),
                id='change_detection',
                name='Change Detection',
                replace_existing=True
            )
            
            # Add daily reconciliation job (runs at midnight)
            scheduler.add_job(
                run_daily_reconciliation,
                CronTrigger(hour=reconcile_hour, minute=0),
                id='daily_reconcile',
                name='Daily Reconciliation',
                replace_existing=True
            )
            
            scheduler.start()
            update_scheduler_next_runs()
            
            logger.info(f"✓ Background scheduler started (change detection every {change_interval} min, reconciliation at {reconcile_hour}:00)")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            scheduler_state["enabled"] = False

    logger.info("✓ FalconHub server started")

    yield
    
    # Shutdown
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("✓ Scheduler shut down")

# Initialize FastAPI app with lifespan
app = FastAPI(
    title="FalconHub",
    description="Intelligent Falcon-HubSpot Integration Platform",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
static_path = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Pydantic models
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Beautiful FalconHub dashboard"""
    try:
        with open("dashboard.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>FalconHub Dashboard</h1><p>Dashboard file not found. Please check dashboard.html</p>"

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0"
    )

@app.get("/api/falcon/status")
async def falcon_status():
    """Check Falcon client status"""
    global wrike_client
    config = load_config()
    token = config.get("wrike", {}).get("api_token")
    
    if wrike_client:
        return {
            "status": "connected", 
            "message": "Falcon client ready",
            "token_status": "Configured" if token else "Missing"
        }
    else:
        return {
            "status": "disconnected", 
            "message": "Falcon client not configured",
            "token_status": "Missing"
        }

@app.get("/api/hubspot/status")
async def hubspot_status():
    """Check HubSpot client status"""
    global hubspot_client
    config = load_config()
    key = config.get("hubspot", {}).get("access_token") or config.get("hubspot", {}).get("api_key")
    
    if hubspot_client:
        return {
            "status": "connected", 
            "message": "HubSpot client ready",
            "token_status": "Configured" if key else "Missing"
        }
    else:
        return {
            "status": "disconnected", 
            "message": "HubSpot client not available",
            "token_status": "Missing"
        }

@app.get("/api/field-mappings")
async def get_field_mappings():
    """Get the configured field mappings for display"""
    try:
        config_path = Path(__file__).parent / "config.yaml"
        if config_path.exists():
            import yaml
            with open(config_path) as f:
                config = yaml.safe_load(f)
            
            # Build field mappings from config
            falcon_fields = []
            hubspot_fields = []
            
            # Company fields: Falcon → HubSpot
            wrike_company = config.get("wrike", {}).get("company_custom_fields", {})
            hubspot_company = config.get("hubspot", {}).get("company_properties", {})
            
            if wrike_company.get("account_status"):
                falcon_fields.append({"field": "Account Status", "direction": "outbound", "target": "HubSpot"})
            if wrike_company.get("affinity_score"):
                falcon_fields.append({"field": "Affinity Score", "direction": "outbound", "target": "HubSpot"})
            if wrike_company.get("account_tier"):
                falcon_fields.append({"field": "Account Tier → Priority", "direction": "outbound", "target": "HubSpot"})
            if wrike_company.get("hubspot_account_name"):
                falcon_fields.append({"field": "Company Name", "direction": "inbound", "target": "from HubSpot"})
            if wrike_company.get("hubspot_account_id"):
                falcon_fields.append({"field": "HubSpot Account ID", "direction": "bidirectional", "target": "↔ HubSpot"})
            
            # Contact fields: Falcon → HubSpot (NOTE: Title is NOT synced - does not exist in Wrike)
            wrike_contact = config.get("wrike", {}).get("contact_custom_fields", {})
            if wrike_contact:
                contact_fields = ["First Name", "Last Name", "Email", "Phone", "Mobile", "Address"]
                for cf in contact_fields:
                    falcon_fields.append({"field": cf, "direction": "outbound", "target": "HubSpot"})
            
            # HubSpot perspective (mirror of above)
            if hubspot_company.get("account_status"):
                hubspot_fields.append({"field": "Account Status", "direction": "inbound", "target": "from Falcon"})
            if hubspot_company.get("affinity_score"):
                hubspot_fields.append({"field": "Affinity Score", "direction": "inbound", "target": "from Falcon"})
            if hubspot_company.get("account_priority"):
                hubspot_fields.append({"field": "Company Priority", "direction": "inbound", "target": "from Falcon Tier"})
            if hubspot_company.get("name"):
                hubspot_fields.append({"field": "Company Name", "direction": "outbound", "target": "to Falcon"})
            if hubspot_company.get("wrike_client_id"):
                hubspot_fields.append({"field": "Wrike Client ID", "direction": "bidirectional", "target": "↔ Falcon"})
            
            # Contact fields: HubSpot receives from Falcon (NOTE: Title is NOT synced - does not exist in Wrike)
            hubspot_contact = config.get("hubspot", {}).get("contact_properties", {})
            if hubspot_contact:
                contact_fields = ["First Name", "Last Name", "Email", "Phone", "Mobile", "Address"]
                for cf in contact_fields:
                    hubspot_fields.append({"field": cf, "direction": "inbound", "target": "from Falcon"})
            
            return {
                "falcon": falcon_fields,
                "hubspot": hubspot_fields
            }
        else:
            return {"falcon": [], "hubspot": [], "error": "Config not found"}
    except Exception as e:
        logger.error(f"Failed to load field mappings: {e}")
        return {"falcon": [], "hubspot": [], "error": str(e)}

@app.post("/api/sync/run")
async def run_sync():
    """Manually trigger a sync cycle"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured. Please check config.yaml"}
    
    try:
        results = sync_engine.sync_once()
        return {"status": "success", "results": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/sync/run-stream")
async def run_sync_stream():
    """Trigger sync with streaming output via Server-Sent Events"""
    from fastapi.responses import StreamingResponse
    import asyncio
    import json
    import queue
    import threading
    
    global sync_engine
    if not sync_engine:
        async def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Sync Engine not configured'})}\n\n"
        return StreamingResponse(error_stream(), media_type="text/event-stream")
    
    # Create a queue to capture log messages
    log_queue = queue.Queue()
    sync_done = threading.Event()
    sync_results = {}
    sync_error = None
    
    # Custom logging handler to capture logs
    class QueueHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                level = record.levelname.lower()
                # Map log levels to terminal display levels
                if level == 'error':
                    display_level = 'error'
                elif level == 'warning':
                    display_level = 'warning'
                elif level == 'info':
                    display_level = 'info'
                else:
                    display_level = 'dim'
                log_queue.put({'message': msg, 'level': display_level})
            except Exception:
                self.handleError(record)
    
    # Add handler to capture sync_engine and related loggers
    queue_handler = QueueHandler()
    queue_handler.setFormatter(logging.Formatter('%(message)s'))
    
    # Get all relevant loggers
    loggers_to_capture = [
        logging.getLogger('sync_engine'),
        logging.getLogger('hubspot_client'),
        logging.getLogger('wrike_client'),
    ]
    
    for log in loggers_to_capture:
        log.addHandler(queue_handler)
    
    def run_sync():
        nonlocal sync_results, sync_error
        try:
            sync_results = sync_engine.sync_once()
        except Exception as e:
            sync_error = str(e)
        finally:
            sync_done.set()
    
    # Start sync in background thread
    sync_thread = threading.Thread(target=run_sync)
    sync_thread.start()
    
    async def sync_stream():
        try:
            def make_sse(msg, level=''):
                return "data: " + json.dumps({'type': 'log', 'message': msg, 'level': level}) + "\n\n"
            
            yield make_sse('═' * 50, 'dim')
            yield make_sse('STARTING FULL SYNCHRONIZATION', 'header')
            yield make_sse('═' * 50, 'dim')
            yield make_sse('', '')
            
            # Stream logs while sync is running
            while not sync_done.is_set() or not log_queue.empty():
                try:
                    log_entry = log_queue.get(timeout=0.1)
                    yield make_sse(log_entry['message'], log_entry['level'])
                except queue.Empty:
                    await asyncio.sleep(0.05)
                    continue
            
            # Clean up handlers
            for log in loggers_to_capture:
                log.removeHandler(queue_handler)
            
            # Check for errors
            if sync_error:
                yield make_sse('', '')
                yield make_sse(f'✗ SYNC FAILED: {sync_error}', 'error')
                yield f"data: {json.dumps({'type': 'error', 'message': sync_error})}\n\n"
                return
            
            results = sync_results
            
            # Output results summary
            yield make_sse('', '')
            yield make_sse('═' * 50, 'dim')
            yield make_sse('SYNC RESULTS', 'header')
            yield make_sse('═' * 50, 'dim')
            
            # Companies to HubSpot
            c2h = results.get('companies_to_hubspot', {})
            c2h_proc = c2h.get('processed', 0)
            c2h_created = c2h.get('created', 0)
            c2h_updated = c2h.get('updated', 0)
            c2h_failed = c2h.get('failed', 0)
            yield make_sse(f'Companies → HubSpot: {c2h_proc} processed', 'info')
            yield make_sse(f'  Created: {c2h_created}, Updated: {c2h_updated}, Failed: {c2h_failed}', 'dim')
            
            # Contacts to HubSpot
            ct2h = results.get('contacts_to_hubspot', {})
            ct2h_proc = ct2h.get('processed', 0)
            ct2h_created = ct2h.get('created', 0)
            ct2h_updated = ct2h.get('updated', 0)
            ct2h_failed = ct2h.get('failed', 0)
            yield make_sse(f'Contacts → HubSpot: {ct2h_proc} processed', 'info')
            yield make_sse(f'  Created: {ct2h_created}, Updated: {ct2h_updated}, Failed: {ct2h_failed}', 'dim')
            
            # Companies to Wrike
            c2w = results.get('companies_to_wrike', {})
            c2w_proc = c2w.get('processed', 0)
            yield make_sse(f'Companies → Falcon: {c2w_proc} processed', 'info')
            
            # Company names
            names = results.get('company_names_synced', {})
            names_updated = names.get('updated', 0)
            yield make_sse(f'Company Names Updated: {names_updated}', 'info')
            
            # IDs
            ids = results.get('company_ids_synced', {})
            total_ids = (ids.get('wrike_ids_updated', 0) or 0) + (ids.get('hubspot_ids_updated', 0) or 0)
            yield make_sse(f'IDs Synced: {total_ids}', 'info')
            
            yield make_sse('', '')
            duration = results.get('duration_seconds', 0)
            activity_id = results.get('activity_id', 'N/A')
            yield make_sse(f'Duration: {duration:.2f}s', 'dim')
            yield make_sse(f'Activity ID: {activity_id}', 'dim')
            
            issues = results.get('issues_found', 0)
            if issues > 0:
                yield make_sse(f'⚠ Issues Found: {issues}', 'warning')
            
            yield "data: " + json.dumps({'type': 'complete', 'results': results}) + "\n\n"
            
        except Exception as e:
            logger.error(f"Streaming sync failed: {e}")
            # Clean up handlers on error
            for log in loggers_to_capture:
                try:
                    log.removeHandler(queue_handler)
                except:
                    pass
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        sync_stream(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )

@app.get("/api/reconciliation/issues")
async def get_issues():
    """Get unresolved reconciliation issues"""
    global db
    if not db:
        return {"status": "error", "message": "Database not available"}
    
    issues = db.list_unresolved_issues()
    return {
        "status": "success",
        "count": len(issues),
        "issues": [
            {
                "id": i[0],
                "timestamp": i[1],
                "source": i[2],
                "type": i[3],
                "id": i[4],
                "issue": i[5],
                "detail": i[6]
            } for i in issues
        ]
    }

@app.get("/api/reconciliation/report")
async def get_report():
    """Export and download reconciliation report"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not available"}
    
    try:
        report_path = sync_engine.export_reconciliation_report()
        return FileResponse(
            path=report_path,
            filename="reconciliation_report.csv",
            media_type="text/csv"
        )
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/sync/report")
async def sync_report_alias():
    """Alias for dashboard compatibility"""
    return await get_report()

@app.get("/api/sync/test")
async def test_sync_endpoint():
    """Perform a limited test sync"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    try:
        results = sync_engine.test_sync()
        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/sync/verify-config")
async def verify_config_endpoint_alias():
    """Alias for dashboard compatibility during transition"""
    return await test_sync_endpoint()

@app.post("/api/sync/ids")
async def sync_company_ids():
    """Sync HubSpot Account IDs and Wrike Client IDs bidirectionally"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    try:
        results = sync_engine.sync_company_ids_bidirectional()
        return {
            "status": "success",
            "message": f"Synced {results['wrike_ids_updated']} Wrike IDs and {results['hubspot_ids_updated']} HubSpot IDs",
            "details": results
        }
    except Exception as e:
        logger.error(f"ID sync failed: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/sync/company-names")
async def sync_company_names():
    """Sync HubSpot company names to Wrike"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    try:
        results = sync_engine.sync_hubspot_company_names_to_wrike()
        return {
            "status": "success",
            "message": f"Updated {results['updated']} company names",
            "details": results
        }
    except Exception as e:
        logger.error(f"Company name sync failed: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/activities")
async def list_activities():
    """List recent sync activities"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured", "activities": []}
    
    try:
        activities = sync_engine.db.list_activities(limit=50)
        return {"status": "success", "activities": activities}
    except Exception as e:
        logger.error(f"Failed to list activities: {e}")
        return {"status": "error", "message": str(e), "activities": []}

@app.get("/api/activities/{activity_id}")
async def get_activity_details(activity_id: int):
    """Get details of a specific activity"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    try:
        changes = sync_engine.db.get_activity_changes(activity_id)
        return {"status": "success", "activity_id": activity_id, "changes": changes, "total_changes": len(changes)}
    except Exception as e:
        logger.error(f"Failed to get activity details: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/activities/{activity_id}/report")
async def download_activity_report(activity_id: int):
    """Download CSV report for a specific activity"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    try:
        report_path = sync_engine.db.export_activity_report(activity_id)
        return FileResponse(
            path=report_path,
            filename=f"activity_report_{activity_id}.csv",
            media_type="text/csv"
        )
    except Exception as e:
        logger.error(f"Failed to generate activity report: {e}")
        return {"status": "error", "message": str(e)}

# ============================================
# SCHEDULER & EVENT-DRIVEN SYNC ENDPOINTS
# ============================================

@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get current scheduler status and next run times"""
    global scheduler, scheduler_state
    
    update_scheduler_next_runs()
    
    return {
        "status": "success",
        "scheduler": {
            "available": SCHEDULER_AVAILABLE,
            "running": scheduler.running if scheduler else False,
            **scheduler_state
        }
    }

@app.post("/api/scheduler/pause")
async def pause_scheduler():
    """Pause the background scheduler"""
    global scheduler
    if not scheduler:
        return {"status": "error", "message": "Scheduler not available"}
    
    try:
        scheduler.pause()
        scheduler_state["enabled"] = False
        logger.info("⏸ Scheduler paused")
        return {"status": "success", "message": "Scheduler paused"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/scheduler/resume")
async def resume_scheduler():
    """Resume the background scheduler"""
    global scheduler
    if not scheduler:
        return {"status": "error", "message": "Scheduler not available"}
    
    try:
        scheduler.resume()
        scheduler_state["enabled"] = True
        update_scheduler_next_runs()
        logger.info("▶ Scheduler resumed")
        return {"status": "success", "message": "Scheduler resumed"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/scheduler/trigger-change-detection")
async def trigger_change_detection(background_tasks: BackgroundTasks):
    """Manually trigger change detection"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    background_tasks.add_task(run_change_detection)
    return {"status": "success", "message": "Change detection triggered"}

@app.post("/api/scheduler/trigger-reconciliation")
async def trigger_reconciliation(background_tasks: BackgroundTasks):
    """Manually trigger daily reconciliation"""
    global sync_engine
    if not sync_engine:
        return {"status": "error", "message": "Sync Engine not configured"}
    
    background_tasks.add_task(run_daily_reconciliation)
    return {"status": "success", "message": "Reconciliation triggered"}

@app.get("/api/changes/pending")
async def get_pending_changes():
    """Get pending changes that haven't been synced yet"""
    global db
    if not db:
        return {"status": "error", "message": "Database not available"}
    
    try:
        pending = db.get_pending_changes()
        return {"status": "success", "count": len(pending), "changes": pending}
    except Exception as e:
        logger.error(f"Failed to get pending changes: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/reconciliation/last-report")
async def get_last_reconciliation_report():
    """Get the last reconciliation report"""
    global db
    if not db:
        return {"status": "error", "message": "Database not available"}
    
    try:
        report = db.get_last_reconciliation_report()
        return {"status": "success", "report": report}
    except Exception as e:
        logger.error(f"Failed to get reconciliation report: {e}")
        return {"status": "error", "message": str(e)}

# ============================================
# WEBHOOK ENDPOINTS (Optional - for cloud deployment)
# ============================================

@app.post("/api/webhook/wrike")
async def wrike_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Webhook endpoint for Wrike task change notifications.
    Wrike sends POST when tasks are created/updated in subscribed folders.
    """
    global sync_engine, scheduler_state
    
    if not scheduler_state.get("webhooks_enabled", False):
        return JSONResponse(
            status_code=403,
            content={"status": "error", "message": "Webhooks are disabled"}
        )
    
    if not sync_engine:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": "Sync engine not available"}
        )
    
    try:
        body = await request.json()
        logger.info(f"📥 Received Wrike webhook: {body}")
        
        # Extract task IDs from webhook payload
        # Wrike webhook format: {"taskId": "...", "event": "TaskUpdated", ...}
        task_id = body.get("taskId")
        event_type = body.get("event", "unknown")
        
        if task_id:
            # Queue sync for this specific task
            background_tasks.add_task(
                sync_engine.sync_single_record, "wrike", task_id
            )
            logger.info(f"✓ Queued sync for Wrike task {task_id} (event: {event_type})")
            return {"status": "success", "message": f"Sync queued for task {task_id}"}
        
        return {"status": "ignored", "message": "No task ID in payload"}
        
    except Exception as e:
        logger.error(f"Wrike webhook error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

@app.post("/api/webhook/hubspot")
async def hubspot_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Webhook endpoint for HubSpot company change notifications.
    HubSpot sends POST when company records are created/updated.
    """
    global sync_engine, scheduler_state
    
    if not scheduler_state.get("webhooks_enabled", False):
        return JSONResponse(
            status_code=403,
            content={"status": "error", "message": "Webhooks are disabled"}
        )
    
    if not sync_engine:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": "Sync engine not available"}
        )
    
    try:
        body = await request.json()
        logger.info(f"📥 Received HubSpot webhook: {body}")
        
        # HubSpot webhook format varies; this handles the common formats
        # Format 1: [{"objectId": 123, "subscriptionType": "company.propertyChange", ...}]
        # Format 2: {"objectId": 123, "subscriptionType": "company.creation", ...}
        
        events = body if isinstance(body, list) else [body]
        queued_count = 0
        
        for event in events:
            object_id = str(event.get("objectId", ""))
            subscription_type = event.get("subscriptionType", "unknown")
            
            if object_id and "company" in subscription_type.lower():
                # Queue sync for this specific company
                background_tasks.add_task(
                    sync_engine.sync_single_record, "hubspot", object_id
                )
                queued_count += 1
                logger.info(f"✓ Queued sync for HubSpot company {object_id} (event: {subscription_type})")
        
        if queued_count > 0:
            return {"status": "success", "message": f"Sync queued for {queued_count} companies"}
        
        return {"status": "ignored", "message": "No company events in payload"}
        
    except Exception as e:
        logger.error(f"HubSpot webhook error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

# Create a simple startup script
def start_server():
    """Start the server manually"""
    import uvicorn
    print("🦅 Starting FalconHub Dashboard (Falcon-HubSpot Middleware)...")
    print("🌐 Dashboard will be available at: http://localhost:8004")
    print("📖 API Documentation at: http://localhost:8004/docs")
    print("📋 Following Manual Protocol for Falcon <-> HubSpot Communication")
    print("Press Ctrl+C to stop the server")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8004, reload=True)
    except KeyboardInterrupt:
        print("\n👋 Server stopped")

if __name__ == "__main__":
    start_server()
