# 🦅 Connect Falcon-HubSpot

**Integration between Falcon (Wrike) and HubSpot workflows**

---

## 📋 Overview

This project provides seamless integration between:

- **Falcon (Wrike)**: Project management and task tracking
- **HubSpot**: CRM and deal management
- **Automated Workflows**: Grant Engine's 7-step handoff process

## 🎨 Dashboard

The integration includes a beautiful, modern dashboard with:

- **Real-time Status Monitoring**: Live system health and integration status
- **Interactive Controls**: One-click sync operations and configuration verification
- **Activity Log**: Real-time logging with color-coded severity levels
- **Responsive Design**: Works perfectly on desktop, tablet, and mobile
- **Modern UI**: Gradient backgrounds, smooth animations, and professional typography

**Access the dashboard at**: `http://localhost:8080` (when server is running)

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Git
- Access to Falcon-HS-Interconnect system

### Installation
```bash
# Clone the repository
git clone https://github.com/samtetlow/connect-falcon-hs-apollo.git
cd connect-falcon-hs-apollo

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure settings
cp config.json.example config.json
# Edit config.json with your API keys
```

### Running
```bash
# Option 1: Use the dashboard launcher (recommended)
python start_dashboard.py

# Option 2: Run directly with Python
python app.py

# Option 3: Use uvicorn directly
uvicorn app:app --reload --port 8080

# Access the dashboard at: http://localhost:8080
```

## 📁 Project Structure

```
connect-falcon-hubspot/
├── app.py                    # Main FastAPI application
├── wrike_client.py           # Falcon integration client
├── hubspot_client.py         # HubSpot integration client
├── sync_engine.py            # Enhanced sync engine with database
├── config.json              # Basic API credentials (optional)
├── config.yaml              # Advanced configuration (recommended)
├── config.json.example      # Basic config template
├── config.yaml.example      # Advanced config template
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── sync.db                 # SQLite database (auto-created)
├── logs/                   # Log files directory (auto-created)
└── tests/                  # Integration tests
```

## 🔧 Configuration

## 🔧 Enhanced Configuration

For the full-featured sync engine, create a `config.yaml` file (see `config.yaml.example`):

### Basic Configuration (JSON)
Create a `config.json` file with your API credentials:

```json
{
  "wrike": {
    "api_token": "your-falcon-permanent-access-token",
    "base_url": "https://www.wrike.com/api/v4"
  },
  "hubspot": {
    "api_key": "your-hubspot-api-key"
  }
}
```

### Advanced Configuration (YAML)
For the enhanced sync engine with full features:

```yaml
environment: development
hubspot:
  api_key: "your-hubspot-api-key"
  company_properties:
    name: "name"
    account_status: "account_status"
  contact_properties:
    first_name: "firstname"
    last_name: "lastname"
    email: "email"
falcon:
  api_token: "your-falcon-token"
  companies_folder_id: "your-companies-folder"
  contacts_folder_id: "your-contacts-folder"
  company_custom_fields:
    account_status: "field-id-1"
sync:
  polling_interval_seconds: 300
  create_missing_companies_in_hubspot: true
```

## 🤝 Integration Points

### Current Scope
- [x] Falcon folder/project management
- [x] Falcon task creation and updates
- [x] HubSpot deal and contact management
- [x] Enhanced sync engine with SQLite database
- [x] Configuration validation and verification
- [x] Comprehensive logging and error handling
- [x] Bidirectional sync with reconciliation
- [x] Automated workflow triggers (Falcon ↔ HubSpot)
- [x] Rate limiting and retry logic
- [x] **Beautiful modern dashboard UI**
- [x] Real-time activity monitoring
- [x] Interactive status indicators
- [x] Responsive mobile design
- [ ] Webhook support (planned)

### API Endpoints
- `GET /health` - Health check
- `POST /api/sync/run` - Run complete sync cycle (enhanced engine)
- `GET /api/falcon/status` - Check Falcon client status
- `GET /api/hubspot/status` - Check HubSpot client status
- `GET /api/reconciliation/issues` - Get unresolved reconciliation issues
- `GET /api/reconciliation/report` - Export and download reconciliation report

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=. --cov-report=html
```

## 📝 Development

### Adding New Integration Points
1. Define the data flow in `integration_flows.py`
2. Implement client methods in respective client files
3. Add API endpoints in `app.py`
4. Write tests for new functionality

### Code Standards
- Use type hints
- Follow PEP 8
- Add docstrings
- Write tests for new features

## 🔐 Security

- Never commit API keys to version control
- Use environment variables in production
- Rotate keys regularly
- Implement proper authentication

---

**Built for Grant Engine**  
*Connecting health systems since 2025* 🏥
