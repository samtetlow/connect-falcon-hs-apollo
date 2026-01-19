"""
Tests for the Connect Falcon-HS-Apollo application
"""

import pytest
from fastapi.testclient import TestClient
from app import app


@pytest.fixture
def client():
    """Test client fixture"""
    return TestClient(app)


def test_health_check(client):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "version" in data


def test_root_endpoint(client):
    """Test root dashboard endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert "Connect Falcon-HS-Apollo" in response.text
    assert "text/html" in response.headers["content-type"]


def test_sync_falcon_to_apollo(client):
    """Test Falcon to Apollo sync endpoint"""
    payload = {
        "integration_id": "test-123",
        "direction": "falcon-to-apollo",
        "data": {"test": "data"}
    }
    response = client.post("/sync/falcon-to-apollo", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["integration_id"] == "test-123"
    assert data["status"] == "queued"


def test_sync_apollo_to_falcon(client):
    """Test Apollo to Falcon sync endpoint"""
    payload = {
        "integration_id": "test-456",
        "direction": "apollo-to-falcon",
        "data": {"test": "data"}
    }
    response = client.post("/sync/apollo-to-falcon", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["integration_id"] == "test-456"
    assert data["status"] == "queued"


def test_get_integration_status(client):
    """Test integration status endpoint"""
    response = client.get("/status/test-123")
    assert response.status_code == 200
    data = response.json()
    assert data["integration_id"] == "test-123"
    assert "status" in data


