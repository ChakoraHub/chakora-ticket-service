import pytest
from fastapi.testclient import TestClient
from ticket_service import app


@pytest.fixture(scope="module")
def client():
    # Avoid context manager entry to bypass remote Oracle network startup delay during local unit test
    return TestClient(app, raise_server_exceptions=False)


def test_prometheus_metrics_endpoint_status(client):
    """Verify that /metrics endpoint returns HTTP 200 OK."""
    response = client.get("/metrics")
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}"


def test_prometheus_metrics_content_type(client):
    """Verify standard Prometheus exposition content type header."""
    response = client.get("/metrics")
    content_type = response.headers.get("content-type", "")
    assert "text/plain" in content_type, f"Unexpected Content-Type: {content_type}"


def test_prometheus_metrics_expose_expected_metrics(client):
    """Verify presence of core Prometheus metrics in the output payload."""
    client.get("/openapi.json")
    response = client.get("/metrics")
    body = response.text

    assert response.status_code == 200
    # Core Python process metrics
    assert "python_gc_objects_collected_total" in body or "process_cpu_seconds_total" in body
    # Standard Prometheus metric definitions
    assert "# HELP" in body
    assert "# TYPE" in body
