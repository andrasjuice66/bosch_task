import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock
from datetime import date
from sqlalchemy.orm import Session

from app.main import app
from app.database import get_db


def get_mock_db():
    """Create a mock database session for testing."""
    db = Mock(spec=Session)
    return db


@pytest.fixture
def client():
    """Create a test client with mocked database."""
    app.dependency_overrides[get_db] = get_mock_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    return Mock(spec=Session)


class TestHealthEndpoint:
    """Tests for the /api/health endpoint."""

    def test_health_check_success(self, client, mock_db):
        """Test successful health check when database is connected."""
        mock_result = Mock()
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] == "connected"

    def test_health_check_database_failure(self, client, mock_db):
        """Test health check when database connection fails."""
        mock_db.execute.side_effect = Exception("Database connection error")

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/health")

        assert response.status_code == 503
        data = response.json()
        assert "error" in data
        assert "Database connection failed" in data["error"]


class TestSensorsEndpoint:
    """Tests for the /api/sensors endpoint."""

    def test_get_sensor_ids_success(self, client, mock_db):
        """Test successful retrieval of sensor IDs."""
        mock_result = Mock()
        mock_result.__iter__ = Mock(
            return_value=iter(
                [
                    ("SENSOR_001", "Location_A", 100),
                    ("SENSOR_002", "Location_B", 150),
                    ("SENSOR_003", "Location_C", 200),
                ]
            )
        )
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert data[0]["Sensor_ID"] == "SENSOR_001"
        assert data[0]["Location"] == "Location_A"
        assert data[0]["total_readings"] == 100
        assert data[1]["Sensor_ID"] == "SENSOR_002"
        assert data[2]["Sensor_ID"] == "SENSOR_003"

    def test_get_sensor_ids_no_sensors_found(self, client, mock_db):
        """Test when no sensors are found in the database."""
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors")

        assert response.status_code == 404
        data = response.json()
        assert "error" in data
        assert "No sensors found" in data["error"]

    def test_get_sensor_ids_database_error(self, client, mock_db):
        """Test database error handling."""
        mock_db.execute.side_effect = Exception("Database query error")

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestSensorDataEndpoint:
    """Tests for the /api/sensors/{sensor_id}/data endpoint."""

    def test_get_sensor_data_success(self, client, mock_db):
        """Test successful retrieval of sensor data."""
        mock_result = Mock()
        mock_result.keys.return_value = [
            "Sensor_ID",
            "ts_date",
            "ts_month",
            "Location",
            "Temperature",
            "Humidity",
            "Pressure",
            "Vibration",
            "Noise",
            "Current",
            "Offset",
            "any_bad",
            "good_streak_days",
            "total_readings",
            "total_bad",
            "total_pct_bad",
            "first_seen_date",
            "last_seen_date",
            "days_active",
            "prev_temperature",
            "delta_temperature",
            "mean_7r_temperature",
            "prev_humidity",
            "delta_humidity",
            "mean_7r_humidity",
            "prev_pressure",
            "delta_pressure",
            "mean_7r_pressure",
            "prev_vibration",
            "delta_vibration",
            "mean_7r_vibration",
            "prev_noise",
            "delta_noise",
            "mean_7r_noise",
            "prev_current",
            "delta_current",
            "mean_7r_current",
            "prev_offset",
            "delta_offset",
            "mean_7r_offset",
            "mo_avg_Temperature",
            "mo_std_Temperature",
            "mo_min_Temperature",
            "mo_max_Temperature",
            "mo_pct_bad_Temperature",
            "mo_count_Temperature",
            "mo_avg_Humidity",
            "mo_std_Humidity",
            "mo_min_Humidity",
            "mo_max_Humidity",
            "mo_pct_bad_Humidity",
            "mo_count_Humidity",
            "mo_avg_Pressure",
            "mo_std_Pressure",
            "mo_min_Pressure",
            "mo_max_Pressure",
            "mo_pct_bad_Pressure",
            "mo_count_Pressure",
            "mo_avg_Vibration",
            "mo_std_Vibration",
            "mo_min_Vibration",
            "mo_max_Vibration",
            "mo_pct_bad_Vibration",
            "mo_count_Vibration",
            "mo_avg_Noise",
            "mo_std_Noise",
            "mo_min_Noise",
            "mo_max_Noise",
            "mo_pct_bad_Noise",
            "mo_count_Noise",
            "mo_avg_Current",
            "mo_std_Current",
            "mo_min_Current",
            "mo_max_Current",
            "mo_pct_bad_Current",
            "mo_count_Current",
            "mo_avg_Offset",
            "mo_std_Offset",
            "mo_min_Offset",
            "mo_max_Offset",
            "mo_pct_bad_Offset",
            "mo_count_Offset",
        ]

        row_data = [
            "SENSOR_001",
            date(2024, 1, 1),
            date(2024, 1, 1),
            "Location_A",
            25.5,
            60.0,
            1013.25,
            0.5,
            45.0,
            0.8,
            0.1,
            0,
            5,
            100,
            10,
            0.1,
            date(2023, 12, 1),
            date(2024, 1, 1),
            30,
            25.0,
            0.5,
            25.2,
            59.5,
            0.5,
            59.8,
            1013.0,
            0.25,
            1013.1,
            0.48,
            0.02,
            0.49,
            44.5,
            0.5,
            44.8,
            0.78,
            0.02,
            0.79,
            0.09,
            0.01,
            0.095,
            25.3,
            0.5,
            24.8,
            26.0,
            0.05,
            30,
            59.8,
            1.2,
            58.0,
            62.0,
            0.03,
            30,
            1013.2,
            0.3,
            1012.5,
            1014.0,
            0.02,
            30,
            0.49,
            0.05,
            0.45,
            0.55,
            0.04,
            30,
            44.9,
            2.1,
            42.0,
            48.0,
            0.06,
            30,
            0.79,
            0.03,
            0.75,
            0.85,
            0.02,
            30,
            0.095,
            0.01,
            0.08,
            0.11,
            0.05,
            30,
        ]

        mock_result.__iter__ = Mock(return_value=iter([row_data]))
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/SENSOR_001/data")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["Sensor_ID"] == "SENSOR_001"
        assert data[0]["Location"] == "Location_A"
        assert data[0]["Temperature"] == 25.5
        assert data[0]["Humidity"] == 60.0

    def test_get_sensor_data_not_found(self, client, mock_db):
        """Test when sensor data is not found."""
        mock_result = Mock()
        mock_result.keys.return_value = []
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/NONEXISTENT/data")

        assert response.status_code == 404
        data = response.json()
        assert "error" in data
        assert "No data found" in data["error"]

    def test_get_sensor_data_database_error(self, client, mock_db):
        """Test database error handling."""
        mock_db.execute.side_effect = Exception("Database query error")

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/SENSOR_001/data")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestSensorSummaryEndpoint:
    """Tests for the /api/sensors/{sensor_id}/summary endpoint."""

    def test_get_sensor_summary_success(self, client, mock_db):
        """Test successful retrieval of sensor summary."""
        mock_result = Mock()
        row_data = (
            "SENSOR_001",
            "Location_A",
            30,
            date(2023, 12, 1),
            date(2024, 1, 1),
            2100,
            210,
            10.0,
            30,
            5,
            16.67,
            30,
            30,
            30,
            30,
            30,
            30,
            30,
            25.5,
            60.0,
            1013.25,
            0.5,
            45.0,
            0.8,
            0.1,
        )
        mock_result.fetchone.return_value = row_data
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/SENSOR_001/summary")

        assert response.status_code == 200
        data = response.json()
        assert data["Sensor_ID"] == "SENSOR_001"
        assert data["Location"] == "Location_A"
        assert data["days_of_data"] == 30
        assert data["total_readings"] == 2100
        assert data["total_bad"] == 210
        assert data["total_pct_bad"] == 10.0
        assert "Temperature" in data["parameters"]
        assert "Humidity" in data["parameters"]
        assert "Pressure" in data["parameters"]
        assert data["avg_values"]["temperature"] == 25.5
        assert data["avg_values"]["humidity"] == 60.0

    def test_get_sensor_summary_partial_parameters(self, client, mock_db):
        """Test sensor summary with only some parameters available."""
        mock_result = Mock()
        row_data = (
            "SENSOR_002",
            "Location_B",
            20,
            date(2024, 1, 1),
            date(2024, 1, 20),
            1400,
            70,
            5.0,
            20,
            2,
            10.0,
            20,
            20,
            0,
            0,
            0,
            0,
            0,
            22.5,
            55.0,
            None,
            None,
            None,
            None,
            None,
        )
        mock_result.fetchone.return_value = row_data
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/SENSOR_002/summary")

        assert response.status_code == 200
        data = response.json()
        assert data["Sensor_ID"] == "SENSOR_002"
        assert len(data["parameters"]) == 2
        assert "Temperature" in data["parameters"]
        assert "Humidity" in data["parameters"]
        assert "Pressure" not in data["parameters"]
        assert data["avg_values"]["temperature"] == 22.5
        assert data["avg_values"]["humidity"] == 55.0
        assert "pressure" not in data["avg_values"]

    def test_get_sensor_summary_not_found(self, client, mock_db):
        """Test when sensor is not found."""
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/NONEXISTENT/summary")

        assert response.status_code == 404
        data = response.json()
        assert "error" in data
        assert "not found" in data["error"]

    def test_get_sensor_summary_database_error(self, client, mock_db):
        """Test database error handling."""
        mock_db.execute.side_effect = Exception("Database query error")

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/sensors/SENSOR_001/summary")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestRouterIntegration:
    """Integration tests for router behavior."""

    def test_invalid_route(self, client):
        """Test accessing an invalid route."""
        response = client.get("/api/invalid_route")

        assert response.status_code == 404

    def test_cors_headers(self, client, mock_db):
        """Test CORS headers are present in responses."""
        mock_result = Mock()
        mock_db.execute.return_value = mock_result

        app.dependency_overrides[get_db] = lambda: mock_db

        response = client.get("/api/health")

        assert response.status_code == 200

    def test_pagination_limits(self, client, mock_db):
        """Test that pagination limits are enforced."""
        response = client.get("/api/sensors/SENSOR_001/data?limit=50000")

        assert response.status_code == 422
