import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from app.main import app
from app.pipeline_router import pipeline_status


@pytest.fixture
def client():
    """Create a test client."""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def reset_pipeline_status():
    """Reset pipeline status before each test."""
    global pipeline_status
    for step in pipeline_status:
        pipeline_status[step] = {
            "running": False,
            "last_run": None,
            "status": "idle",
            "message": "",
        }
    yield
    for step in pipeline_status:
        pipeline_status[step] = {
            "running": False,
            "last_run": None,
            "status": "idle",
            "message": "",
        }


class TestPipelineStatusEndpoint:
    """Tests for the /api/pipeline/status endpoint."""

    def test_get_pipeline_status_initial(self, client):
        """Test getting pipeline status when nothing has run yet."""
        response = client.get("/api/pipeline/status")

        assert response.status_code == 200
        data = response.json()
        assert "generate" in data
        assert "process" in data
        assert "load" in data
        assert data["generate"]["status"] == "idle"
        assert data["generate"]["running"] is False
        assert data["process"]["status"] == "idle"
        assert data["load"]["status"] == "idle"

    def test_get_pipeline_status_after_run(self, client):
        """Test getting pipeline status after setting a status."""
        from app.pipeline_router import pipeline_status

        pipeline_status["generate"]["status"] = "success"
        pipeline_status["generate"]["message"] = "Generation completed"
        pipeline_status["generate"]["last_run"] = "2024-01-01T10:00:00"

        response = client.get("/api/pipeline/status")

        assert response.status_code == 200
        data = response.json()
        assert data["generate"]["status"] == "success"
        assert data["generate"]["message"] == "Generation completed"


class TestPipelineDebugEndpoint:
    """Tests for the /api/pipeline/debug endpoint."""

    def test_get_debug_info(self, client):
        """Test getting debug information."""
        response = client.get("/api/pipeline/debug")

        assert response.status_code == 200
        data = response.json()
        assert "python_executable" in data
        assert "project_root" in data
        assert "current_dir" in data
        assert "scripts" in data
        assert "environment" in data

        assert "generate" in data["scripts"]
        assert "process" in data["scripts"]
        assert "load" in data["scripts"]
        assert "path" in data["scripts"]["generate"]
        assert "exists" in data["scripts"]["generate"]


class TestPipelineGenerateEndpoint:
    """Tests for the /api/pipeline/generate endpoint."""

    @patch("app.pipeline_router.run_pipeline_step")
    def test_generate_data_success(self, mock_run_step, client):
        """Test successful data generation."""

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "success"
            pipeline_status[step]["message"] = "Generate completed successfully"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/generate?num_rows=1000")

        assert response.status_code == 200
        data = response.json()
        assert data["step"] == "generate"
        assert data["status"] == "success"
        assert "success" in data["message"].lower()

        mock_run_step.assert_called_once_with(
            "generate", "pipeline/generate_sensor_data.py", ["1000"]
        )

    @patch("app.pipeline_router.run_pipeline_step")
    def test_generate_data_default_rows(self, mock_run_step, client):
        """Test data generation with default number of rows."""

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "success"
            pipeline_status[step]["message"] = "Generate completed successfully"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/generate")

        assert response.status_code == 200
        mock_run_step.assert_called_once()
        args = mock_run_step.call_args[0]
        assert args[2] == ["100000"]

    @patch("app.pipeline_router.run_pipeline_step")
    def test_generate_data_already_running(self, mock_run_step, client):
        """Test error when generation is already running."""
        from app.pipeline_router import pipeline_status

        pipeline_status["generate"]["running"] = True

        response = client.post("/api/pipeline/generate?num_rows=1000")

        assert response.status_code == 409
        data = response.json()
        assert "error" in data
        assert "already running" in data["error"].lower()

        mock_run_step.assert_not_called()

    @patch("app.pipeline_router.run_pipeline_step")
    def test_generate_data_error(self, mock_run_step, client):
        """Test error handling during data generation."""

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "error"
            pipeline_status[step]["message"] = "Script execution failed"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/generate?num_rows=1000")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestPipelineProcessEndpoint:
    """Tests for the /api/pipeline/process endpoint."""

    @patch("os.path.exists")
    @patch("app.pipeline_router.run_pipeline_step")
    def test_process_data_success(self, mock_run_step, mock_exists, client):
        """Test successful data processing."""
        mock_exists.return_value = True

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "success"
            pipeline_status[step]["message"] = "Process completed successfully"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/process")

        assert response.status_code == 200
        data = response.json()
        assert data["step"] == "process"
        assert data["status"] == "success"

        mock_run_step.assert_called_once_with(
            "process", "pipeline/process_sensor_data.py"
        )

    @patch("app.pipeline_router.run_pipeline_step")
    def test_process_data_already_running(self, mock_run_step, client):
        """Test error when processing is already running."""
        from app.pipeline_router import pipeline_status

        pipeline_status["process"]["running"] = True

        response = client.post("/api/pipeline/process")

        assert response.status_code == 409
        data = response.json()
        assert "error" in data
        assert "already running" in data["error"].lower()

        mock_run_step.assert_not_called()

    @patch("os.path.exists")
    def test_process_data_no_raw_data(self, mock_exists, client):
        """Test error when raw data file doesn't exist."""
        mock_exists.return_value = False

        response = client.post("/api/pipeline/process")

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "not found" in data["error"].lower()

    @patch("os.path.exists")
    @patch("app.pipeline_router.run_pipeline_step")
    def test_process_data_error(self, mock_run_step, mock_exists, client):
        """Test error handling during data processing."""
        mock_exists.return_value = True

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "error"
            pipeline_status[step]["message"] = "Processing failed"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/process")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestPipelineLoadEndpoint:
    """Tests for the /api/pipeline/load endpoint."""

    @patch("os.path.exists")
    @patch("app.pipeline_router.run_pipeline_step")
    def test_load_data_success(self, mock_run_step, mock_exists, client):
        """Test successful data loading."""
        mock_exists.return_value = True

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "success"
            pipeline_status[step]["message"] = "Load completed successfully"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/load")

        assert response.status_code == 200
        data = response.json()
        assert data["step"] == "load"
        assert data["status"] == "success"

        mock_run_step.assert_called_once_with("load", "pipeline/load_to_database.py")

    @patch("app.pipeline_router.run_pipeline_step")
    def test_load_data_already_running(self, mock_run_step, client):
        """Test error when loading is already running."""
        from app.pipeline_router import pipeline_status

        pipeline_status["load"]["running"] = True

        response = client.post("/api/pipeline/load")

        assert response.status_code == 409
        data = response.json()
        assert "error" in data
        assert "already running" in data["error"].lower()

        mock_run_step.assert_not_called()

    @patch("os.path.exists")
    def test_load_data_no_processed_data(self, mock_exists, client):
        """Test error when processed data doesn't exist."""
        mock_exists.return_value = False

        response = client.post("/api/pipeline/load")

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "not found" in data["error"].lower()

    @patch("os.path.exists")
    @patch("app.pipeline_router.run_pipeline_step")
    def test_load_data_error(self, mock_run_step, mock_exists, client):
        """Test error handling during data loading."""
        mock_exists.return_value = True

        def side_effect(step, script_path, script_args=None):
            from app.pipeline_router import pipeline_status

            pipeline_status[step]["status"] = "error"
            pipeline_status[step]["message"] = "Loading failed"
            pipeline_status[step]["last_run"] = datetime.now().isoformat()

        mock_run_step.side_effect = side_effect

        response = client.post("/api/pipeline/load")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestClearDataEndpoint:
    """Tests for the /api/pipeline/data DELETE endpoint."""

    @patch("app.database.engine")
    @patch("shutil.rmtree")
    @patch("os.remove")
    @patch("os.path.exists")
    def test_clear_data_all_files_exist(
        self, mock_exists, mock_remove, mock_rmtree, mock_engine, client
    ):
        """Test clearing data when all files exist."""
        mock_exists.return_value = True

        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        response = client.delete("/api/pipeline/data")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "deleted_files" in data
        assert "actions" in data
        assert len(data["deleted_files"]) == 2

        mock_remove.assert_called_once_with("data/raw/sensor_data.csv")
        mock_rmtree.assert_called_once_with("data/processed/sensor_data_main.parquet")

        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("app.database.engine")
    @patch("os.path.exists")
    def test_clear_data_no_files(self, mock_exists, mock_engine, client):
        """Test clearing data when no files exist."""
        mock_exists.return_value = False

        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        response = client.delete("/api/pipeline/data")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert len(data["deleted_files"]) == 0

    @patch("app.database.engine")
    @patch("shutil.rmtree")
    @patch("os.remove")
    @patch("os.path.exists")
    def test_clear_data_database_error(
        self, mock_exists, mock_remove, mock_rmtree, mock_engine, client
    ):
        """Test clearing data when database drop fails."""
        mock_exists.return_value = True

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database connection error")
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        response = client.delete("/api/pipeline/data")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "Database table drop failed" in " ".join(data["actions"])

    @patch("os.path.exists")
    @patch("os.remove")
    def test_clear_data_file_deletion_error(self, mock_remove, mock_exists, client):
        """Test error when file deletion fails."""
        mock_exists.return_value = True

        mock_remove.side_effect = Exception("Permission denied")

        response = client.delete("/api/pipeline/data")

        assert response.status_code == 500
        data = response.json()
        assert "error" in data


class TestRunPipelineStep:
    """Tests for the run_pipeline_step function."""

    @patch("subprocess.run")
    @patch("os.path.exists")
    def test_run_pipeline_step_success(self, mock_exists, mock_subprocess):
        """Test successful pipeline step execution."""
        from app.pipeline_router import run_pipeline_step, pipeline_status

        mock_exists.return_value = True

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Success output"
        mock_result.stderr = ""
        mock_subprocess.return_value = mock_result

        run_pipeline_step("generate", "pipeline/generate_sensor_data.py", ["1000"])

        assert pipeline_status["generate"]["status"] == "success"
        assert pipeline_status["generate"]["running"] is False
        assert (
            "completed successfully" in pipeline_status["generate"]["message"].lower()
        )

    @patch("subprocess.run")
    @patch("os.path.exists")
    def test_run_pipeline_step_script_not_found(self, mock_exists, mock_subprocess):
        """Test pipeline step when script doesn't exist."""
        from app.pipeline_router import run_pipeline_step, pipeline_status

        mock_exists.return_value = False

        run_pipeline_step("generate", "pipeline/nonexistent.py")

        assert pipeline_status["generate"]["status"] == "error"
        assert pipeline_status["generate"]["running"] is False
        assert "not found" in pipeline_status["generate"]["message"].lower()

    @patch("subprocess.run")
    @patch("os.path.exists")
    def test_run_pipeline_step_subprocess_error(self, mock_exists, mock_subprocess):
        """Test pipeline step when subprocess fails."""
        from app.pipeline_router import run_pipeline_step, pipeline_status

        mock_exists.return_value = True

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error message"
        mock_subprocess.return_value = mock_result

        run_pipeline_step("process", "pipeline/process_sensor_data.py")

        assert pipeline_status["process"]["status"] == "error"
        assert pipeline_status["process"]["running"] is False
        assert "Error" in pipeline_status["process"]["message"]

    @patch("subprocess.run")
    @patch("os.path.exists")
    def test_run_pipeline_step_timeout(self, mock_exists, mock_subprocess):
        """Test pipeline step timeout."""
        from app.pipeline_router import run_pipeline_step, pipeline_status
        import subprocess

        mock_exists.return_value = True

        mock_subprocess.side_effect = subprocess.TimeoutExpired("cmd", 600)

        run_pipeline_step("load", "pipeline/load_to_database.py")

        assert pipeline_status["load"]["status"] == "error"
        assert pipeline_status["load"]["running"] is False
        assert "timed out" in pipeline_status["load"]["message"].lower()
