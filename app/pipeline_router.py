from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import logging
import os
from typing import Optional

from app.utils import run_pipeline_step, pipeline_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pipeline_router = APIRouter(prefix="/api/pipeline", tags=["Pipeline"])


class PipelineResponse(BaseModel):
    """Response model for pipeline operations."""

    step: str
    status: str
    message: str
    started_at: Optional[str] = None


class PipelineStatusResponse(BaseModel):
    """Response model for pipeline status."""

    generate: dict
    process: dict
    load: dict


@pipeline_router.get("/status", response_model=PipelineStatusResponse)
async def get_pipeline_status():
    """
    Get the current status of all pipeline operations.

    Returns:
        Current status of generate, process, and load operations
    """
    return pipeline_status


@pipeline_router.get("/debug")
async def debug_info():
    """
    Get debug information about the pipeline environment.

    Returns:
        Environment information for debugging
    """
    import sys

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    scripts = {
        "generate": "pipeline/generate_sensor_data.py",
        "process": "pipeline/process_sensor_data.py",
        "load": "pipeline/load_to_database.py",
    }

    script_status = {}
    for name, path in scripts.items():
        full_path = os.path.join(project_root, path)
        script_status[name] = {"path": full_path, "exists": os.path.exists(full_path)}

    return {
        "python_executable": sys.executable,
        "project_root": project_root,
        "current_dir": os.getcwd(),
        "scripts": script_status,
        "environment": {
            "PATH": os.environ.get("PATH", ""),
            "VIRTUAL_ENV": os.environ.get("VIRTUAL_ENV", "Not set"),
        },
    }


@pipeline_router.post("/generate", response_model=PipelineResponse)
async def generate_data(num_rows: int = 100000):
    """Generate sensor data with specified number of rows."""
    if pipeline_status["generate"]["running"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Data generation is already running",
        )

    run_pipeline_step("generate", "pipeline/generate_sensor_data.py", [str(num_rows)])

    status_data = pipeline_status["generate"]
    if status_data["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=status_data["message"],
        )

    return {
        "step": "generate",
        "status": status_data["status"],
        "message": status_data["message"],
        "started_at": status_data["last_run"],
    }


@pipeline_router.post("/process", response_model=PipelineResponse)
async def process_data():
    """Process raw sensor data."""
    if pipeline_status["process"]["running"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Data processing is already running",
        )

    if not os.path.exists("data/raw/sensor_data.csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Raw sensor data not found. Please run data generation first.",
        )

    run_pipeline_step("process", "pipeline/process_sensor_data.py")

    status_data = pipeline_status["process"]
    if status_data["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=status_data["message"],
        )

    return {
        "step": "process",
        "status": status_data["status"],
        "message": status_data["message"],
        "started_at": status_data["last_run"],
    }


@pipeline_router.post("/load", response_model=PipelineResponse)
async def load_data():
    """Load processed data to database."""
    if pipeline_status["load"]["running"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Data loading is already running",
        )

    if not os.path.exists("data/processed/sensor_data_main.parquet"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Processed data not found. Please run data processing first.",
        )

    run_pipeline_step("load", "pipeline/load_to_database.py")

    status_data = pipeline_status["load"]
    if status_data["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=status_data["message"],
        )

    return {
        "step": "load",
        "status": status_data["status"],
        "message": status_data["message"],
        "started_at": status_data["last_run"],
    }


@pipeline_router.delete("/data")
async def clear_data():
    """
    Clear all generated and processed data files and drop the database table.

    Warning: This will delete all CSV and Parquet files, and drop the sensor_data_main table.
    """
    try:
        import shutil
        from sqlalchemy import text
        from app.database import engine

        deleted_files = []
        actions = []

        if os.path.exists("data/raw/sensor_data.csv"):
            os.remove("data/raw/sensor_data.csv")
            deleted_files.append("data/raw/sensor_data.csv")
            actions.append("Deleted raw CSV file")

        if os.path.exists("data/processed/sensor_data_main.parquet"):
            shutil.rmtree("data/processed/sensor_data_main.parquet")
            deleted_files.append("data/processed/sensor_data_main.parquet")
            actions.append("Deleted processed Parquet files")

        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sensor_data_main CASCADE"))
                conn.commit()
                actions.append("Dropped database table: sensor_data_main")
        except Exception as db_error:
            logger.warning(f"Error dropping database table: {str(db_error)}")
            actions.append(f"Database table drop failed: {str(db_error)}")

        for step in pipeline_status:
            pipeline_status[step] = {
                "running": False,
                "last_run": None,
                "status": "idle",
                "message": "",
            }

        return {
            "status": "success",
            "message": "Data files and database table cleared successfully",
            "deleted_files": deleted_files,
            "actions": actions,
        }

    except Exception as e:
        logger.error(f"Error clearing data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clearing data: {str(e)}",
        )
