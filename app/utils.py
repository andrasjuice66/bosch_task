import os
import sys
import subprocess
import logging
from typing import Optional
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pipeline_status = {
    "generate": {"running": False, "last_run": None, "status": "idle", "message": ""},
    "process": {"running": False, "last_run": None, "status": "idle", "message": ""},
    "load": {"running": False, "last_run": None, "status": "idle", "message": ""},
}


def run_pipeline_step(
    step: str, script_path: str, script_args: Optional[list] = None
) -> None:
    """
    Run a pipeline step as a subprocess.

    Args:
        step: Pipeline step name (generate, process, load)
        script_path: Path to the Python script
        script_args: Optional list of arguments to pass to the script
    """
    global pipeline_status

    try:
        pipeline_status[step]["running"] = True
        pipeline_status[step]["status"] = "running"
        pipeline_status[step]["message"] = f"Running {step} step..."
        pipeline_status[step]["last_run"] = datetime.now().isoformat()

        logger.info(f"Starting {step} step: {script_path}")

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        script_absolute_path = os.path.join(project_root, script_path)

        python_executable = sys.executable

        logger.info(f"Project root: {project_root}")
        logger.info(f"Script path: {script_absolute_path}")
        logger.info(f"Python executable: {python_executable}")

        if not os.path.exists(script_absolute_path):
            raise FileNotFoundError(f"Script not found: {script_absolute_path}")

        command = [python_executable, script_absolute_path]
        if script_args:
            command.extend(script_args)

        logger.info(f"Running command: {' '.join(command)}")

        result = subprocess.run(
            command, cwd=project_root, capture_output=True, text=True, timeout=600
        )

        if result.returncode == 0:
            pipeline_status[step]["status"] = "success"
            pipeline_status[step][
                "message"
            ] = f"{step.capitalize()} completed successfully"
            logger.info(f"{step} step completed successfully")
            logger.info(f"Output: {result.stdout[:500]}")
        else:
            error_msg = result.stderr if result.stderr else result.stdout
            pipeline_status[step]["status"] = "error"
            pipeline_status[step]["message"] = f"Error: {error_msg[:200]}"
            logger.error(f"{step} step failed with return code {result.returncode}")
            logger.error(f"STDERR: {result.stderr}")
            logger.error(f"STDOUT: {result.stdout}")

    except subprocess.TimeoutExpired:
        pipeline_status[step]["status"] = "error"
        pipeline_status[step]["message"] = "Operation timed out (10 minutes exceeded)"
        logger.error(f"{step} step timed out")

    except FileNotFoundError as e:
        pipeline_status[step]["status"] = "error"
        pipeline_status[step]["message"] = f"Script not found: {str(e)}"
        logger.error(f"File not found in {step} step: {str(e)}")

    except Exception as e:
        pipeline_status[step]["status"] = "error"
        pipeline_status[step]["message"] = f"Error: {str(e)}"
        logger.error(f"Error in {step} step: {str(e)}", exc_info=True)

    finally:
        pipeline_status[step]["running"] = False
