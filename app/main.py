from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import logging

from app.routers import api_router
from app.pipeline_router import pipeline_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_application() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance
    """
    app = FastAPI(
        title="Sensor Data API",
        description="API for querying processed sensor data from Bosch manufacturing facilities",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    app.include_router(api_router)
    app.include_router(pipeline_router)

    register_exception_handlers(app)

    @app.get("/", tags=["Frontend"])
    async def read_root():
        """Redirect to the frontend application."""
        return RedirectResponse(url="/static/index.html")

    return app


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register custom exception handlers for the application.

    Args:
        app: FastAPI application instance
    """

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request, exc):
        """Custom exception handler for HTTP exceptions."""
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.detail, "status_code": exc.status_code},
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request, exc):
        """Custom exception handler for general exceptions."""
        logger.error(f"Unexpected error: {str(exc)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal server error", "detail": str(exc)},
        )


app = create_application()
