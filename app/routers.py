from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
import logging

from app.database import get_db
from app.models import SensorIDResponse, SensorDataDetail, SensorSummary, HealthResponse
from app.sql_scripts import table_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_router = APIRouter(prefix="/api")


@api_router.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint to verify API and database connectivity.

    Returns:
        Health status of API and database
    """
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        db_status = "disconnected"
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed",
        )

    return {"status": "healthy", "database": db_status}


@api_router.get(
    "/sensors",
    response_model=List[SensorIDResponse],
    tags=["Sensors"],
    summary="Get list of all sensor IDs",
)
async def get_sensor_ids(db: Session = Depends(get_db)):
    """
    Get a list of all unique sensor IDs.

    Args:
        db: Database session

    Returns:
        List of unique sensor IDs with their reading counts
    """
    try:
        query = """
            SELECT 
                "Sensor_ID",
                MIN("Location") as "Location",
                COUNT(*) as total_readings
            FROM sensor_data_main
            GROUP BY "Sensor_ID"
            ORDER BY "Sensor_ID"
        """

        result = db.execute(text(query))

        sensors = []
        for row in result:
            sensors.append(
                {"Sensor_ID": row[0], "Location": row[1], "total_readings": row[2]}
            )

        if not sensors:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No sensors found"
            )

        return sensors

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sensor IDs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching sensor data: {str(e)}",
        )


@api_router.get(
    "/sensors/{sensor_id}/data",
    response_model=List[SensorDataDetail],
    tags=["Sensors"],
    summary="Get all data for a specific sensor",
)
async def get_sensor_data(
    sensor_id: str,
    limit: int = Query(
        1000, ge=1, le=10000, description="Maximum number of records to return"
    ),
    db: Session = Depends(get_db),
):
    """
    Get all processed data for a specific sensor ID from the pivoted table.

    Args:
        sensor_id: Unique sensor identifier
        limit: Maximum number of records to return (default: 1000, max: 10000)
        db: Database session

    Returns:
        List of sensor data records with all features (pivoted structure)
    """
    try:
        query = table_query + """
            ORDER BY ts_date ASC
            LIMIT :limit
        """

        params = {"sensor_id": sensor_id, "limit": limit}

        result = db.execute(text(query), params)
        columns = result.keys()

        data = []
        for row in result:
            row_dict = dict(zip(columns, row))
            data.append(row_dict)

        if not data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data found for sensor {sensor_id}",
            )

        return data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sensor data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching sensor data: {str(e)}",
        )


@api_router.get(
    "/sensors/{sensor_id}/summary",
    response_model=SensorSummary,
    tags=["Sensors"],
    summary="Get summary statistics for a sensor",
)
async def get_sensor_summary(sensor_id: str, db: Session = Depends(get_db)):
    """
    Get aggregated summary statistics for a specific sensor from the pivoted table.

    Args:
        sensor_id: Unique sensor identifier
        db: Database session

    Returns:
        Summary statistics including readings count, date ranges, quality metrics
    """
    try:
        query = """
            WITH most_common_location AS (
                SELECT "Location"
                FROM sensor_data_main
                WHERE "Sensor_ID" = :sensor_id
                GROUP BY "Location"
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            SELECT 
                "Sensor_ID",
                (SELECT "Location" FROM most_common_location) as "Location",
                COUNT(*) as days_of_data,
                MIN(ts_date) as first_reading,
                MAX(ts_date) as last_reading,
                MAX(total_readings) as total_readings,
                MAX(total_bad) as total_bad,
                ROUND(MAX(total_pct_bad)::numeric * 100, 2) as total_pct_bad,
                MAX(days_active) as days_active,
                SUM(any_bad) as bad_days_count,
                ROUND(AVG(any_bad)::numeric * 100, 2) as pct_bad_days,
                COUNT("Temperature") as has_temperature,
                COUNT("Humidity") as has_humidity,
                COUNT("Pressure") as has_pressure,
                COUNT("Vibration") as has_vibration,
                COUNT("Noise") as has_noise,
                COUNT("Current") as has_current,
                COUNT("Offset") as has_offset,
                ROUND(AVG("Temperature")::numeric, 2) as avg_temperature,
                ROUND(AVG("Humidity")::numeric, 2) as avg_humidity,
                ROUND(AVG("Pressure")::numeric, 2) as avg_pressure,
                ROUND(AVG("Vibration")::numeric, 2) as avg_vibration,
                ROUND(AVG("Noise")::numeric, 2) as avg_noise,
                ROUND(AVG("Current")::numeric, 2) as avg_current,
                ROUND(AVG("Offset")::numeric, 2) as avg_offset
            FROM sensor_data_main
            WHERE "Sensor_ID" = :sensor_id
            GROUP BY "Sensor_ID"
        """

        result = db.execute(text(query), {"sensor_id": sensor_id})
        row = result.fetchone()

        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sensor {sensor_id} not found",
            )

        parameters = []
        param_names = [
            "Temperature",
            "Humidity",
            "Pressure",
            "Vibration",
            "Noise",
            "Current",
            "Offset",
        ]
        param_counts = [row[11], row[12], row[13], row[14], row[15], row[16], row[17]]

        for name, count in zip(param_names, param_counts):
            if count > 0:
                parameters.append(name)

        avg_values = {}
        param_avgs = [row[18], row[19], row[20], row[21], row[22], row[23], row[24]]
        for name, avg in zip(param_names, param_avgs):
            if avg is not None:
                avg_values[name.lower()] = float(avg)

        return {
            "Sensor_ID": row[0],
            "Location": row[1],
            "days_of_data": row[2],
            "first_reading": row[3],
            "last_reading": row[4],
            "total_readings": row[5],
            "total_bad": row[6],
            "total_pct_bad": float(row[7]) if row[7] else 0.0,
            "days_active": row[8],
            "bad_days_count": row[9],
            "pct_bad_days": float(row[10]) if row[10] else 0.0,
            "parameters": parameters,
            "avg_values": avg_values,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sensor summary: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching sensor summary: {str(e)}",
        )
