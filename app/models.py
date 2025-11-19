from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import date


class SensorDataBase(BaseModel):
    """Base model for sensor data (deprecated - kept for backward compatibility)."""

    Sensor_ID: str = Field(..., description="Unique sensor identifier")
    Date: date = Field(..., description="Date of the reading")
    Location: str = Field(..., description="Location of the sensor")
    Parameter: str = Field(..., description="Type of parameter measured")
    Value: float = Field(..., description="Measured value")
    Status: str = Field(..., description="Status of the reading (Good/Bad)")


class SensorDataDetail(BaseModel):
    """Detailed sensor data from pivoted sensor_data_main table."""

    Sensor_ID: str = Field(..., description="Unique sensor identifier")
    ts_date: date = Field(..., description="Date of the reading")
    ts_month: Optional[date] = Field(None, description="Month of the reading")
    Location: str = Field(..., description="Location of the sensor")

    Current: Optional[float] = Field(None, description="Current value")
    Humidity: Optional[float] = Field(None, description="Humidity value")
    Noise: Optional[float] = Field(None, description="Noise value")
    Offset: Optional[float] = Field(None, description="Offset value")
    Pressure: Optional[float] = Field(None, description="Pressure value")
    Temperature: Optional[float] = Field(None, description="Temperature value")
    Vibration: Optional[float] = Field(None, description="Vibration value")

    any_bad: Optional[int] = Field(None, description="Any bad reading flag")
    good_streak_days: Optional[int] = Field(None, description="Good streak days")

    prev_current: Optional[float] = Field(None, description="Previous current value")
    delta_current: Optional[float] = Field(None, description="Delta current value")
    mean_7r_current: Optional[float] = Field(
        None, description="7-day rolling mean current"
    )

    prev_humidity: Optional[float] = Field(None, description="Previous humidity value")
    delta_humidity: Optional[float] = Field(None, description="Delta humidity value")
    mean_7r_humidity: Optional[float] = Field(
        None, description="7-day rolling mean humidity"
    )

    prev_noise: Optional[float] = Field(None, description="Previous noise value")
    delta_noise: Optional[float] = Field(None, description="Delta noise value")
    mean_7r_noise: Optional[float] = Field(None, description="7-day rolling mean noise")

    prev_offset: Optional[float] = Field(None, description="Previous offset value")
    delta_offset: Optional[float] = Field(None, description="Delta offset value")
    mean_7r_offset: Optional[float] = Field(
        None, description="7-day rolling mean offset"
    )

    prev_pressure: Optional[float] = Field(None, description="Previous pressure value")
    delta_pressure: Optional[float] = Field(None, description="Delta pressure value")
    mean_7r_pressure: Optional[float] = Field(
        None, description="7-day rolling mean pressure"
    )

    prev_temperature: Optional[float] = Field(
        None, description="Previous temperature value"
    )
    delta_temperature: Optional[float] = Field(
        None, description="Delta temperature value"
    )
    mean_7r_temperature: Optional[float] = Field(
        None, description="7-day rolling mean temperature"
    )

    prev_vibration: Optional[float] = Field(
        None, description="Previous vibration value"
    )
    delta_vibration: Optional[float] = Field(None, description="Delta vibration value")
    mean_7r_vibration: Optional[float] = Field(
        None, description="7-day rolling mean vibration"
    )

    mo_avg_Current: Optional[float] = Field(
        None, description="Monthly average current", alias="mo_avg_Current"
    )
    mo_std_Current: Optional[float] = Field(
        None, description="Monthly std current", alias="mo_std_Current"
    )
    mo_min_Current: Optional[float] = Field(
        None, description="Monthly min current", alias="mo_min_Current"
    )
    mo_max_Current: Optional[float] = Field(
        None, description="Monthly max current", alias="mo_max_Current"
    )
    mo_pct_bad_Current: Optional[float] = Field(
        None, description="Monthly pct bad current", alias="mo_pct_bad_Current"
    )
    mo_count_Current: Optional[int] = Field(
        None, description="Monthly count current", alias="mo_count_Current"
    )

    mo_avg_Humidity: Optional[float] = Field(
        None, description="Monthly average humidity", alias="mo_avg_Humidity"
    )
    mo_std_Humidity: Optional[float] = Field(
        None, description="Monthly std humidity", alias="mo_std_Humidity"
    )
    mo_min_Humidity: Optional[float] = Field(
        None, description="Monthly min humidity", alias="mo_min_Humidity"
    )
    mo_max_Humidity: Optional[float] = Field(
        None, description="Monthly max humidity", alias="mo_max_Humidity"
    )
    mo_pct_bad_Humidity: Optional[float] = Field(
        None, description="Monthly pct bad humidity", alias="mo_pct_bad_Humidity"
    )
    mo_count_Humidity: Optional[int] = Field(
        None, description="Monthly count humidity", alias="mo_count_Humidity"
    )

    mo_avg_Noise: Optional[float] = Field(
        None, description="Monthly average noise", alias="mo_avg_Noise"
    )
    mo_std_Noise: Optional[float] = Field(
        None, description="Monthly std noise", alias="mo_std_Noise"
    )
    mo_min_Noise: Optional[float] = Field(
        None, description="Monthly min noise", alias="mo_min_Noise"
    )
    mo_max_Noise: Optional[float] = Field(
        None, description="Monthly max noise", alias="mo_max_Noise"
    )
    mo_pct_bad_Noise: Optional[float] = Field(
        None, description="Monthly pct bad noise", alias="mo_pct_bad_Noise"
    )
    mo_count_Noise: Optional[int] = Field(
        None, description="Monthly count noise", alias="mo_count_Noise"
    )

    mo_avg_Offset: Optional[float] = Field(
        None, description="Monthly average offset", alias="mo_avg_Offset"
    )
    mo_std_Offset: Optional[float] = Field(
        None, description="Monthly std offset", alias="mo_std_Offset"
    )
    mo_min_Offset: Optional[float] = Field(
        None, description="Monthly min offset", alias="mo_min_Offset"
    )
    mo_max_Offset: Optional[float] = Field(
        None, description="Monthly max offset", alias="mo_max_Offset"
    )
    mo_pct_bad_Offset: Optional[float] = Field(
        None, description="Monthly pct bad offset", alias="mo_pct_bad_Offset"
    )
    mo_count_Offset: Optional[int] = Field(
        None, description="Monthly count offset", alias="mo_count_Offset"
    )

    mo_avg_Pressure: Optional[float] = Field(
        None, description="Monthly average pressure", alias="mo_avg_Pressure"
    )
    mo_std_Pressure: Optional[float] = Field(
        None, description="Monthly std pressure", alias="mo_std_Pressure"
    )
    mo_min_Pressure: Optional[float] = Field(
        None, description="Monthly min pressure", alias="mo_min_Pressure"
    )
    mo_max_Pressure: Optional[float] = Field(
        None, description="Monthly max pressure", alias="mo_max_Pressure"
    )
    mo_pct_bad_Pressure: Optional[float] = Field(
        None, description="Monthly pct bad pressure", alias="mo_pct_bad_Pressure"
    )
    mo_count_Pressure: Optional[int] = Field(
        None, description="Monthly count pressure", alias="mo_count_Pressure"
    )

    mo_avg_Temperature: Optional[float] = Field(
        None, description="Monthly average temperature", alias="mo_avg_Temperature"
    )
    mo_std_Temperature: Optional[float] = Field(
        None, description="Monthly std temperature", alias="mo_std_Temperature"
    )
    mo_min_Temperature: Optional[float] = Field(
        None, description="Monthly min temperature", alias="mo_min_Temperature"
    )
    mo_max_Temperature: Optional[float] = Field(
        None, description="Monthly max temperature", alias="mo_max_Temperature"
    )
    mo_pct_bad_Temperature: Optional[float] = Field(
        None, description="Monthly pct bad temperature", alias="mo_pct_bad_Temperature"
    )
    mo_count_Temperature: Optional[int] = Field(
        None, description="Monthly count temperature", alias="mo_count_Temperature"
    )

    mo_avg_Vibration: Optional[float] = Field(
        None, description="Monthly average vibration", alias="mo_avg_Vibration"
    )
    mo_std_Vibration: Optional[float] = Field(
        None, description="Monthly std vibration", alias="mo_std_Vibration"
    )
    mo_min_Vibration: Optional[float] = Field(
        None, description="Monthly min vibration", alias="mo_min_Vibration"
    )
    mo_max_Vibration: Optional[float] = Field(
        None, description="Monthly max vibration", alias="mo_max_Vibration"
    )
    mo_pct_bad_Vibration: Optional[float] = Field(
        None, description="Monthly pct bad vibration", alias="mo_pct_bad_Vibration"
    )
    mo_count_Vibration: Optional[int] = Field(
        None, description="Monthly count vibration", alias="mo_count_Vibration"
    )

    total_readings: Optional[int] = Field(None, description="Total readings")
    total_bad: Optional[int] = Field(None, description="Total bad readings")
    total_pct_bad: Optional[float] = Field(None, description="Total percentage bad")
    first_seen_date: Optional[date] = Field(None, description="First seen date")
    last_seen_date: Optional[date] = Field(None, description="Last seen date")
    days_active: Optional[int] = Field(None, description="Days active")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class SensorSummary(BaseModel):
    """Summary statistics for a sensor from sensor_data_main table."""

    Sensor_ID: str = Field(..., description="Unique sensor identifier")
    Location: str = Field(..., description="Location of the sensor")
    days_of_data: int = Field(..., description="Number of days with data")
    first_reading: date = Field(..., description="Date of first reading")
    last_reading: date = Field(..., description="Date of last reading")
    total_readings: int = Field(..., description="Total number of readings")
    total_bad: int = Field(..., description="Total bad readings")
    total_pct_bad: float = Field(..., description="Percentage of bad readings")
    days_active: int = Field(..., description="Days active")
    bad_days_count: int = Field(..., description="Number of days with bad readings")
    pct_bad_days: float = Field(..., description="Percentage of days with bad readings")
    parameters: List[str] = Field(
        ..., description="List of parameters measured by this sensor"
    )
    avg_values: dict = Field(..., description="Average values for each parameter")


class SensorIDResponse(BaseModel):
    """Response model for sensor ID list."""

    Sensor_ID: str = Field(..., description="Unique sensor identifier")
    Location: str = Field(..., description="Location of the sensor")
    total_readings: int = Field(..., description="Total number of readings")


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="API status")
    database: str = Field(..., description="Database connection status")
