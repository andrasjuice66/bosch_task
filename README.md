# Sensor Data Pipeline 

**Solution for Task 2 and 3.**

## Overview

This project implements an end-to-end sensor data pipeline with a web-based dashboard for monitoring and management.

### Pipeline Architecture

The pipeline follows a complete data flow:

```
Data Generation → Data Processing → Database Loading → REST API → Web Dashboard
```

**Pipeline Steps:**

1. **Data Generation** (`pipeline/generate_sensor_data.py`)
   - Generates synthetic sensor data for multiple sensors
   - Creates CSV files with timestamps, sensor readings, and metadata
   - Output: `data/raw/sensor_data.csv`

2. **Data Processing** (`pipeline/process_sensor_data.py`)
   - Processes raw data using PySpark
   - Performs data transformations, aggregations, and quality checks
   - Output: `data/processed/sensor_data_main.parquet/`

3. **Database Loading** (`pipeline/load_to_database.py`)
   - Loads processed Parquet files into PostgreSQL
   - Creates necessary tables and indexes

4. **REST API** (`app/`)
   - FastAPI-based backend
   - Provides endpoints for pipeline execution and data querying
   - Handles filtering, sorting, and pagination

5. **Web Dashboard** (`app/static/`)
   - Interactive frontend for pipeline management
   - Data visualization and export capabilities

## Getting Started

### Prerequisites
- Python 3.12+
- Docker and Docker Compose
- 2GB+ free disk space

### 1. Start PostgreSQL with Docker
Create a `.env` file and copy the contents of the `env_template` file into it.

Then start the Docker engine and run to start the PostgreSQL database:
```bash
docker-compose up -d
```

### 2. Create Virtual Environment and Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start the API
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Open Dashboard
- Open your browser: **http://localhost:8000** for the UI
- For API documentation: **http://localhost:8000/api/docs**

## Running the Pipeline

You can run the pipeline in two ways:

### Option 1: Using the Web Dashboard (Recommended)
1. Open **http://localhost:8000** in your browser
2. Use the dashboard buttons to control the pipeline:
   - **"Generate Data"** → Creates raw sensor data
   - **"Process Data"** → Processes data with PySpark
   - **"Load Data"** → Loads data into PostgreSQL
   - **"Run Complete Pipeline"** → Executes all steps automatically

### Option 2: Manual Execution
Run each pipeline step individually from the command line:

```bash
# Step 1: Generate sensor data
python -m pipeline.generate_sensor_data

# Step 2: Process data with PySpark
python -m pipeline.process_sensor_data

# Step 3: Load to database
python -m pipeline.load_to_database
```

## Using the Dashboard

### Running the Pipeline
1. Click **"Run Complete Pipeline"** button or run steps individually:
   - **"Generate Data"** → Specify the number of rows to generate
   - **"Process Data"** → Processes raw CSV with PySpark
   - **"Load Data"** → Loads processed Parquet into PostgreSQL
2. Monitor real-time status updates during execution
3. View completion notifications

### Viewing Data
1. Select a sensor from the dropdown menu
2. Use the data table features:
   - **Sort** by clicking column headers
   - **Filter** using the search box
   - **Navigate** with pagination controls
   - **Export** data to CSV

### Clearing Data
Click **"Clear All Data"** to reset the entire pipeline:
- Deletes raw CSV files
- Removes processed Parquet files
- Clears database tables

## API Endpoints

### Pipeline Management
- `POST /api/pipeline/generate` - Generate sensor data
- `POST /api/pipeline/process` - Process data with PySpark
- `POST /api/pipeline/load` - Load data to database
- `DELETE /api/pipeline/data` - Clear all data
- `GET /api/pipeline/status` - Check pipeline status

### Data Access
- `GET /api/sensors` - List all sensors
- `GET /api/sensors/{sensor_id}/data` - Get sensor data with filtering
- `GET /api/sensors/{sensor_id}/summary` - Get sensor summary/statistics

For detailed API documentation, visit: **http://localhost:8000/api/docs**

## Testing

Run the test suite to verify functionality:

```bash
# Run all tests
python -m pytest
```

## Stopping the Application

### Stop the API Server
Press <kbd>Ctrl</kbd>+<kbd>C</kbd> (or <kbd>Command</kbd>+<kbd>C</kbd> on Mac) in the terminal where the API is running.

### Stop the Database
```bash
docker-compose down
```

### Stop and Remove All Data (including volumes)
```bash
docker-compose down -v
```

## Technologies Used

- **Backend**: FastAPI, Python 
- **Data Processing**: PySpark
- **Database**: PostgreSQL
- **Frontend**: Vanilla JavaScript, HTML5, CSS3
- **Containerization**: Docker, Docker Compose
- **Testing**: pytest

## Project Structure

```
bosch_task/
├── app/                          # FastAPI Application (Task 2)
│   ├── __init__.py               # Package initialization
│   ├── main.py                   # API initialization and configuration
│   ├── routers.py                # Data query endpoints
│   ├── pipeline_router.py        # Pipeline execution endpoints
│   ├── models.py                 # Pydantic models for validation
│   ├── database.py               # Database connection configuration
│   ├── sql_scripts.py            # SQL queries
│   └── static/                   # Frontend files
│       ├── index.html            # Main dashboard page
│       ├── styles.css            # Styling
│       └── app.js                # JavaScript logic
│
├── pipeline/                      # Data Pipeline (Task 3)
│   ├── __init__.py               # Package initialization
│   ├── generate_sensor_data.py  # Synthetic data generation
│   ├── process_sensor_data.py   # PySpark data processing
│   ├── load_to_database.py      # Database loading
│   └── sql_scripts.py            # SQL queries and schema
│
├── tests/                         # Test Suite
│   ├── __init__.py               # Package initialization
│   ├── test_routers.py           # API endpoint tests
│   └── test_pipeline_router.py  # Pipeline endpoint tests
│
├── data/                         # Data Storage
│   ├── raw/                      # Raw CSV files
│   │   └── sensor_data.csv      
│   └── processed/                # Processed Parquet files
│       └── sensor_data_main.parquet/ 
│
├── docker-compose.yml             # PostgreSQL Docker configuration
├── requirements.txt               # Python dependencies
├── env_template                   # Environment variables template
└── README.md                      # Project documentation
```
