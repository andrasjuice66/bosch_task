create_main_table = """
        CREATE TABLE IF NOT EXISTS sensor_data_main (
            "Sensor_ID" VARCHAR(50),
            ts_date DATE,
            ts_month DATE,
            "Location" VARCHAR(100),
            
            "Current" DOUBLE PRECISION,
            "Humidity" DOUBLE PRECISION,
            "Noise" DOUBLE PRECISION,
            "Offset" DOUBLE PRECISION,
            "Pressure" DOUBLE PRECISION,
            "Temperature" DOUBLE PRECISION,
            "Vibration" DOUBLE PRECISION,
            
            any_bad INTEGER,
            good_streak_days BIGINT,
            
            prev_current DOUBLE PRECISION,
            delta_current DOUBLE PRECISION,
            mean_7r_current DOUBLE PRECISION,
            
            prev_humidity DOUBLE PRECISION,
            delta_humidity DOUBLE PRECISION,
            mean_7r_humidity DOUBLE PRECISION,
            
            prev_noise DOUBLE PRECISION,
            delta_noise DOUBLE PRECISION,
            mean_7r_noise DOUBLE PRECISION,
            
            prev_offset DOUBLE PRECISION,
            delta_offset DOUBLE PRECISION,
            mean_7r_offset DOUBLE PRECISION,
            
            prev_pressure DOUBLE PRECISION,
            delta_pressure DOUBLE PRECISION,
            mean_7r_pressure DOUBLE PRECISION,
            
            prev_temperature DOUBLE PRECISION,
            delta_temperature DOUBLE PRECISION,
            mean_7r_temperature DOUBLE PRECISION,
            
            prev_vibration DOUBLE PRECISION,
            delta_vibration DOUBLE PRECISION,
            mean_7r_vibration DOUBLE PRECISION,
            
            mo_avg_current DOUBLE PRECISION,
            mo_std_current DOUBLE PRECISION,
            mo_min_current DOUBLE PRECISION,
            mo_max_current DOUBLE PRECISION,
            mo_pct_bad_current DOUBLE PRECISION,
            mo_count_current BIGINT,
            
            mo_avg_humidity DOUBLE PRECISION,
            mo_std_humidity DOUBLE PRECISION,
            mo_min_humidity DOUBLE PRECISION,
            mo_max_humidity DOUBLE PRECISION,
            mo_pct_bad_humidity DOUBLE PRECISION,
            mo_count_humidity BIGINT,
            
            mo_avg_noise DOUBLE PRECISION,
            mo_std_noise DOUBLE PRECISION,
            mo_min_noise DOUBLE PRECISION,
            mo_max_noise DOUBLE PRECISION,
            mo_pct_bad_noise DOUBLE PRECISION,
            mo_count_noise BIGINT,
            
            mo_avg_offset DOUBLE PRECISION,
            mo_std_offset DOUBLE PRECISION,
            mo_min_offset DOUBLE PRECISION,
            mo_max_offset DOUBLE PRECISION,
            mo_pct_bad_offset DOUBLE PRECISION,
            mo_count_offset BIGINT,
            
            mo_avg_pressure DOUBLE PRECISION,
            mo_std_pressure DOUBLE PRECISION,
            mo_min_pressure DOUBLE PRECISION,
            mo_max_pressure DOUBLE PRECISION,
            mo_pct_bad_pressure DOUBLE PRECISION,
            mo_count_pressure BIGINT,
            
            mo_avg_temperature DOUBLE PRECISION,
            mo_std_temperature DOUBLE PRECISION,
            mo_min_temperature DOUBLE PRECISION,
            mo_max_temperature DOUBLE PRECISION,
            mo_pct_bad_temperature DOUBLE PRECISION,
            mo_count_temperature BIGINT,
            
            mo_avg_vibration DOUBLE PRECISION,
            mo_std_vibration DOUBLE PRECISION,
            mo_min_vibration DOUBLE PRECISION,
            mo_max_vibration DOUBLE PRECISION,
            mo_pct_bad_vibration DOUBLE PRECISION,
            mo_count_vibration BIGINT,
            
            total_readings BIGINT,
            total_bad BIGINT,
            total_pct_bad DOUBLE PRECISION,
            first_seen_date DATE,
            last_seen_date DATE,
            distinct_locations BIGINT,
            distinct_parameters BIGINT,
            days_active INTEGER,
            
            bad_cum BIGINT,
            
            PRIMARY KEY ("Sensor_ID", ts_date)
        );
        """

integrity_checks = [
    {
        "name": "Check 1: Data Completeness",
        "query": """
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT "Sensor_ID") as unique_sensors,
                COUNT(DISTINCT "Location") as unique_locations,
                CASE 
                    WHEN COUNT(*) > 0 AND COUNT(DISTINCT "Sensor_ID") > 0 THEN 'PASS'
                    ELSE 'FAIL'
                END as completeness_status
            FROM sensor_data_main;
        """,
        "assert_field": "completeness_status",
    },
    {
        "name": "Check 2: Primary Key and Critical Column Integrity",
        "query": """
            SELECT 
                COUNT(*) as total_rows,
                SUM(CASE WHEN "Sensor_ID" IS NULL THEN 1 ELSE 0 END) as null_sensor_id,
                SUM(CASE WHEN ts_date IS NULL THEN 1 ELSE 0 END) as null_date,
                SUM(CASE WHEN "Location" IS NULL THEN 1 ELSE 0 END) as null_location,
                CASE 
                    WHEN SUM(CASE WHEN "Sensor_ID" IS NULL THEN 1 ELSE 0 END) = 0 
                     AND SUM(CASE WHEN ts_date IS NULL THEN 1 ELSE 0 END) = 0 
                     AND SUM(CASE WHEN "Location" IS NULL THEN 1 ELSE 0 END) = 0 
                    THEN 'PASS'
                    ELSE 'FAIL'
                END as integrity_status
            FROM sensor_data_main;
        """,
        "assert_field": "integrity_status",
    },
    {
        "name": "Check 3: Value Ranges Validation",
        "query": """
            SELECT 
                COUNT(*) as total_rows_with_values,
                SUM(CASE WHEN "Temperature" < -50 OR "Temperature" > 150 THEN 1 ELSE 0 END) as invalid_temperature,
                SUM(CASE WHEN "Humidity" < 0 OR "Humidity" > 100 THEN 1 ELSE 0 END) as invalid_humidity,
                SUM(CASE WHEN "Pressure" < 0 OR "Pressure" > 2000 THEN 1 ELSE 0 END) as invalid_pressure,
                SUM(CASE WHEN "Vibration" < 0 THEN 1 ELSE 0 END) as invalid_vibration,
                CASE 
                    WHEN SUM(CASE WHEN "Temperature" < -50 OR "Temperature" > 150 THEN 1 ELSE 0 END) = 0
                     AND SUM(CASE WHEN "Humidity" < 0 OR "Humidity" > 100 THEN 1 ELSE 0 END) = 0
                     AND SUM(CASE WHEN "Pressure" < 0 OR "Pressure" > 2000 THEN 1 ELSE 0 END) = 0
                     AND SUM(CASE WHEN "Vibration" < 0 THEN 1 ELSE 0 END) = 0
                    THEN 'PASS'
                    ELSE 'FAIL'
                END as range_validation_status
            FROM sensor_data_main
            WHERE "Temperature" IS NOT NULL OR "Humidity" IS NOT NULL 
               OR "Pressure" IS NOT NULL OR "Vibration" IS NOT NULL;
        """,
        "assert_field": "range_validation_status",
    },
]
