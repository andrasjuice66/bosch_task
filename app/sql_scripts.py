table_query = """
            SELECT 
                "Sensor_ID",
                ts_date,
                ts_month,
                "Location",
                "Current",
                "Humidity",
                "Noise",
                "Offset",
                "Pressure",
                "Temperature",
                "Vibration",
                any_bad,
                good_streak_days,
                prev_current, delta_current, mean_7r_current,
                prev_humidity, delta_humidity, mean_7r_humidity,
                prev_noise, delta_noise, mean_7r_noise,
                prev_offset, delta_offset, mean_7r_offset,
                prev_pressure, delta_pressure, mean_7r_pressure,
                prev_temperature, delta_temperature, mean_7r_temperature,
                prev_vibration, delta_vibration, mean_7r_vibration,
                "mo_avg_Current", "mo_std_Current", "mo_min_Current", "mo_max_Current", "mo_pct_bad_Current", "mo_count_Current",
                "mo_avg_Humidity", "mo_std_Humidity", "mo_min_Humidity", "mo_max_Humidity", "mo_pct_bad_Humidity", "mo_count_Humidity",
                "mo_avg_Noise", "mo_std_Noise", "mo_min_Noise", "mo_max_Noise", "mo_pct_bad_Noise", "mo_count_Noise",
                "mo_avg_Offset", "mo_std_Offset", "mo_min_Offset", "mo_max_Offset", "mo_pct_bad_Offset", "mo_count_Offset",
                "mo_avg_Pressure", "mo_std_Pressure", "mo_min_Pressure", "mo_max_Pressure", "mo_pct_bad_Pressure", "mo_count_Pressure",
                "mo_avg_Temperature", "mo_std_Temperature", "mo_min_Temperature", "mo_max_Temperature", "mo_pct_bad_Temperature", "mo_count_Temperature",
                "mo_avg_Vibration", "mo_std_Vibration", "mo_min_Vibration", "mo_max_Vibration", "mo_pct_bad_Vibration", "mo_count_Vibration",
                total_readings,
                total_bad,
                total_pct_bad,
                first_seen_date,
                last_seen_date,
                days_active
            FROM sensor_data_main
            WHERE "Sensor_ID" = :sensor_id
        """


summary_query = """
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
