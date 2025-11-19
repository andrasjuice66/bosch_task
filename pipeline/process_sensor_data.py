import logging
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SensorDataProcessor:
    """
    Processor for sensor data using PySpark.

    Attributes:
        spark: SparkSession instance
        input_path: Path to input data
        output_path: Path to save processed data (Parquet)
        db_url: JDBC URL for database (optional)
        db_table: Target table name for database (optional)
        db_properties: JDBC connection properties (user, password, driver) (optional)
    """

    def __init__(
        self,
        input_path: str = "data/raw/sensor_data.csv",
        output_path: str = "data/processed/sensor_data_main.parquet",
        db_url: Optional[str] = None,
        db_table: Optional[str] = None,
        db_properties: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the sensor data processor.

        Args:
            input_path: Path to the input CSV file
            output_path: Path to save the processed Parquet file (main table)
            db_url: JDBC URL for database (e.g., "jdbc:postgresql://localhost:5432/sensordb")
            db_table: Target table name in the database (e.g., "public.sensor_main")
            db_properties: JDBC properties dict, must include 'driver' (e.g., {"user":"...", "password":"...", "driver":"org.postgresql.Driver"})
        """
        self.input_path = input_path
        self.output_path = output_path
        self.db_url = db_url
        self.db_table = db_table
        self.db_properties = db_properties or {}
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure a Spark session.

        Returns:
            Configured SparkSession
        """
        logger.info("Creating Spark session...")
        spark = (
            SparkSession.builder.appName("SensorDataProcessing")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.executor.instances", "10")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.files.maxPartitionBytes", "128MB")
            .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "2")
            .config("spark.dynamicAllocation.maxExecutors", "20")
            .config("spark.speculation", "true")
            .config("spark.speculation.multiplier", "2")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark

    def load_data(self) -> DataFrame:
        """
        Load sensor data from CSV file with defined schema.

        Returns:
            DataFrame with loaded sensor data
        """
        logger.info(f"Loading data from {self.input_path}...")

        schema = StructType(
            [
                StructField("Sensor_ID", StringType(), nullable=True),
                StructField("Date", StringType(), nullable=True),
                StructField("Location", StringType(), nullable=True),
                StructField("Parameter", StringType(), nullable=True),
                StructField("Value", DoubleType(), nullable=True),
                StructField("Status", StringType(), nullable=True),
            ]
        )

        df = (
            self.spark.read.option("header", "true")
            .option("mode", "DROPMALFORMED")
            .schema(schema)
            .csv(self.input_path)
        )

        logger.info(f"Loaded {df.count()} rows")
        return df

    def save_parquet(self, df: DataFrame, path: str) -> None:
        """
        Save DataFrame to Parquet (overwrite).

        Args:
            df: DataFrame to save
            path: Destination path
        """
        logger.info(f"Saving main table to {path}...")
        (df.write.mode("overwrite").parquet(path))
        logger.info("Parquet save complete")

    def write_to_database(self, df: DataFrame) -> None:
        """
        Write the main DataFrame to a database via JDBC (single table).

        Requires the JDBC driver JAR on the Spark classpath (e.g., via --jars).

        Args:
            df: Main DataFrame to write
        """
        if not (self.db_url and self.db_table and self.db_properties.get("driver")):
            logger.info("Database parameters not fully provided; skipping DB write.")
            return

        logger.info(
            f"Writing main table to database {self.db_url} table {self.db_table} ..."
        )
        (
            df.write.mode("overwrite").jdbc(
                url=self.db_url, table=self.db_table, properties=self.db_properties
            )
        )
        logger.info("Database write complete")

    def preprocess(self, df: DataFrame) -> DataFrame:
        """
        Standardize types and values: trim strings, parse dates, add derived date columns.

        - Normalize casing (Parameter title-case, Status upper, Location title-case)
        - Parse Date from multiple formats into DateType column `ts_date`
        - Add year, month, day, and month-start (`ts_month`)
        - Add is_bad numeric flag

        Args:
            df: Raw input DataFrame

        Returns:
            Cleaned DataFrame with standardized fields
        """
        logger.info("Preprocessing: standardizing strings and dates...")

        ts_date = F.coalesce(
            F.to_date(F.col("Date"), "yyyy-MM-dd"),
            F.to_date(F.col("Date"), "yyyy.MM.dd"),
            F.to_date(F.col("Date"), "yyyy/MM/dd"),
        ).alias("ts_date")

        clean = (
            df.withColumn("Sensor_ID", F.trim(F.col("Sensor_ID")))
            .withColumn("Location", F.initcap(F.trim(F.col("Location"))))
            .withColumn("Parameter", F.initcap(F.trim(F.col("Parameter"))))
            .withColumn("Status", F.upper(F.trim(F.col("Status"))))
            .withColumn("ts_date", ts_date)
            .drop("Date")
            .filter(F.col("ts_date").isNotNull())
        )

        clean = (
            clean.withColumn("year", F.year("ts_date"))
            .withColumn("month", F.month("ts_date"))
            .withColumn("day", F.dayofmonth("ts_date"))
            .withColumn("ts_month", F.trunc("ts_date", "MM").cast(DateType()))
        )

        clean = clean.withColumn(
            "Status",
            F.when(F.col("Status").isin("GOOD", "BAD"), F.col("Status")).otherwise(
                F.lit("UNKNOWN")
            ),
        )

        clean = clean.withColumn(
            "is_bad", F.when(F.col("Status") == "BAD", 1).otherwise(0)
        )

        logger.info("Preprocessing complete")
        return clean

    def pivot_transformations(self, df: DataFrame) -> DataFrame:
        """
        Create a daily wide table per Sensor_ID and ts_date:
        - Pivot Parameter values into columns (daily max as example)
        - Compute any_bad flag per day
        - Pick a Location per day (first non-null)

        Args:
            df: Cleaned long-format DataFrame

        Returns:
            Wide-format DataFrame (one row per sensor per day)
        """
        logger.info("Applying pivot transformations...")

        daily_bad = df.groupBy("Sensor_ID", "ts_date").agg(
            F.max("is_bad").alias("any_bad")
        )

        first_loc = df.groupBy("Sensor_ID", "ts_date").agg(
            F.first(F.col("Location"), ignorenulls=True).alias("Location")
        )

        pivoted = (
            df.groupBy("Sensor_ID", "ts_date").pivot("Parameter").agg(F.max("Value"))
        )

        wide = pivoted.join(first_loc, on=["Sensor_ID", "ts_date"], how="left").join(
            daily_bad, on=["Sensor_ID", "ts_date"], how="left"
        )

        wide = wide.join(
            df.select("Sensor_ID", "ts_date", "ts_month").dropDuplicates(),
            on=["Sensor_ID", "ts_date"],
            how="left",
        )

        param_cols = [
            c
            for c in wide.columns
            if c not in {"Sensor_ID", "ts_date", "ts_month", "Location", "any_bad"}
        ]
        ordered = wide.select(
            ["Sensor_ID", "ts_date", "ts_month", "Location"]
            + sorted(param_cols)
            + ["any_bad"]
        )

        logger.info("Pivot transformations complete")
        return ordered

    def aggregation_transformations(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Compute:
        - Monthly stats per Sensor_ID + Parameter + ts_month
        - Overall sensor profile per Sensor_ID

        Args:
            df: Cleaned long-format DataFrame

        Returns:
            (monthly_stats_long, sensor_profile)
        """
        logger.info("Applying aggregation transformations...")

        monthly_stats = df.groupBy("Sensor_ID", "Parameter", "ts_month").agg(
            F.count(F.lit(1)).alias("readings_count"),
            F.avg("Value").alias("avg_value"),
            F.stddev_samp("Value").alias("std_value"),
            F.min("Value").alias("min_value"),
            F.max("Value").alias("max_value"),
            (F.sum("is_bad") / F.count(F.lit(1))).alias("pct_bad"),
        )

        sensor_profile = (
            df.groupBy("Sensor_ID")
            .agg(
                F.count(F.lit(1)).alias("total_readings"),
                F.sum("is_bad").alias("total_bad"),
                (F.sum("is_bad") / F.count(F.lit(1))).alias("total_pct_bad"),
                F.min("ts_date").alias("first_seen_date"),
                F.max("ts_date").alias("last_seen_date"),
                F.countDistinct("Location").alias("distinct_locations"),
                F.countDistinct("Parameter").alias("distinct_parameters"),
            )
            .withColumn(
                "days_active",
                F.datediff("last_seen_date", "first_seen_date") + F.lit(1),
            )
        )

        logger.info("Aggregation transformations complete")
        return monthly_stats, sensor_profile

    def window_transformations(self, wide_df: DataFrame) -> DataFrame:
        """
        Add per-sensor window features on the wide daily table:
        - For each numeric parameter column: prev_X, delta_X, mean_7r_X
        - Good streak days based on any_bad

        Args:
            wide_df: Wide daily DataFrame

        Returns:
            Wide DataFrame with window columns
        """
        logger.info("Applying window transformations...")

        params: List[str] = [
            c
            for c in [
                "Noise",
                "Vibration",
                "Humidity",
                "Pressure",
                "Current",
                "Temperature",
                "Offset",
            ]
            if c in wide_df.columns
        ]

        order_w = Window.partitionBy("Sensor_ID").orderBy("ts_date")
        last7_w = order_w.rowsBetween(-6, 0)
        cum_w = (
            Window.partitionBy("Sensor_ID")
            .orderBy("ts_date")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        out = wide_df
        for p in params:
            prev_col = f"prev_{p.lower()}"
            delta_col = f"delta_{p.lower()}"
            mean7_col = f"mean_7r_{p.lower()}"

            out = out.withColumn(prev_col, F.lag(F.col(p), 1).over(order_w))
            out = out.withColumn(
                delta_col, F.when(F.col(p).isNotNull(), F.col(p) - F.col(prev_col))
            )
            out = out.withColumn(mean7_col, F.avg(F.col(p)).over(last7_w))

        out = out.withColumn(
            "bad_cum", F.sum(F.coalesce(F.col("any_bad"), F.lit(0))).over(cum_w)
        )
        grp_w = Window.partitionBy("Sensor_ID", "bad_cum").orderBy("ts_date")
        out = out.withColumn("row_in_group", F.row_number().over(grp_w))
        out = out.withColumn(
            "good_streak_days",
            F.when(F.col("any_bad") == 1, F.lit(0)).otherwise(
                F.col("row_in_group") - 1
            ),
        )

        out = out.drop("row_in_group")

        logger.info("Window transformations complete")
        return out

    def _pivot_monthly_metrics(self, monthly_stats: DataFrame) -> DataFrame:
        """
        Pivot monthly metrics so each metric and parameter becomes a column at grain (Sensor_ID, ts_month).

        The resulting columns are named like:
        - Noise_avg, Temperature_std, Humidity_min, ... then renamed to mo_avg_Noise, mo_std_Temperature, etc.

        Args:
            monthly_stats: long-format monthly stats

        Returns:
            Wide monthly metrics DataFrame at (Sensor_ID, ts_month)
        """
        logger.info("Pivoting monthly metrics to wide columns...")

        monthly_pivot = (
            monthly_stats.groupBy("Sensor_ID", "ts_month")
            .pivot("Parameter")
            .agg(
                F.first("avg_value").alias("avg"),
                F.first("std_value").alias("std"),
                F.first("min_value").alias("min"),
                F.first("max_value").alias("max"),
                F.first("pct_bad").alias("pct_bad"),
                F.first("readings_count").alias("count"),
            )
        )

        def rename_cols(cols: List[str]) -> List[str]:
            new_cols = []
            for c in cols:
                if c in ("Sensor_ID", "ts_month"):
                    new_cols.append(c)
                    continue
                if "_" in c:
                    param, metric = c.split("_", 1)
                    new_cols.append(f"mo_{metric}_{param}")
                else:
                    new_cols.append(c)
            return new_cols

        renamed_cols = rename_cols(monthly_pivot.columns)
        monthly_pivot = monthly_pivot.toDF(*renamed_cols)
        return monthly_pivot

    def build_main_table(self, clean_df: DataFrame) -> DataFrame:
        """
        Build a single main table that includes:
        - Daily wide pivoted values and any_bad
        - Window features (prev/delta/rolling/streak)
        - Aggregation features joined in:
            * Sensor-level profile (joined by Sensor_ID)
            * Monthly per-parameter stats (joined by Sensor_ID + ts_month)

        Args:
            clean_df: Cleaned long-format DataFrame

        Returns:
            main_df: Single, analysis-ready table to persist and load to DB
        """
        logger.info("Building main table...")

        wide_df = self.pivot_transformations(clean_df)

        windowed_df = self.window_transformations(wide_df)

        monthly_stats_long, sensor_profile = self.aggregation_transformations(clean_df)

        monthly_metrics_wide = self._pivot_monthly_metrics(monthly_stats_long)

        main = windowed_df.join(
            monthly_metrics_wide, on=["Sensor_ID", "ts_month"], how="left"
        ).join(sensor_profile, on="Sensor_ID", how="left")

        id_cols = ["Sensor_ID", "ts_date", "ts_month", "Location"]
        base_cols = [
            c
            for c in [
                "Noise",
                "Vibration",
                "Humidity",
                "Pressure",
                "Current",
                "Temperature",
                "Offset",
            ]
            if c in main.columns
        ]
        flag_cols = ["any_bad", "good_streak_days"]
        window_cols = [
            c for c in main.columns if c.startswith(("prev_", "delta_", "mean_7r_"))
        ]
        monthly_cols = [c for c in main.columns if c.startswith("mo_")]
        profile_cols = [
            c
            for c in [
                "total_readings",
                "total_bad",
                "total_pct_bad",
                "first_seen_date",
                "last_seen_date",
                "distinct_locations",
                "distinct_parameters",
                "days_active",
            ]
            if c in main.columns
        ]
        other_cols = [
            c
            for c in main.columns
            if c
            not in (
                id_cols
                + base_cols
                + flag_cols
                + window_cols
                + monthly_cols
                + profile_cols
            )
        ]

        ordered_cols = (
            id_cols
            + base_cols
            + flag_cols
            + window_cols
            + monthly_cols
            + profile_cols
            + other_cols
        )
        main = main.select(*ordered_cols)

        logger.info("Main table built successfully")
        return main

    def run(self) -> None:
        """
        Execute the pipeline and produce a single main table:
        - Load raw
        - Preprocess
        - Build main table (pivot + windows + aggregations merged)
        - Save to Parquet (single dataset)
        - Optionally write the same main table to a database (single table)
        """
        raw_df = self.load_data()
        clean_df = self.preprocess(raw_df)
        main_df = self.build_main_table(clean_df)

        self.save_parquet(main_df, self.output_path)

        self.write_to_database(main_df)

        logger.info("Pipeline finished. Main table saved and optionally loaded to DB.")


def main():
    processor = SensorDataProcessor(
        input_path="data/raw/sensor_data.csv",
        output_path="data/processed/sensor_data_main.parquet",
    )
    processor.run()


if __name__ == "__main__":
    main()
