from pipeline.sql_scripts import integrity_checks, create_main_table

from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """
    Loader for transferring PySpark processed data to PostgreSQL.

    This loader creates and populates a single comprehensive main table
    (sensor_data_main) that combines all processed features into one
    denormalized structure for efficient querying and analysis.

    Attributes:
        spark: SparkSession instance
        db_url: SQLAlchemy database URL
        jdbc_url: JDBC connection URL for PySpark
        connection_properties: JDBC connection properties
    """

    def __init__(self):
        """Initialize the database loader with connection settings."""
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")
        self.db_name = os.getenv("POSTGRES_DB", "sensor_data")
        self.db_user = os.getenv("POSTGRES_USER", "bosch_user")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "bosch_pass")

        self.db_url = (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )

        self.jdbc_url = (
            f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"
        )

        self.connection_properties = {
            "user": self.db_user,
            "password": self.db_password,
            "driver": "org.postgresql.Driver",
        }

        self.spark = self._create_spark_session()
        self.engine = create_engine(self.db_url)

    def _create_spark_session(self) -> SparkSession:
        """
        Create Spark session configured for PostgreSQL connectivity.

        Returns:
            Configured SparkSession with PostgreSQL JDBC driver
        """
        logger.info("Creating Spark session with PostgreSQL support...")

        spark = (
            SparkSession.builder.appName("SensorDataDatabaseLoader")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark session created successfully")
        return spark

    def create_tables(self) -> None:
        """
        Create database table matching the processor output structure.

        Creates a single main table that includes:
        - Daily wide format with pivoted parameters
        - Window functions (prev/delta/rolling/streak)
        - Monthly aggregation features (joined per sensor/month)
        - Sensor profile features (joined per sensor)

        Note: Drops existing table to ensure clean state on each run.
        """
        logger.info("Dropping any existing tables for clean state...")

        drop_tables = """
        DROP TABLE IF EXISTS sensor_data_main CASCADE;
        """

        logger.info("Creating new main database table...")

        with self.engine.connect() as conn:
            conn.execute(text(drop_tables))
            conn.execute(text(create_main_table))
            conn.commit()

        logger.info("Main table created successfully")

    def load_parquet_to_postgres(
        self, parquet_path: str, table_name: str, mode: str = "overwrite"
    ) -> None:
        """
        Load data from Parquet file to PostgreSQL table using PySpark.

        Args:
            parquet_path: Path to the Parquet file or directory
            table_name: Target PostgreSQL table name
            mode: Write mode ('overwrite' or 'append')
        """
        logger.info(f"Loading data from {parquet_path} to table {table_name}...")

        df = self.spark.read.parquet(parquet_path)

        logger.info(f"Sample data from {parquet_path}:")
        df.show(5, truncate=False)

        row_count = df.count()
        logger.info(f"Total rows to load: {row_count}")

        (
            df.write.format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", table_name)
            .option("user", self.db_user)
            .option("password", self.db_password)
            .option("driver", "org.postgresql.Driver")
            .mode(mode)
            .save()
        )

        logger.info(f"Successfully loaded {row_count} rows to {table_name}")

    def run_data_integrity_checks(self) -> None:
        """
        Execute SQL queries to verify data integrity and processing correctness.

        Runs checks on the main table with automatic assertions:
        1. Data Completeness - Ensure core data exists
        2. Primary Key Integrity - No NULLs in critical columns
        3. Value Ranges - Sensor values are reasonable

        Raises:
            AssertionError: If any integrity check fails
        """
        logger.info("=" * 80)
        logger.info("DATA INTEGRITY CHECKS")
        logger.info("=" * 80)

        all_passed = True
        results = []

        with self.engine.connect() as conn:
            for i, check in enumerate(integrity_checks, 1):
                logger.info(f"\n{check['name']}")
                logger.info("-" * 80)

                try:
                    result = conn.execute(text(check["query"]))
                    row = result.fetchone()
                    columns = result.keys()

                    row_dict = dict(zip(columns, row))

                    for col, val in row_dict.items():
                        logger.info(f"  {col}: {val}")

                    assert_field = check.get("assert_field")
                    if assert_field:
                        status = row_dict.get(assert_field)

                        if status == "PASS":
                            logger.info("\n  ✓ ASSERTION PASSED")
                            results.append((check["name"], "PASS", row_dict))
                        else:
                            logger.error("\n  ✗ ASSERTION FAILED")
                            all_passed = False
                            results.append((check["name"], "FAIL", row_dict))
                    else:
                        results.append((check["name"], "INFO", row_dict))

                except Exception as e:
                    logger.error(f"  ✗ ERROR: {e}")
                    all_passed = False
                    results.append((check["name"], "ERROR", {"error": str(e)}))

        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        for name, status, _ in results:
            symbol = (
                "✓" if status == "PASS" else "✗" if status in ["FAIL", "ERROR"] else "ℹ"
            )
            logger.info(f"{symbol} {name}: {status}")

        logger.info("\n" + "=" * 80)
        if all_passed:
            logger.info("ALL INTEGRITY CHECKS PASSED ✓")
        else:
            logger.error("SOME INTEGRITY CHECKS FAILED ✗")
        logger.info("=" * 80)

        if not all_passed:
            raise AssertionError("Data integrity checks failed! See log for details.")

    def load_all_data(self) -> None:
        """
        Load processed main table Parquet file into PostgreSQL database.

        Loads a single comprehensive table:
        - sensor_data_main: Main table with all features (pivoted values, window features, monthly metrics, sensor profile)
        """
        logger.info("Starting data loading process...")

        self.create_tables()

        logger.info("\n" + "=" * 80)
        logger.info("LOADING MAIN TABLE (sensor_data_main)")
        logger.info("=" * 80)
        self.load_parquet_to_postgres(
            "data/processed/sensor_data_main.parquet", "sensor_data_main"
        )

        logger.info("\n" + "=" * 80)
        logger.info("DATA LOADED SUCCESSFULLY!")
        logger.info("=" * 80 + "\n")

        self.run_data_integrity_checks()

    def stop(self) -> None:
        """Clean up resources."""
        logger.info("Stopping Spark session...")
        self.spark.stop()
        self.engine.dispose()


def main():
    loader = DatabaseLoader()

    try:
        loader.load_all_data()
    finally:
        loader.stop()


if __name__ == "__main__":
    main()
