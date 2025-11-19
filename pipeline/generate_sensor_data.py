import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd  # type: ignore[import-untyped]
from faker import Faker
import logging
import os
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SensorDataGenerator:
    """
    Generator for creating synthetic sensor data.

    Attributes:
        num_rows: Number of rows to generate
        fake: Faker instance for generating random data
    """

    def __init__(
        self,
        num_rows: int = 500000,
        start_date: datetime = datetime(2024, 1, 1),
        end_date: datetime = datetime(2025, 11, 14),
        output_path: str = "data/raw/sensor_data.csv",
    ):
        """
        Initialize the sensor data generator.

        Args:
            num_rows: Number of rows to generate (default: 500,000)
        """
        self.num_rows = num_rows
        self.start_date = start_date
        self.end_date = end_date
        self.output_path = output_path
        self.fake = Faker()
        Faker.seed(42)
        random.seed(42)

        self.sensor_ids = [
            f"sensor_{self.fake.bothify(text='???####')}" for _ in range(100)
        ]
        self.locations = [
            "Hungary",
            "Germany",
            "France",
            "Italy",
            "Spain",
            "Poland",
            "Netherlands",
            "Belgium",
            "Austria",
            "Czech Republic",
        ]
        self.parameters = [
            "Offset",
            "Noise",
            "Temperature",
            "Pressure",
            "Humidity",
            "Vibration",
            "Voltage",
            "Current",
        ]
        self.statuses = ["Good", "Bad"]

    def _generate_random_date(self) -> str:
        """
        Generate a random date between 2024-01-01 and 2025-11-14.

        Returns:
            Date string in YYYY-MM-DD format
        """

        time_between = self.end_date - self.start_date
        random_days = random.randint(0, time_between.days)
        random_date = self.start_date + timedelta(days=random_days)
        return random_date.strftime("%Y-%m-%d")

    def _generate_value_for_parameter(self, parameter: str) -> float:
        """
        Generate a realistic value based on the parameter type.

        Args:
            parameter: The parameter name

        Returns:
            A float value appropriate for the parameter
        """
        value_ranges = {
            "Offset": (0.0, 100.0),
            "Noise": (0.0, 50.0),
            "Temperature": (-20.0, 80.0),
            "Pressure": (900.0, 1100.0),
            "Humidity": (0.0, 100.0),
            "Vibration": (0.0, 10.0),
            "Voltage": (0.0, 240.0),
            "Current": (0.0, 20.0),
        }

        min_val, max_val = value_ranges.get(parameter, (0.0, 100.0))
        return round(random.uniform(min_val, max_val), 2)

    def generate_data(self) -> pd.DataFrame:
        """
        Generate the complete sensor dataset.
        For each (Sensor_ID, Date) combination that has data, all parameters are generated.

        Returns:
            DataFrame containing all generated sensor data
        """
        num_combinations = self.num_rows // len(self.parameters)
        total_rows = num_combinations * len(self.parameters)

        logger.info(
            f"Generating {total_rows} rows of sensor data ({num_combinations} sensor-date combinations with all {len(self.parameters)} parameters)..."
        )

        data: List[Dict] = []

        for i in range(num_combinations):
            if i % 10000 == 0 and i > 0:
                logger.info(
                    f"Generated {i * len(self.parameters)} rows ({i} sensor-date combinations)..."
                )

            sensor_id = random.choice(self.sensor_ids)
            date = self._generate_random_date()
            location = random.choice(self.locations)

            for parameter in self.parameters:
                row = {
                    "Sensor_ID": sensor_id,
                    "Date": date,
                    "Location": location,
                    "Parameter": parameter,
                    "Value": self._generate_value_for_parameter(parameter),
                    "Status": random.choices(self.statuses, weights=[0.97, 0.03])[0],
                }
                data.append(row)

        df = pd.DataFrame(data)
        logger.info(f"Successfully generated {len(df)} rows of sensor data")
        logger.info(f"Dataset shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")

        return df

    def save_to_csv(self, df: pd.DataFrame, output_path: Optional[str] = None) -> None:
        """
        Save the generated data to a CSV file, overwriting any previous file.

        Args:
            df: DataFrame to save
            output_path: Optional path where to save the CSV file
        """
        save_path = output_path or self.output_path
        if os.path.exists(save_path):
            try:
                os.remove(save_path)
                logger.info(f"Deleted existing file at {save_path}")
            except Exception as ex:
                logger.warning(f"Could not delete existing file at {save_path}: {ex}")
        logger.info(f"Saving data to {save_path} (overwrite)...")
        df.to_csv(save_path, index=False)
        logger.info(f"Data saved successfully to {save_path}")


def main():
    """Main function to generate sensor data."""
    parser = argparse.ArgumentParser(description="Generate synthetic sensor data")
    parser.add_argument(
        "num_rows",
        type=int,
        nargs="?",
        default=100000,
        help="Number of rows to generate (default: 100000)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-01-01",
        help="Start date in YYYY-MM-DD format (default: 2024-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-11-14",
        help="End date in YYYY-MM-DD format (default: 2025-11-14)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/raw/sensor_data.csv",
        help="Output CSV file path (default: data/raw/sensor_data.csv)",
    )

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    logger.info(f"Starting data generation with {args.num_rows} rows")
    logger.info(f"Date range: {args.start_date} to {args.end_date}")
    logger.info(f"Output path: {args.output}")

    generator = SensorDataGenerator(
        num_rows=args.num_rows,
        start_date=start_date,
        end_date=end_date,
        output_path=args.output,
    )
    df = generator.generate_data()
    generator.save_to_csv(df)


if __name__ == "__main__":
    main()
