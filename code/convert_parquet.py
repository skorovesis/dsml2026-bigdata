from __future__ import annotations

from common import CRIME_CSV_PATHS, CRIME_PARQUET_PATH, load_crime_csv, parse_args, time_action
from pyspark.sql import SparkSession


def main() -> None:
    args = parse_args("Convert LA crime CSV files to Parquet on HDFS")
    spark = SparkSession.builder.appName("dsml2026_convert_parquet").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crime_df = load_crime_csv(spark)

    def _write() -> None:
        crime_df.write.mode("overwrite").parquet(CRIME_PARQUET_PATH)

    elapsed = time_action(_write, label="CONVERT")
    print(f"Wrote parquet to: {CRIME_PARQUET_PATH}")
    print(f"Source CSV files: {CRIME_CSV_PATHS}")
    print(f"CONVERT_ELAPSED_SECONDS={elapsed:.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
