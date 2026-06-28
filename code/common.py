from __future__ import annotations

import argparse
import math
import os
import sys
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

HDFS_DATA = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
CRIME_CSV_PATHS = [
    f"{HDFS_DATA}/LA_Crime_Data/LA_Crime_Data_2010_2019.csv",
    f"{HDFS_DATA}/LA_Crime_Data/LA_Crime_Data_2020_2025.csv",
]
CRIME_PARQUET_PATH = (
    "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/"
    f"{os.environ.get('DSML_USER', os.environ.get('HADOOP_USER_NAME', 'dsml00297'))}"
    "/data/LA_Crime_Data/parquet"
)
CENSUS_GEOJSON = f"{HDFS_DATA}/LA_Census_Blocks_2020.geojson"
INCOME_CSV = f"{HDFS_DATA}/LA_income_2021.csv"
POLICE_CSV = f"{HDFS_DATA}/LA_Police_Stations.csv"

PERIOD_ORDER = ["Πρωί", "Απόγευμα", "Βράδυ", "Νύχτα"]


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--output-base",
        default=None,
        help="HDFS base path for outputs (defaults to /user/<DSML_USER>/project-output).",
    )
    parser.add_argument("--output", default=None, help="Explicit output path.")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv")
    parser.add_argument(
        "--join-hint",
        choices=["auto", "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"],
        default="auto",
        help="Join strategy hint for queries that support it.",
    )
    parser.add_argument("--explain", action="store_true", help="Print formatted physical plan.")
    parser.add_argument("--master", default=None)
    return parser.parse_args()


def build_spark(app_name: str, master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
        if master.startswith("local"):
            builder = builder.config("spark.submit.deployMode", "client")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def default_output_base() -> str:
    user = os.environ.get("DSML_USER", os.environ.get("HADOOP_USER_NAME", "dsml00297"))
    return f"hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/{user}/project-output"


def resolve_output(args: argparse.Namespace, spark: SparkSession, suffix: str) -> str | None:
    if args.output:
        return args.output
    if args.output_base:
        return f"{args.output_base.rstrip('/')}/{suffix}_{spark.sparkContext.applicationId}"
    return f"{default_output_base()}/{suffix}_{spark.sparkContext.applicationId}"


def time_action(action_fn, label: str = "QUERY") -> float:
    start = perf_counter()
    action_fn()
    elapsed = perf_counter() - start
    print(f"{label}_ELAPSED_SECONDS={elapsed:.3f}")
    return elapsed


def write_and_time(df: DataFrame, output_path: str, label: str = "QUERY") -> float:
    def _write() -> None:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    return time_action(_write, label)


def period_from_time_occ(time_occ: int | None) -> str | None:
    if time_occ is None:
        return None
    if 500 <= time_occ <= 1159:
        return "Πρωί"
    if 1200 <= time_occ <= 1659:
        return "Απόγευμα"
    if 1700 <= time_occ <= 2059:
        return "Βράδυ"
    if time_occ >= 2100 or time_occ <= 459:
        return "Νύχτα"
    return None


period_from_time_occ_udf = F.udf(period_from_time_occ, StringType())


def load_crime_csv(spark: SparkSession) -> DataFrame:
    frames = [
        spark.read.option("header", True).option("quote", '"').csv(path)
        for path in CRIME_CSV_PATHS
    ]
    return frames[0].unionByName(frames[1], allowMissingColumns=True)


def load_crime(spark: SparkSession, fmt: str = "csv") -> DataFrame:
    if fmt == "parquet":
        return spark.read.parquet(CRIME_PARQUET_PATH)
    return load_crime_csv(spark)


def crime_with_periods(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("time_occ_int", F.col("TIME OCC").cast(IntegerType()))
        .withColumn("period", period_from_time_occ_udf(F.col("time_occ_int")))
        .filter(F.col("period").isNotNull())
    )


def load_census_blocks(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("multiLine", True).json(CENSUS_GEOJSON)
    exploded = raw.selectExpr("explode(features) as feature").select("feature.*")
    property_names = exploded.schema["properties"].dataType.fieldNames()
    return exploded.select(
        [F.col(f"properties.{name}").alias(name) for name in property_names]
    )


def load_income(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("header", True).option("sep", ";").csv(INCOME_CSV)
    return raw.select(
        F.regexp_replace(F.col("Zip Code"), r"[^0-9]", "").alias("zip_code"),
        F.regexp_replace(F.col("Estimated Median Income"), r"[^0-9.]", "")
        .cast(DoubleType())
        .alias("median_income"),
    ).filter(F.col("zip_code") != "")


def load_police_stations(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", True)
        .csv(POLICE_CSV)
        .select(
            F.col("DIVISION").alias("division"),
            F.col("X").cast(DoubleType()).alias("station_lon"),
            F.col("Y").cast(DoubleType()).alias("station_lat"),
        )
    )


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return float("nan")
    radius_miles = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * radius_miles * math.asin(math.sqrt(a))


haversine_udf = F.udf(haversine_miles, DoubleType())
