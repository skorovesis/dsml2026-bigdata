from __future__ import annotations

import argparse
import os

from common import load_crime, time_action
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def benchmark_q2(spark: SparkSession, fmt: str) -> float:
    user = os.environ.get("DSML_USER", os.environ.get("HADOOP_USER_NAME", "dsml00297"))
    sink = (
        "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/"
        f"{user}/project-output/format_benchmark_q2_{fmt}"
    )
    crime_df = (
        load_crime(spark, fmt)
        .withColumn("occurred_at", F.to_timestamp("DATE OCC"))
        .filter(F.col("occurred_at").isNotNull())
        .withColumn("year", F.year("occurred_at"))
        .withColumn("month", F.month("occurred_at"))
    )
    monthly = crime_df.groupBy("year", "month").count().withColumnRenamed("count", "crime_total")
    ranking_window = Window.partitionBy("year").orderBy(
        F.col("crime_total").desc(), F.col("month").desc()
    )
    result = (
        monthly.withColumn("ranking", F.rank().over(ranking_window))
        .filter(F.col("ranking") <= 3)
        .orderBy("year", F.col("crime_total").desc(), F.col("ranking").desc())
    )

    def _run() -> None:
        result.write.mode("overwrite").parquet(sink)

    return time_action(_run, label=f"Q2_{fmt.upper()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Q2 on CSV vs Parquet")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("dsml2026_format_benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    csv_time = benchmark_q2(spark, "csv")
    parquet_time = benchmark_q2(spark, "parquet")
    print(f"SUMMARY_CSV_SECONDS={csv_time:.3f}")
    print(f"SUMMARY_PARQUET_SECONDS={parquet_time:.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
