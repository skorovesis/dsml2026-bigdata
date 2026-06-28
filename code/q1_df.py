from __future__ import annotations

from common import crime_with_periods, load_crime, parse_args, resolve_output, write_and_time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    args = parse_args("Q1 DataFrame: street-crime share by time-of-day period")
    spark = SparkSession.builder.appName("dsml2026_q1_df").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crime_df = crime_with_periods(load_crime(spark, args.format))

    aggregated = crime_df.groupBy("period").agg(
        F.count("*").alias("total_crimes"),
        F.sum(F.when(F.col("Premis Desc") == "STREET", 1).otherwise(0)).alias("street_crimes"),
    )
    result = (
        aggregated.withColumn(
            "street_pct", F.round(F.col("street_crimes") / F.col("total_crimes") * 100.0, 3)
        )
        .orderBy(F.col("street_pct").desc(), F.col("period"))
        .select("period", "street_pct", "total_crimes", "street_crimes")
    )

    for row in result.collect():
        print((row.period, row.street_pct, row.total_crimes, row.street_crimes))

    output_path = resolve_output(args, spark, "q1_df")
    if output_path:
        write_and_time(result, output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
