from __future__ import annotations

from common import (
    crime_with_periods,
    load_crime,
    parse_args,
    period_from_time_occ,
    resolve_output,
    time_action,
)
from pyspark.sql import SparkSession


def main() -> None:
    args = parse_args("Q1 RDD: street-crime share by time-of-day period")
    spark = SparkSession.builder.appName("dsml2026_q1_rdd").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crime_rdd = (
        load_crime(spark, args.format)
        .select("TIME OCC", "Premis Desc")
        .rdd
        .map(lambda row: (int(row["TIME OCC"]), row["Premis Desc"]))
        .filter(lambda x: x[0] is not None)
        .map(lambda x: (period_from_time_occ(x[0]), x[1]))
        .filter(lambda x: x[0] is not None)
    )

    period_totals = crime_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
    street_totals = (
        crime_rdd.filter(lambda x: x[1] == "STREET")
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    joined = period_totals.leftOuterJoin(street_totals).map(
        lambda x: (
            x[0],
            (x[1][1] or 0) / x[1][0] * 100.0,
            x[1][0],
            x[1][1] or 0,
        )
    )
    results = joined.sortBy(lambda x: (-x[1], x[0])).collect()

    for period, pct, total, street in results:
        print((period, round(pct, 3), total, street))

    output_path = resolve_output(args, spark, "q1_rdd")
    if output_path:
        result_df = spark.createDataFrame(
            [(p, round(pct, 3), total, street) for p, pct, total, street in results],
            ["period", "street_pct", "total_crimes", "street_crimes"],
        )

        def _write() -> None:
            result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

        time_action(_write)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
