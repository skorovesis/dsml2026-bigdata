from __future__ import annotations

from common import load_census_blocks, load_income, parse_args, resolve_output, time_action
from pyspark.sql import SparkSession


def main() -> None:
    args = parse_args("Q3 RDD: per-capita income by zip code")
    spark = SparkSession.builder.appName("dsml2026_q3_rdd").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    population_rdd = (
        load_census_blocks(spark)
        .select("ZCTA20", "POP20")
        .rdd.map(lambda row: (str(row.ZCTA20), int(row.POP20 or 0)))
        .filter(lambda x: x[0] and x[1] > 0)
        .reduceByKey(lambda a, b: a + b)
    )

    income_rdd = (
        load_income(spark)
        .rdd.map(lambda row: (row.zip_code, float(row.median_income) if row.median_income is not None else None))
        .filter(lambda x: x[1] is not None)
    )

    joined = population_rdd.join(income_rdd).map(
        lambda x: (
            x[0],
            x[1][0],
            x[1][1],
            round(x[1][1] / x[1][0], 2),
        )
    )
    results = joined.sortBy(lambda x: x[0]).collect()

    for zip_code, population, income, per_capita in results[:20]:
        print((zip_code, population, income, per_capita))
    print(f"TOTAL_ZIPS={len(results)}")

    output_path = resolve_output(args, spark, "q3_rdd")
    if output_path:
        result_df = spark.createDataFrame(
            results,
            ["zip_code", "population_2020", "median_income", "per_capita_income_2020_2021"],
        )

        def _write() -> None:
            result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

        time_action(_write)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
