from __future__ import annotations

from common import (
    load_census_blocks,
    load_income,
    parse_args,
    resolve_output,
    write_and_time,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def main() -> None:
    args = parse_args("Q3 DataFrame: per-capita income by zip code")
    spark = SparkSession.builder.appName("dsml2026_q3_df").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    population_by_zip = (
        load_census_blocks(spark)
        .select(
            F.col("ZCTA20").alias("zip_code"),
            F.col("POP20").cast("long").alias("population_2020"),
        )
        .filter(F.col("zip_code").isNotNull())
        .groupBy("zip_code")
        .agg(F.sum("population_2020").alias("population_2020"))
        .filter(F.col("population_2020") > 0)
    )

    income_df = load_income(spark)

    if args.join_hint == "broadcast":
        joined = population_by_zip.join(broadcast(income_df), "zip_code", "inner")
    elif args.join_hint == "merge":
        joined = population_by_zip.hint("merge").join(income_df.hint("merge"), "zip_code", "inner")
    elif args.join_hint == "shuffle_hash":
        joined = population_by_zip.hint("shuffle_hash").join(
            income_df.hint("shuffle_hash"), "zip_code", "inner"
        )
    elif args.join_hint == "shuffle_replicate_nl":
        joined = population_by_zip.hint("shuffle_replicate_nl").join(
            income_df.hint("shuffle_replicate_nl"), "zip_code", "inner"
        )
    else:
        joined = population_by_zip.join(income_df, "zip_code", "inner")

    result = (
        joined.withColumn(
            "per_capita_income_2020_2021",
            F.round(F.col("median_income") / F.col("population_2020"), 2),
        )
        .select("zip_code", "population_2020", "median_income", "per_capita_income_2020_2021")
        .orderBy("zip_code")
    )

    if args.explain:
        result.explain("formatted")

    preview = result.limit(20).collect()
    for row in preview:
        print((row.zip_code, row.population_2020, row.median_income, row.per_capita_income_2020_2021))
    print(f"TOTAL_ZIPS={result.count()}")

    output_path = resolve_output(args, spark, "q3_df")
    if output_path:
        write_and_time(result, output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
