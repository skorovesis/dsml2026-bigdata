from __future__ import annotations

from common import load_crime, parse_args, resolve_output, write_and_time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main() -> None:
    args = parse_args("Q2 DataFrame: top-3 crime months per year")
    spark = SparkSession.builder.appName("dsml2026_q2_df").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crime_df = (
        load_crime(spark, args.format)
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
        .select("year", "month", "crime_total", "ranking")
    )

    for row in result.collect():
        print((row.year, row.month, row.crime_total, row.ranking))

    output_path = resolve_output(args, spark, "q2_df")
    if output_path:
        write_and_time(result, output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
