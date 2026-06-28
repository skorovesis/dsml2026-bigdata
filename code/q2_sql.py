from __future__ import annotations

from common import load_crime, parse_args, resolve_output, write_and_time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    args = parse_args("Q2 Spark SQL: top-3 crime months per year")
    spark = SparkSession.builder.appName("dsml2026_q2_sql").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crime_df = (
        load_crime(spark, args.format)
        .withColumn("occurred_at", F.to_timestamp("DATE OCC"))
        .filter(F.col("occurred_at").isNotNull())
        .withColumn("year", F.year("occurred_at"))
        .withColumn("month", F.month("occurred_at"))
    )
    crime_df.createOrReplaceTempView("crime")

    result = spark.sql(
        """
        WITH monthly AS (
            SELECT year, month, COUNT(*) AS crime_total
            FROM crime
            GROUP BY year, month
        ),
        ranked AS (
            SELECT
                year,
                month,
                crime_total,
                RANK() OVER (
                    PARTITION BY year
                    ORDER BY crime_total DESC, month DESC
                ) AS ranking
            FROM monthly
        )
        SELECT year, month, crime_total, ranking
        FROM ranked
        WHERE ranking <= 3
        ORDER BY year ASC, crime_total DESC, ranking DESC
        """
    )

    for row in result.collect():
        print((row.year, row.month, row.crime_total, row.ranking))

    output_path = resolve_output(args, spark, "q2_sql")
    if output_path:
        write_and_time(result, output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
