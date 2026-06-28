from __future__ import annotations

from common import (
    haversine_udf,
    load_crime,
    load_police_stations,
    parse_args,
    resolve_output,
    write_and_time,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window


def main() -> None:
    args = parse_args("Q4 DataFrame: crimes nearest police division")
    spark = SparkSession.builder.appName("dsml2026_q4_df").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crimes = (
        load_crime(spark, args.format)
        .select(
            F.col("DR_NO").alias("crime_id"),
            F.col("LAT").cast("double").alias("crime_lat"),
            F.col("LON").cast("double").alias("crime_lon"),
        )
        .filter(F.col("crime_lat").isNotNull() & F.col("crime_lon").isNotNull())
    )

    stations = load_police_stations(spark)

    if args.join_hint == "broadcast":
        pairs = crimes.crossJoin(broadcast(stations))
    elif args.join_hint == "merge":
        pairs = crimes.hint("merge").crossJoin(stations.hint("merge"))
    elif args.join_hint == "shuffle_hash":
        pairs = crimes.hint("shuffle_hash").crossJoin(stations.hint("shuffle_hash"))
    elif args.join_hint == "shuffle_replicate_nl":
        pairs = crimes.hint("shuffle_replicate_nl").crossJoin(
            stations.hint("shuffle_replicate_nl")
        )
    else:
        pairs = crimes.crossJoin(broadcast(stations))

    with_distance = pairs.withColumn(
        "distance_miles",
        haversine_udf(
            F.col("crime_lat"),
            F.col("crime_lon"),
            F.col("station_lat"),
            F.col("station_lon"),
        ),
    )

    nearest_window = Window.partitionBy("crime_id").orderBy(F.col("distance_miles").asc())
    nearest = (
        with_distance.withColumn("rn", F.row_number().over(nearest_window))
        .filter(F.col("rn") == 1)
        .select("crime_id", "division", "distance_miles")
    )

    result = (
        nearest.groupBy("division")
        .agg(
            F.count("*").alias("crime_count"),
            F.round(F.avg("distance_miles"), 3).alias("average_distance"),
        )
        .orderBy(F.col("crime_count").desc())
    )

    if args.explain:
        result.explain("formatted")

    for row in result.collect():
        print((row.division, row.average_distance, row.crime_count))

    output_path = resolve_output(args, spark, "q4_df")
    if output_path:
        write_and_time(result, output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
