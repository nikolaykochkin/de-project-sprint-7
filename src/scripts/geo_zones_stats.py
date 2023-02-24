import datetime
import sys
from calendar import monthrange

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Window


def get_month_start_end(date):
    d = datetime.date.fromisoformat(date)
    start_date = datetime.date(d.year, d.month, 1)
    end_date = datetime.date(d.year, d.month, monthrange(d.year, d.month)[1])
    return start_date, end_date


def main():
    events_path = sys.argv[1]
    output_path = sys.argv[2]

    conf = SparkConf().setAppName(f"GeoZonesStatsJob")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events = sql.read.parquet(events_path) \
        .filter(F.col("city_id").isNotNull())

    unique_users_count = events.filter(F.col("event_type") == "message") \
        .select(F.col("date"),
                F.date_trunc("month", "date").alias("month"),
                F.date_trunc("week", "date").alias("week"),
                F.col("city_id").alias("zone_id"),
                F.col("event.message_from").alias("user")) \
        .withColumn("first_month", F.row_number().over(Window().partitionBy("user", "month").orderBy("date"))) \
        .withColumn("first_week", F.row_number().over(Window().partitionBy("user", "week").orderBy("date"))) \
        .filter("first_month = 1 OR first_week = 1") \
        .select("month", "week", "zone_id",
                F.when(F.col("first_month").eqNullSafe(1), 1).otherwise(0).alias("month_user"),
                F.when(F.col("first_week").eqNullSafe(1), 1).otherwise(0).alias("week_user")) \
        .groupBy("month", "week", "zone_id") \
        .agg(F.sum("month_user").alias("month_user"), F.sum("week_user").alias("week_user")) \
        .select("month", "week", "zone_id",
                F.create_map(F.lit("month_user"), "month_user", F.lit("week_user"), "week_user").alias("map")) \
        .select("month", "week", "zone_id", F.explode("map").alias("type", "count"))

    events.select(F.date_trunc("month", "date").alias("month"),
                  F.date_trunc("week", "date").alias("week"),
                  F.col("city_id").alias("zone_id"),
                  F.col("event_type").alias("type")) \
        .groupBy("month", "week", "zone_id", "type").count() \
        .union(unique_users_count) \
        .groupBy("month", "week", "zone_id").pivot("type").sum("count") \
        .select("month", "week", "zone_id",
                F.col("message").alias("week_message"),
                F.col("reaction").alias("week_reaction"),
                F.col("subscription").alias("week_subscription"),
                F.col("week_user").alias("week_user"),
                F.col("message").alias("month_message"),
                F.col("reaction").alias("month_reaction"),
                F.col("subscription").alias("month_subscription"),
                F.col("month_user").alias("month_user")) \
        .write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    main()
