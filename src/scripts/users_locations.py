import sys

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Window


def main():
    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    consecutive_days_bound = int(sys.argv[3])
    output_path = sys.argv[4]

    conf = SparkConf().setAppName(f"UsersLocationsMartJob")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities = sql.read.parquet(cities_path)
    events = sql.read.parquet(events_path) \
        .filter(F.col("event_type") == "message") \
        .select(F.col("event.message_from").alias("user_id"),
                F.col("city_id"),
                F.coalesce("event.datetime", "event.message_ts").cast("timestamp").alias("ts")) \
        .join(cities, "city_id", "left").select("user_id", "ts", "city_name")

    w = Window().partitionBy("user_id")

    travels = events.withColumn("prev_city", F.lag("city_name").over(w.orderBy("ts"))) \
        .filter("prev_city <> city_name") \
        .groupBy("user_id").agg(F.collect_list("city_name").alias("travel_array"),
                                F.count("city_name").alias("travel_count"))

    home_cities = events.withColumn("date", F.to_date("ts")) \
        .withColumn("rank", F.rank().over(w.orderBy("date"))) \
        .withColumn("grp", F.date_sub("date", "rank")) \
        .groupBy("user_id", "city_name", "grp").agg(F.min("date").alias("min"), F.max("date").alias("max")) \
        .withColumn("cons_days", F.least(F.datediff("max", "min"), F.lit(consecutive_days_bound))) \
        .withColumn("priority", F.row_number().over(w.orderBy(F.col("cons_days").desc(), F.col("max").desc()))) \
        .filter("priority = 1").select("user_id", F.col("city_name").alias("home_city"))

    # TODO сделать в справочнике городов таймзону
    events.withColumn("row_num", F.row_number().over(w.orderBy(F.col("ts").desc()))) \
        .filter("row_num = 1") \
        .select(F.col("user_id"),
                F.col("city_name").alias("act_city"),
                F.from_utc_timestamp(F.col("ts"), F.lit("Australia/Sydney")).alias("local_time")) \
        .join(travels, "user_id", "left") \
        .join(home_cities, "user_id", "left") \
        .write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    main()
