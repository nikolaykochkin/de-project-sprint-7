import sys

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Window


def main():
    events_path = sys.argv[1]
    output_path = sys.argv[2]

    conf = SparkConf().setAppName(f"CrossUsersByLastMessageJob")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    last_messages = sql.read.parquet(events_path) \
        .filter(F.col("event_type") == "message") \
        .select(F.col("event.message_from").alias("user_id"), "lat", "lon", "city_id",
                F.coalesce("event.datetime", "event.message_ts").cast("timestamp").alias("ts")) \
        .withColumn("row", F.row_number().over(Window().partitionBy("user_id").orderBy("ts"))) \
        .filter("row = 1").drop("ts", "row").cache()

    dialogs = sql.read.parquet(events_path) \
        .filter(F.col("event_type") == "message") \
        .filter(F.col("event.message_from").isNotNull()) \
        .filter(F.col("event.message_to").isNotNull()) \
        .select("event.message_from", "event.message_to") \
        .distinct().cache()

    all_connections = dialogs.selectExpr("message_from as left_user", "message_to as right_user") \
        .union(dialogs.selectExpr("message_to as left_user", "message_from as right_user")) \
        .distinct().cache()

    left_messages = last_messages.selectExpr("city_id", "user_id as left_user", "lat as left_lat", "lon as left_lon")
    right_messages = last_messages.selectExpr("city_id", "user_id as right_user", "lat as right_lat",
                                              "lon as right_lon")

    left_messages.join(right_messages, ["city_id", left_messages.left_user != right_messages.right_user]) \
        .join(all_connections, ["left_user", "right_user"], "anti") \
        .write.partitionBy("city_id").mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    main()
