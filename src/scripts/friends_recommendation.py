import datetime
import sys

import pyspark.sql.functions as F
import pytz
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def main():
    nearest_users_path = sys.argv[1]
    events_path = sys.argv[2]
    output_path = sys.argv[3]

    conf = SparkConf().setAppName(f"NearestUsersFromCrossUsersJob")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    nu = sql.read.parquet(nearest_users_path).cache()

    user_subs = sql.read.parquet(events_path) \
        .filter(F.col("event_type") == "subscription") \
        .filter(F.col("event.user").isNotNull()) \
        .filter(F.col("event.subscription_channel").isNotNull()) \
        .select(F.col("event.user").alias("user_id"), "event.subscription_channel") \
        .groupBy("user_id").agg(F.collect_set("subscription_channel").alias("subs")) \
        .cache()

    now = datetime.datetime.utcnow()
    datetime.tzinfo()

    nu.join(user_subs, nu.left_user == user_subs.user_id, "left") \
        .drop("user_id").withColumnRenamed("subs", "left_subs") \
        .join(user_subs, nu.right_user == user_subs.user_id, "left") \
        .drop("user_id").withColumnRenamed("subs", "right_subs") \
        .filter(F.arrays_overlap("left_subs", "right_subs")) \
        .selectExpr("left_user", "right_user", "city_id as zone_id") \
        .withColumn("processed_dttm", F.lit(now)) \
        .withColumn("local_time", F.lit(now.astimezone(pytz.timezone("Australia/Sydney")))) \
        .write.mode("append").parquet(output_path)


if __name__ == "__main__":
    main()
