import sys
from math import radians, cos, sin, asin, sqrt

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


@F.udf
def get_distance(longit_a, latit_a, longit_b, latit_b):
    # Transform to radians
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a, latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    # Calculate area
    area = sin(dist_latit / 2) ** 2 + cos(latit_a) * cos(latit_b) * sin(dist_longit / 2) ** 2
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    # Calculate Distance
    distance = central_angle * radius
    return abs(round(distance, 2))


def main():
    cross_users_path = sys.argv[1]
    cities_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName(f"NearestUsersFromCrossUsersJob")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities = sql.read.parquet(cities_path).collect()

    for city in cities:
        city_id = city['id']
        cross_users = sql.read.parquet(cross_users_path).filter(f"city_id = {city_id}")
        cross_users.withColumn("dist", get_distance("left_lon", "left_lat", "right_lon", "right_lat")) \
            .filter("dist <= 1.0").drop("left_lon", "left_lat", "right_lon", "right_lat") \
            .write.mode("overwrite").parquet(f"{base_output_path}/city_id={city_id}")


if __name__ == "__main__":
    main()
