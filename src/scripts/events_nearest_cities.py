import sys
from math import radians, cos, sin, asin, sqrt, inf

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType


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


class GeoUtils:

    def __init__(self, cities_list) -> None:
        self.cities_list = cities_list

    def get_nearest_city_id(self, lon, lat):
        if not lon or not lat:
            return None
        min_distance = inf
        result = None
        for city in self.cities_list:
            distance = get_distance(lon, lat, city["city_lon"], city["city_lat"])
            if distance < min_distance:
                min_distance = distance
                result = city
        return result["city_id"]


def main():
    date = sys.argv[1]
    events_path = sys.argv[2]
    cities_path = sys.argv[3]
    output_path = sys.argv[4]

    conf = SparkConf().setAppName(f"EventsNearestCitiesJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities = sql.read.parquet(cities_path)
    geo_utils = GeoUtils(cities.collect())
    nearest_city_id = F.udf(lambda lon, lat: geo_utils.get_nearest_city_id(lon, lat), IntegerType())

    events = sql.read.parquet(events_path).filter(F.col("date") == date)

    events.withColumn("city_id", nearest_city_id("lon", "lat")) \
        .write.option("header", True) \
        .partitionBy("event_type") \
        .mode("overwrite") \
        .parquet(f"{output_path}/date={date}")


if __name__ == "__main__":
    main()
