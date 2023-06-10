import math

import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext 
from pyspark.sql.window import Window 


def main():
    conf = SparkConf().setAppName(f"UserData")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    messages = sql.read.parquet('/user/alxkov19/data/geo/events/event_type=message').\
    withColumn('datetime_correct', F.when(F.col('event.datetime').isNull(),\
    F.col('event.message_ts')).otherwise(F.col('event.datetime')))
    
    geo = sql.read.option("delimiter", ";").\
    option("header", "true").csv('/user/alxkov19/geo.csv', header=True)

    geo_converted = geo.withColumn('lat_city', F.regexp_replace(F.col('lat'), ',', '.').\
    cast('double')).withColumn('lng_city', F.regexp_replace(F.col('lng'), ',', '.').\
    cast('double')).drop('lat', 'lng')

    messages_cities = define_city(messages, geo_converted)
    actual_address = user_actual_address(messages_cities)
    home_address = user_home_address(messages_cities)
    trips = user_trips(messages_cities)
    local_time = user_local_time(messages_cities)

    user_data(actual_address, home_address, trips, local_time).\
    write.mode('overwrite').parquet('/user/alxkov19/data/aus/result/user_data')


def measure_distance(lat1, lon1, lat2, lon2):
    
    radius_earth = 6371
    radians_converter = math.pi/180
    
    return 2*radius_earth*\
    F.asin(F.sqrt(F.pow(F.sin((lat2*radians_converter - lat1*radians_converter)/F.lit(2)),2)+\
    F.cos(lat1*radians_converter)*F.cos(lat2*radians_converter)*\
    F.pow(F.sin((lon2*radians_converter - lon1*radians_converter)/F.lit(2)),2)))
   

def define_city(messages, geo):
    
    window = Window().partitionBy(['event.message_id', 'event.message_from']).\
    orderBy(F.asc('distance'))
    
    messages_cities = messages.join(geo).\
    withColumn('distance',\
    measure_distance(F.col('lat'), F.col('lon'), F.col('lat_city'), F.col('lng_city'))).\
    withColumn("rank", F.row_number().over(window)).filter("rank = 1").drop('rank')
    
    return messages_cities


def user_coordinates(messages_cities):
    
    window = Window().partitionBy(['event.message_from']).orderBy(F.desc('datetime_correct'))
    
    return messages_cities.\
    withColumn("rank", F.row_number().over(window)).filter("rank = 1").\
    drop('rank').select('event.message_from', 'lat', 'lon').\
    withColumnRenamed('message_from', 'user_id')


def user_actual_address(messages_cities):
    
    window = Window().partitionBy(['event.message_from']).\
    orderBy(F.desc('datetime_correct'))
    
    return messages_cities.withColumn("rank", F.row_number().over(window)).\
    filter("rank = 1").drop('rank').select('event.message_from', 'city').\
    withColumnRenamed('message_from', 'user_id').withColumnRenamed('city', 'act_city')


def user_home_address(messages_cities):
    
    window = Window().partitionBy(['event.message_from']).orderBy(F.desc('datetime_correct'))
        
    return messages_cities.withColumn("previous_city",F.lag("city", -1).over(window)).\
    withColumn("next_city",F.lag("city", 1).over(window)).\
    select('event.message_from','datetime_correct','city','previous_city','next_city').na.fill("na").\
    where("previous_city != next_city").\
    withColumn("previous_datetime",F.lag("datetime_correct", -1).\
    over(Window().partitionBy(['message_from']).orderBy(F.desc('datetime_correct')))).\
    where("previous_city = city").\
    withColumn('datetime_difference', F.datediff(F.to_timestamp(F.col("datetime_correct"),\
    "yyyy-MM-dd HH:mm:ss"),F.to_timestamp(F.col("previous_datetime"), "yyyy-MM-dd HH:mm:ss"))).\
    where("datetime_difference >= 27").\
    withColumn("max_datetime",F.max("datetime_correct").over(Window().partitionBy(['message_from']))).\
    where("datetime_correct = max_datetime").select('message_from', 'city').\
    withColumnRenamed('message_from', 'user_id').withColumnRenamed('city', 'home_city')


def user_trips(messages_cities):
    
    window_first = Window().partitionBy(['event.message_from']).orderBy(F.desc('datetime_correct'))
    
    return messages_cities.withColumn("previous_city",F.lag("city", -1).over(window_first)).\
    select('event.message_from','datetime_correct','city','previous_city').na.fill("na").\
    where("city != previous_city").groupBy("message_from").\
    agg(F.collect_list('city').alias("travel_array")).\
    withColumn("travel_count", F.size(F.col("travel_array"))).orderBy('travel_count').\
    withColumnRenamed('message_from', 'user_id')


def user_local_time(messages_cities):
    
    window = Window().partitionBy(['event.message_from'])
    
    return messages_cities.\
    withColumn('local_time', F.from_utc_timestamp(F.col("datetime_correct"),F.col('timezone'))).\
    withColumn("max_datetime", F.max('datetime_correct').over(window)).\
    select('event.message_from', 'local_time').where("datetime_correct = max_datetime").\
    withColumnRenamed('message_from', 'user_id')


def user_data(actual_address, home_address, trips, local_time):
    
    return actual_address.join(home_address, ['user_id'], 'left').join(trips, ['user_id'], 'left').\
    join(local_time, ['user_id'], 'left')


if __name__ == "__main__":
    main()