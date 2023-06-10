import math

import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext 
from pyspark.sql.window import Window 


def main():
    conf = SparkConf().setAppName(f"FriendsReco")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    messages = sql.read.parquet('/user/alxkov19/data/geo/events/event_type=message').\
    withColumn('datetime_correct', F.when(F.col('event.datetime').isNull(),\
    F.col('event.message_ts')).otherwise(F.col('event.datetime')))
    
    subscriptions = sql.read.parquet('/user/alxkov19/data/geo/events/event_type=subscription')

    geo = sql.read.option("delimiter", ";").\
    option("header", "true").csv('/user/alxkov19/geo.csv', header=True)

    geo_converted = geo.withColumn('lat_city', F.regexp_replace(F.col('lat'), ',', '.').\
    cast('double')).withColumn('lng_city', F.regexp_replace(F.col('lng'), ',', '.').\
    cast('double')).drop('lat', 'lng')

    friends_reco(messages, subscriptions, geo_converted).\
    write.mode('overwrite').parquet('/user/alxkov19/data/aus/result/friends_reco')


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


def friends_reco(messages, subscriptions, geo):
    users_coordinates = user_coordinates(messages)
    users_cities = user_actual_address(define_city(messages, geo))
    
    users_subscriptions = subscriptions.groupBy("event.user").\
    agg(F.collect_list('event.subscription_channel').alias("channels")).\
    withColumnRenamed('user', 'user_id')
    
    users_contacts = messages.\
    select(F.col('event.message_from').alias('user_id'), F.col('event.message_to').\
    alias('contact_id')).\
    union(messages.select(F.col('event.message_to').\
    alias('user_id'), F.col('event.message_from').alias('contact_id'))).\
    distinct().groupBy("user_id").agg(F.collect_list('contact_id').alias("contacts")).\
    filter(F.col('user_id').isNotNull())
    
    users_info = users_contacts.join(users_subscriptions, ['user_id'], 'left').\
    join(users_coordinates, ['user_id'], 'left')
    
    users_info_mapping = users_info.\
    crossJoin(users_info.withColumnRenamed("user_id", "user_id_right").\
    withColumnRenamed("contacts", "contacts_right").withColumnRenamed("channels", "channels_right").\
    withColumnRenamed("lat", "lat_right").withColumnRenamed("lon", "lon_right")).\
    where('user_id != user_id_right').\
    withColumn('distance', measure_distance(F.col('lat'), F.col('lon'), F.col('lat_right'),\
    F.col('lon_right'))).where('distance <= 1').\
    withColumn("crosses_channels", F.arrays_overlap('channels', 'channels_right')).\
    where('crosses_channels == true').\
    withColumn("crosses_users", F.array_contains('contacts', F.col('user_id_right'))).\
    where('crosses_users == false').\
    select(F.col('user_id').alias('user_left'), F.col('user_id_right').alias('user_right')).\
    withColumn("distinct_cols", F.array_distinct(F.array("user_left", "user_left"))).\
    dropDuplicates(["distinct_cols"])
    
    return users_info_mapping.\
    join(users_cities, users_info_mapping.user_left == users_cities.user_id, 'left').\
    join(geo, users_cities.act_city == geo.city, 'left').\
    withColumn('processed_dttm', F.from_utc_timestamp(F.current_timestamp(), "UTC")).\
    withColumn('local_time', F.from_utc_timestamp(F.col('processed_dttm'), F.col('timezone'))).\
    select('user_left', 'user_right', 'processed_dttm', F.col('id').alias('zone_id'), 'local_time')


if __name__ == "__main__":
    main()

