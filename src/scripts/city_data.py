import math

import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext 
from pyspark.sql.window import Window 


def main():
    conf = SparkConf().setAppName(f"CityData")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    messages = sql.read.parquet('/user/alxkov19/data/geo/events/event_type=message').\
    withColumn('datetime_correct', F.when(F.col('event.datetime').isNull(),\
    F.col('event.message_ts')).otherwise(F.col('event.datetime')))
    
    reactions = sql.read.parquet('/user/alxkov19/data/geo/events/event_type=reaction')
    subscriptions = sql.read.parquet('/user/alxkov19/data/geo/events/event_type=subscription')

    geo = sql.read.option("delimiter", ";").\
    option("header", "true").csv('/user/alxkov19/geo.csv', header=True)

    geo_converted = geo.withColumn('lat_city', F.regexp_replace(F.col('lat'), ',', '.').\
    cast('double')).withColumn('lng_city', F.regexp_replace(F.col('lng'), ',', '.').\
    cast('double')).drop('lat', 'lng')

    messages_cities = define_city(messages, geo_converted)
    user_city = user_actual_address(messages_cities)
    messages = city_messages_count(messages_cities)
    reactions = city_reactions_count(reactions, user_city)
    subscriptions = city_subscriptions_count(subscriptions, user_city)
    registrations = city_registrations_count(messages_cities)

    city_count(messages, reactions, subscriptions, registrations, geo_converted).\
    write.mode('overwrite').parquet('/user/alxkov19/data/aus/result/cities_data')


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


def user_actual_address(messages_cities):
    
    window = Window().partitionBy(['event.message_from']).\
    orderBy(F.desc('datetime_correct'))
    
    return messages_cities.withColumn("rank", F.row_number().over(window)).\
    filter("rank = 1").drop('rank').select('event.message_from', 'city').\
    withColumnRenamed('message_from', 'user_id').withColumnRenamed('city', 'act_city')


def city_reactions_count(reactions, user_city):
    
    reactions_cities = reactions.\
    join(user_city, reactions.event.reaction_from == user_city.user_id, 'left').\
    withColumnRenamed('act_city', 'city').filter(F.col('city').isNotNull())
    
    reactions_cities_weeks = reactions_cities.\
    withColumn("month", F.trunc(F.col("event.datetime"), "month")).\
    withColumn("week", F.trunc(F.col("event.datetime"), "week")).\
    groupBy('city', 'month', 'week').agg(F.count('city').alias("week_reaction"))
    
    reactions_cities_months = reactions_cities.\
    withColumn("month", F.trunc(F.col("event.datetime"), "month")).\
    groupBy('city', 'month').agg(F.count('city').alias("month_reaction"))
    
    return reactions_cities_weeks.\
    join(reactions_cities_months, ['city', 'month'], 'left').\
    orderBy('city', 'month', 'week')


def city_subscriptions_count(subscriptions, user_city):
    
    subscriptions_cities = subscriptions.\
    join(user_city, subscriptions.event.user == user_city.user_id, 'left').\
    withColumnRenamed('act_city', 'city').filter(F.col('city').isNotNull())
    
    subscriptions_cities_weeks = subscriptions_cities.\
    withColumn("month", F.trunc(F.col("event.datetime"), "month")).\
    withColumn("week", F.trunc(F.col("event.datetime"), "week")).\
    groupBy('city', 'month', 'week').agg(F.count('city').alias("week_subscription"))
    
    subscriptions_cities_months = subscriptions_cities.\
    withColumn("month", F.trunc(F.col("event.datetime"), "month")).\
    groupBy('city', 'month').agg(F.count('city').alias("month_subscription"))
    
    return subscriptions_cities_weeks.\
    join(subscriptions_cities_months, ['city', 'month'], 'left').\
    orderBy('city', 'month', 'week')


def city_messages_count(messages_cities):
    
    messages_cities_weeks = messages_cities.\
    withColumn("month", F.trunc(F.col("datetime_correct"), "month")).\
    withColumn("week", F.trunc(F.col("datetime_correct"), "week")).\
    groupBy('city', 'month', 'week').agg(F.count('city').alias("week_message"))
    
    messages_cities_months = messages_cities.\
    withColumn("month", F.trunc(F.col("datetime_correct"), "month")).\
    groupBy('city', 'month').agg(F.count('city').alias("month_message"))
    
    return messages_cities_weeks.\
    join(messages_cities_months, ['city', 'month'], 'left').\
    orderBy('city', 'month', 'week')


def city_registrations_count(messages_cities):
    
    registrations = messages_cities.\
    withColumn("min_datetime", F.max('datetime_correct').\
    over(Window().partitionBy(['event.message_from']))).\
    where('datetime_correct=min_datetime').\
    drop('event_type').withColumn('event_type', F.lit('registration'))
    
    registrations_cities_weeks = registrations.\
    withColumn("month", F.trunc(F.col("datetime_correct"), "month")).\
    withColumn("week", F.trunc(F.col("datetime_correct"), "week")).\
    groupBy('city', 'month', 'week').agg(F.count('city').alias("week_user"))
    
    registrations_cities_months = registrations.\
    withColumn("month", F.trunc(F.col("datetime_correct"), "month")).\
    groupBy('city', 'month').agg(F.count('city').alias("month_user"))
    
    return registrations_cities_weeks.\
    join(registrations_cities_months, ['city', 'month'], 'left').\
    orderBy('city', 'month', 'week')


def city_count(messages, reactions, subscriptions, registrations, geo):
    
    return messages.join(reactions, ['month', 'week', 'city'], 'left').\
    join(subscriptions, ['month', 'week', 'city'], 'left').\
    join(registrations, ['month', 'week', 'city'], 'left').\
    join(geo.select('city',F.col('id').alias('zone_id')),['city'], 'left').drop('city')


if __name__ == "__main__":
    main()