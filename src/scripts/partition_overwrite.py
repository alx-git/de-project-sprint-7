import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext 


def main():

    conf = SparkConf().setAppName(f"PartitionOverwrite")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)


    events = sql.read.parquet('/user/master/data/geo/events').sample(0.05)

    events.write.partitionBy("event_type").mode('overwrite').\
    parquet('/user/alxkov19/data/geo/events')


if __name__ == "__main__":
    main()