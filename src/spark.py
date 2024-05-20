from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, count, avg, max as pyspark_max, to_timestamp

from config import SPARK_VERSION, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

if __name__ == '__main__':
    data_schema = StructType([
        StructField('name', StringType()),
        StructField('origin', StringType()),
        StructField('destination', StringType()),
        StructField('time', StringType()),
        StructField('link', StringType()),
        StructField('position', FloatType()),
        StructField('spacing', FloatType()),
        StructField('speed', FloatType()),
    ])

    spark = SparkSession \
        .builder \
        .appName('big-data-sim') \
        .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}') \
        .getOrCreate()

    data = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
        .option('subscribe', KAFKA_TOPIC) \
        .load()

    json_data = data \
        .select(from_json(col('value').cast('string'), data_schema).alias('data')) \
        .withColumn('time', to_timestamp('data.time')) \
        .select('data.*')

    vehicle_stats = json_data \
        .groupby('link') \
        .agg(count('*').alias('vcount'), avg('speed').alias('vspeed'), pyspark_max('time').alias('time')) \
        .select('link', 'vcount', 'vspeed', 'time')

    query = vehicle_stats \
        .writeStream \
        .outputMode('update') \
        .format('console') \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print('Stopped by user. Shutting down...')

    spark.stop()
