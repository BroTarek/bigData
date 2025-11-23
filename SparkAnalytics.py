
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, count, sum as Fsum, from_json, schema_of_json

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
INPUT_TOPIC = os.getenv('SPARK_INPUT_TOPIC')
POSTGRES_URL = os.getenv('POSTGRES_URL')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

spark = SparkSession.builder \
    .appName('AirplaneCrashAnalytics') \
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0') \
    .getOrCreate()

raw_df = spark.read.format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
    .option('subscribe', INPUT_TOPIC) \
    .option('startingOffsets', 'earliest') \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

sample = json_df.select('json_str').limit(1).collect()
if sample:
    schema = schema_of_json(json_df.select('json_str').first()[0])
    df = json_df.select(from_json(col('json_str'), schema).alias('data')).select('data.*')
else:
    df = json_df.selectExpr('from_json(json_str, "event_id STRING, timestamp STRING, airline STRING, flight_number STRING, aircraft_model STRING, aircraft_type STRING, origin STRING, destination STRING, latitude DOUBLE, longitude DOUBLE, city STRING, country STRING, fatalities INT, injuries INT, damage STRING, cause STRING") as data').select('data.*')

# Analytics
by_country = df.groupBy('country').count().withColumnRenamed('count', 'crash_count')
by_model = df.groupBy('aircraft_model').agg(count('*').alias('crash_count'), Fsum('fatalities').alias('total_fatalities'))
hotspots = df.withColumn('lat_bucket', round(col('latitude')*10)/10).withColumn('lon_bucket', round(col('longitude')*10)/10)
hotspots = hotspots.groupBy('lat_bucket','lon_bucket').count().withColumnRenamed('count','crash_count')

# Write to Postgres
for table, data in [('crashes_by_country', by_country), ('crashes_by_model', by_model), ('crash_hotspots', hotspots)]:
    data.write.format('jdbc').options(
        url=POSTGRES_URL,
        driver='org.postgresql.Driver',
        dbtable=table,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    ).mode('overwrite').save()

print('Analytics written to Postgres')
spark.stop()