import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as Fsum, from_json, to_timestamp
from pyspark.sql.functions import avg, min, max, hour, month, year, when, collect_set, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampType

# Environment variables (same)
KAFKA_BOOTSTRAP = 'host.docker.internal:9092'
INPUT_TOPIC = 'produceToSpark'
POSTGRES_URL = 'jdbc:postgresql://localhost:5432/airplane_analytics'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'

print("Starting Spark Analytics...")
print(f"Kafka: {KAFKA_BOOTSTRAP}")
print(f"Topic: {INPUT_TOPIC}")
print(f"PostgreSQL: {POSTGRES_URL}")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName('AirplaneCrashAnalytics') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    # Read from Kafka
    print("Reading from Kafka topic...")
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert to string
    json_df = df.selectExpr("CAST(value AS STRING) as json_str")
    
    message_count = json_df.count()
    print(f"Read {message_count} messages from Kafka")

    if message_count == 0:
        print("No messages found in Kafka topic")
        spark.stop()
        exit(1)

    # CORRECTED SCHEMA - matches your actual data
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),  # Changed to StringType
        StructField("airline", StringType(), True),
        StructField("flight_number", StringType(), True),
        StructField("aircraft_model", StringType(), True),
        StructField("aircraft_type", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("fatalities", StringType(), True),
        StructField("injuries", StringType(), True),
        StructField("damage", StringType(), True),
        StructField("cause", StringType(), True)
    ])

    # Parse JSON
    parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
    print(f"Parsed {parsed_df.count()} records")
    
    # Show schema to debug
    parsed_df.printSchema()
    
    # Show sample data
    print("Sample data:")
    parsed_df.show(5, truncate=False)

    # Clean and convert data types
    from pyspark.sql.functions import coalesce, lit, to_timestamp
    
    processed_df = parsed_df \
        .withColumn("timestamp_ts", 
                   to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("fatalities_int", 
                   coalesce(col("fatalities").cast(IntegerType()), lit(0))) \
        .withColumn("injuries_int", 
                   coalesce(col("injuries").cast(IntegerType()), lit(0))) \
        .withColumn("latitude_double", 
                   coalesce(col("latitude").cast(DoubleType()), lit(0.0))) \
        .withColumn("longitude_double", 
                   coalesce(col("longitude").cast(DoubleType()), lit(0.0))) \
        .withColumn("year", year(col("timestamp_ts"))) \
        .withColumn("month", month(col("timestamp_ts"))) \
        .withColumn("hour", hour(col("timestamp_ts"))) \
        .withColumn("damage_severity", 
                   when(col("damage").contains("Major"), 2)
                   .when(col("damage").contains("Minor"), 1)
                   .when(col("damage").contains("Hull Loss"), 3)
                   .otherwise(0)) \
        .withColumn("has_casualties", 
                   when((col("fatalities_int") > 0) | (col("injuries_int") > 0), "YES")
                   .otherwise("NO")) \
        .withColumn("region",
                   when(col("country").isin(["USA", "Canada", "Mexico"]), "NORTH_AMERICA")
                   .when(col("country").isin(["UK", "France", "Germany"]), "EUROPE")
                   .when(col("country").isin(["China", "India", "Japan"]), "ASIA")
                   .when(col("country").isin(["Brazil", "Argentina"]), "SOUTH_AMERICA")
                   .when(col("country").isin(["Australia"]), "OCEANIA")
                   .when(col("country").isin(["Egypt"]), "AFRICA")
                   .otherwise("OTHER"))

    # Remove duplicates
    unique_df = processed_df.dropDuplicates(["event_id"])
    final_count = unique_df.count()
    print(f"After removing duplicates: {final_count} unique records")

    if final_count == 0:
        print("No unique records found")
        spark.stop()
        exit(1)

    # =================== BASIC ANALYTICS ===================
    print("\n=== Running Basic Analytics ===")
    
    # 1. Crashes by country
    by_country = unique_df.groupBy("country").agg(
        count("*").alias("crash_count"),
        Fsum("fatalities_int").alias("total_fatalities"),
        Fsum("injuries_int").alias("total_injuries")
    ).orderBy(col("crash_count").desc())
    
    print("Top 10 countries by crash count:")
    by_country.show(10, truncate=False)

    # 2. Crashes by damage level
    by_damage = unique_df.groupBy("damage").agg(
        count("*").alias("incident_count"),
        avg("fatalities_int").alias("avg_fatalities"),
        avg("injuries_int").alias("avg_injuries"),
        countDistinct("aircraft_model").alias("models_affected")
    ).orderBy(col("incident_count").desc())
    
    print("\nCrashes by damage level:")
    by_damage.show(truncate=False)

    # 3. Crashes by airline
    by_airline = unique_df.groupBy("airline").agg(
        count("*").alias("incident_count"),
        Fsum("fatalities_int").alias("total_fatalities"),
        collect_set("aircraft_model").alias("aircraft_used")
    ).orderBy(col("incident_count").desc())
    
    print("\nTop airlines by incident count:")
    by_airline.show(10, truncate=False)

    # 4. Crashes by region
    by_region = unique_df.groupBy("region").agg(
        count("*").alias("crash_count"),
        Fsum("fatalities_int").alias("total_fatalities"),
        
        countDistinct("country").alias("countries_in_region")
    ).orderBy(col("crash_count").desc())
    
    print("\nCrashes by region:")
    by_region.show(truncate=False)

    # 6. Aircraft model analysis
    by_aircraft = unique_df.groupBy("aircraft_model").agg(
        count("*").alias("crash_count"),
        Fsum("fatalities_int").alias("total_fatalities"),
        collect_set("airline").alias("operating_airlines")
    ).orderBy(col("crash_count").desc()).limit(10)
    
    print("\nTop aircraft models by crashes:")
    by_aircraft.show(truncate=False)

    # =================== WRITE TO POSTGRESQL ===================
    print("\n=== Writing results to PostgreSQL ===")
    
    # Write basic tables
    try:
        by_country.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "crashes_by_country") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("overwrite") \
            .save()
        print("✓ crashes_by_country written")
    except Exception as e:
        print(f"⚠ Could not write crashes_by_country: {e}")

    try:
        by_damage.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "crashes_by_damage") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("overwrite") \
            .save()
        print("✓ crashes_by_damage written")
    except Exception as e:
        print(f"⚠ Could not write crashes_by_damage: {e}")


    print("\n" + "="*60)
    print("✓ ANALYTICS COMPLETED!")
    print(f"Total records analyzed: {final_count}")
    print("="*60)

except Exception as e:
    print(f"\n✗ ERROR in Spark analytics: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

finally:
    spark.stop()
    print("\nSpark session closed.")