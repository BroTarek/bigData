import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as Fsum, from_json, schema_of_json
from pyspark.sql.functions import avg, min, max, hour, month, year, when , collect_set, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

# Environment variables
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

    # Define schema based on your actual data
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("airline", StringType(), True),
        StructField("flight_number", StringType(), True),
        StructField("aircraft_model", StringType(), True),
        StructField("aircraft_type", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("latitude", StringType(), True),  # Keep as string for now
        StructField("longitude", StringType(), True),  # Keep as string for now
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("fatalities", StringType(), True),  # Your data has strings
        StructField("injuries", StringType(), True),    # Your data has strings
        StructField("damage", StringType(), True),
        StructField("cause", StringType(), True),
        # Fields from your NiFi transformation (if they exist):
        StructField("damage_severity_score", IntegerType(), True),
        StructField("is_valid", StringType(), True),
        StructField("region", StringType(), True),
        StructField("has_casualties", StringType(), True),
        StructField("_processed_timestamp", LongType(), True)
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
    from pyspark.sql.functions import coalesce, lit
    
    processed_df = parsed_df \
        .withColumn("fatalities_int", 
                   coalesce(col("fatalities").cast(IntegerType()), lit(0))) \
        .withColumn("injuries_int", 
                   coalesce(col("injuries").cast(IntegerType()), lit(0))) \
        .withColumn("latitude_double", 
                   coalesce(col("latitude").cast(DoubleType()), lit(0.0))) \
        .withColumn("longitude_double", 
                   coalesce(col("longitude").cast(DoubleType()), lit(0.0))) \
        .withColumn("damage_score", 
                   coalesce(col("damage_severity_score"), lit(-1)))

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
        Fsum("injuries_int").alias("total_injuries"),
        avg("damage_score").alias("avg_damage_score")
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
        avg("damage_score").alias("avg_damage_severity"),
        collect_set("aircraft_model").alias("aircraft_used")
    ).orderBy(col("incident_count").desc())
    
    print("\nTop airlines by incident count:")
    by_airline.show(10, truncate=False)

    # 4. Fatal vs non-fatal incidents
    fatal_stats = unique_df.withColumn(
        "is_fatal", when(col("fatalities_int") > 0, "Fatal").otherwise("Non-Fatal")
    ).groupBy("is_fatal").agg(
        count("*").alias("incident_count"),
        avg("injuries_int").alias("avg_injuries"),
        collect_set("damage").alias("damage_types")
    )
    
    print("\nFatal vs Non-Fatal incidents:")
    fatal_stats.show(truncate=False)

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

    # Write raw data for reference
    try:
        unique_df.select(
            "event_id", "timestamp", "airline", "flight_number", 
            "aircraft_model", "origin", "destination", "country",
            "fatalities_int", "injuries_int", "damage", "cause",
            "damage_score", "region", "has_casualties"
        ).write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "raw_incidents") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("overwrite") \
            .save()
        print("✓ raw_incidents written")
    except Exception as e:
        print(f"⚠ Could not write raw_incidents: {e}")

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