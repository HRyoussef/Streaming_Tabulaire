from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, col, window, count, avg, to_timestamp

spark = SparkSession.builder \
    .appName("TaxiStreamingKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Lire depuis Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Extraire la valeur texte
lines_df = raw_df.selectExpr("CAST(value AS STRING) as value")

# 3. Parser le CSV
schema_str = """
    idx LONG, VendorID LONG, tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING, passenger_count DOUBLE,
    trip_distance DOUBLE, RatecodeID DOUBLE, store_and_fwd_flag STRING,
    PULocationID LONG, DOLocationID LONG, payment_type LONG,
    fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE,
    tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE,
    congestion_surcharge DOUBLE, Airport_fee DOUBLE, cbd_congestion_fee DOUBLE
"""

parsed_df = lines_df.select(
    from_csv(col("value"), schema_str).alias("data")
).select("data.*") \
 .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))

# 4. Watermark + fenêtres glissantes
agg_df = parsed_df \
    .withWatermark("tpep_pickup_datetime", "999999 minutes") \
    .groupBy(
        window("tpep_pickup_datetime", "2 minutes", "1 minute"),
        "PULocationID"
    ) \
    .agg(
        count("*").alias("nb_trips"),
        avg("fare_amount").alias("avg_fare")
    )

# 5. Écriture HDFS
query = agg_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://172.19.0.5:9000/results/streaming_kafka/") \
    .option("checkpointLocation", "hdfs://172.19.0.5:9000/checkpoints/kafka/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
