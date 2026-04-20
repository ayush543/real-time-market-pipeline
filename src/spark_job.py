import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market-prices")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_DOCKER", "kafka:29092")

POSTGRES_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    "jdbc:postgresql://postgres:5432/market_db"
)
POSTGRES_USER = os.getenv("POSTGRES_DOCKER_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_DOCKER_PASSWORD", "password")
POSTGRES_TABLE = "processed_market_data"


schema = StructType([
    StructField("asset_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("name", StringType(), False),
    StructField("price_usd", DoubleType(), False),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("price_change_pct_24h", DoubleType(), True),
    StructField("source_last_updated", StringType(), True),
    StructField("event_ts", StringType(), False),
    StructField("is_anomaly", BooleanType(), True),
])


def write_batch_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    final_df = batch_df.select(
        col("asset_id"),
        col("symbol"),
        col("name"),
        col("price_usd"),
        col("market_cap"),
        col("total_volume"),
        col("price_change_pct_24h"),
        col("source_last_updated").cast("timestamp"),
        col("event_ts").cast("timestamp"),
        col("is_anomaly")
    )

    final_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"[INFO] Batch {batch_id} written to PostgreSQL")


def main():
    spark = SparkSession.builder \
        .appName("MarketKafkaToPostgres") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(write_batch_to_postgres) \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
