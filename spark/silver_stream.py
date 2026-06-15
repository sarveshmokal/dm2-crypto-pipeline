# Structured Streaming: continuously read landed trade files from GCS, compute
# per-minute microstructure metrics with windowed aggregation + watermarking,
# and write completed windows to BigQuery silver as a streaming append.
#
# honest note on scope: the streaming job computes the core metrics that
# aggregate cleanly in a streaming window (price-variance proxy, volume, trade
# counts, buy/sell split, OFI). the fuller estimator suite that needs strict
# per-trade ordering within a window (bipower variation, jump component via lag)
# is done in the batch silver job. streaming optimizes for continuous low-latency
# core metrics.
#
# dependency note: the GCS connector and the BigQuery connector bundle clashing
# Guava versions, which causes a NoSuchMethodError on Preconditions.checkState.
# the fix is to load the SHADED gcs-connector jar (Guava relocated, no clash)
# via spark.jars, while the BigQuery connector comes via packages.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

PROJECT = "dm2-crypto-microstructure"
BUCKET  = "dm2-crypto-microstructure-raw"
SOURCE_DIR = f"gs://{BUCKET}/streaming/trades/"
SILVER  = f"{PROJECT}.silver.trades_minute_stream"
CHECKPOINT = f"gs://{BUCKET}/streaming/_checkpoint/silver_stream"
SHADED_GCS_JAR = "/home/g100004344/jars/gcs-connector-shaded.jar"

spark = (
    SparkSession.builder
    .appName("silver_stream")
    .config("spark.sql.shuffle.partitions", "4")
    # only the BigQuery connector via packages; GCS connector comes from the
    # shaded jar below to avoid the Guava conflict
    .config("spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0")
    .config("spark.jars", SHADED_GCS_JAR)
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_type",     StringType()),
    StructField("event_time",     LongType()),
    StructField("symbol",         StringType()),
    StructField("trade_id",       LongType()),
    StructField("price",          StringType()),
    StructField("quantity",       StringType()),
    StructField("trade_time",     LongType()),
    StructField("is_buyer_maker", StringType()),
])

raw = (
    spark.readStream
    .schema(schema)
    .json(SOURCE_DIR)
)

clean = (
    raw
    .withColumn("price",    F.col("price").cast("double"))
    .withColumn("quantity", F.col("quantity").cast("double"))
    .withColumn("ts",       (F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("side",     F.when(F.col("is_buyer_maker") == "false", F.lit("BUY"))
                             .otherwise(F.lit("SELL")))
    .filter((F.col("price") > 0) & (F.col("quantity") > 0))
)

agg = (
    clean
    .withWatermark("ts", "30 seconds")
    .groupBy(F.col("symbol"), F.window(F.col("ts"), "1 minute"))
    .agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("quantity").alias("volume"),
        F.count("*").alias("trade_count"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.variance("price").alias("price_variance"),
        F.sum(F.when(F.col("side") == "BUY",  F.col("quantity")).otherwise(0)).alias("buy_volume"),
        F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
    )
    .withColumn("window_start", F.col("window").getField("start"))
    .withColumn("window_end",   F.col("window").getField("end"))
    .withColumn("price_variance", F.coalesce(F.col("price_variance"), F.lit(0.0)))
    .withColumn("signed_volume", F.col("buy_volume") - F.col("sell_volume"))
    .withColumn("ofi",
                F.when(F.col("volume") > 0,
                       (F.col("buy_volume") - F.col("sell_volume")) / F.col("volume"))
                 .otherwise(F.lit(0.0)))
    .drop("window")
)


def write_to_bq(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    (
        batch_df.write.format("bigquery")
        .option("table", SILVER)
        .option("writeMethod", "direct")
        .mode("append")
        .save()
    )
    print(f"batch {batch_id}: wrote {batch_df.count()} windows to {SILVER}")


query = (
    agg.writeStream
    .outputMode("append")
    .foreachBatch(write_to_bq)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

print("streaming started. press Ctrl+C to stop.")
query.awaitTermination()
