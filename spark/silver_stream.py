# Structured Streaming: continuously read landed trade files from GCS, compute
# per-minute microstructure metrics, and upsert completed windows to BigQuery silver.
#
# scope: this job computes TRUE realized variance and realized volatility (sum of
# squared log returns between consecutive trades) - the same estimator as the batch
# silver job, not a price-variance proxy.
#
# correctness (window completeness): a single minute's trades can arrive across
# multiple micro-batches. to guarantee each minute is written exactly once and
# computed from ALL its trades, each batch:
#   1. identifies which minutes it touched,
#   2. recomputes those minutes from the FULL set of trades for them in GCS, and
#   3. MERGEs the complete result into silver keyed on (symbol, window_start).
# this makes writes idempotent and each minute converge to its complete value.
#
# bipower/jump remain batch-only (need strict adjacent-return ordering across the
# whole window); realized variance is an additive sum and promotes cleanly here.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
from google.cloud import bigquery

PROJECT = "dm2-crypto-microstructure"
BUCKET  = "dm2-crypto-microstructure-raw"
SOURCE_DIR = f"gs://{BUCKET}/streaming/trades/"
SILVER  = f"{PROJECT}.silver.trades_minute_stream"
STAGING = f"{PROJECT}.silver._stream_stage"
CHECKPOINT = f"gs://{BUCKET}/streaming/_checkpoint/silver_stream"
SHADED_GCS_JAR = "/home/g100004344/jars/gcs-connector-shaded.jar"

spark = (
    SparkSession.builder
    .appName("silver_stream")
    .config("spark.sql.shuffle.partitions", "4")
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

def read_all_trades():
    return (
        spark.read.schema(schema).json(SOURCE_DIR)
        .withColumn("price",    F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("ts",       (F.col("trade_time") / 1000).cast("timestamp"))
        .withColumn("side",     F.when(F.col("is_buyer_maker") == "false", F.lit("BUY"))
                                 .otherwise(F.lit("SELL")))
        .filter((F.col("price") > 0) & (F.col("quantity") > 0))
        .filter(F.col("ts") >= (F.current_timestamp() - F.expr("INTERVAL 20 MINUTES")))
        .withColumn("minute",   F.date_trunc("minute", F.col("ts")))
    )

raw = spark.readStream.schema(schema).json(SOURCE_DIR)
clean_stream = (
    raw
    .withColumn("ts", (F.col("trade_time") / 1000).cast("timestamp"))
    .filter(F.col("trade_time").isNotNull())
    .withColumn("minute", F.date_trunc("minute", F.col("ts")))
    .withColumn("symbol", F.col("symbol"))
    .withWatermark("ts", "30 seconds")
)

bq = bigquery.Client(project=PROJECT)

def compute_minutes(all_trades, touched):
    cond = F.lit(False)
    for sym, minute in touched:
        cond = cond | ((F.col("symbol") == sym) & (F.col("minute") == F.lit(minute)))
    sub = all_trades.filter(cond)
    w = Window.partitionBy("symbol", "minute").orderBy("ts", "trade_id")
    enriched = (
        sub
        .withColumn("prev_price", F.lag("price").over(w))
        .withColumn("log_ret", F.log(F.col("price") / F.col("prev_price")))
    )
    return (
        enriched.groupBy("symbol", "minute").agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.sum(F.col("log_ret") * F.col("log_ret")).alias("realized_variance"),
            F.sum(F.when(F.col("side") == "BUY",  F.col("quantity")).otherwise(0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
        )
        .withColumn("realized_variance", F.coalesce(F.col("realized_variance"), F.lit(0.0)))
        .withColumn("realized_vol", F.sqrt(F.col("realized_variance")))
        .withColumnRenamed("minute", "window_start")
        .withColumn("window_end", (F.col("window_start").cast("long") + 60).cast("timestamp"))
        .withColumn("signed_volume", F.col("buy_volume") - F.col("sell_volume"))
        .withColumn("ofi",
                    F.when(F.col("volume") > 0,
                           (F.col("buy_volume") - F.col("sell_volume")) / F.col("volume"))
                     .otherwise(F.lit(0.0)))
    )

MERGE_SQL = f"""
MERGE `{SILVER}` T
USING `{STAGING}` S
ON T.symbol = S.symbol AND T.window_start = S.window_start
WHEN MATCHED THEN UPDATE SET
  open=S.open, high=S.high, low=S.low, close=S.close, volume=S.volume,
  trade_count=S.trade_count, vwap=S.vwap, realized_variance=S.realized_variance,
  realized_vol=S.realized_vol, buy_volume=S.buy_volume, sell_volume=S.sell_volume,
  window_end=S.window_end, signed_volume=S.signed_volume, ofi=S.ofi
WHEN NOT MATCHED THEN INSERT (symbol, window_start, window_end, open, high, low, close, volume, trade_count, vwap, realized_variance, realized_vol, buy_volume, sell_volume, signed_volume, ofi) VALUES (S.symbol, S.window_start, S.window_end, S.open, S.high, S.low, S.close, S.volume, S.trade_count, S.vwap, S.realized_variance, S.realized_vol, S.buy_volume, S.sell_volume, S.signed_volume, S.ofi)
"""

def write_to_bq(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    touched_rows = batch_df.select("symbol", "minute").distinct().collect()
    touched = [(r["symbol"], r["minute"]) for r in touched_rows if r["symbol"] and r["minute"]]
    if not touched:
        return
    all_trades = read_all_trades()
    result = compute_minutes(all_trades, touched)
    if result.rdd.isEmpty():
        return
    (
        result.write.format("bigquery")
        .option("table", STAGING)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    bq.query(MERGE_SQL).result()
    print(f"batch {batch_id}: merged {len(touched)} minute(s) into {SILVER}")

query = (
    clean_stream.writeStream
    .outputMode("append")
    .foreachBatch(write_to_bq)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

print("streaming started. press Ctrl+C to stop.")
query.awaitTermination()
