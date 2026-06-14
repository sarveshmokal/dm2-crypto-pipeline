from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT = "dm2-crypto-microstructure"
BRONZE  = f"{PROJECT}.bronze.trades_raw"
SILVER  = f"{PROJECT}.silver.trades_minute"

spark = (
    SparkSession.builder
    .appName("bronze_to_silver_trades")
    .config("spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0")
    .getOrCreate()
)
spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", "bronze")

# read raw trades from bronze
bronze = spark.read.format("bigquery").option("table", BRONZE).load()
print("bronze rows:", bronze.count())

# clean: cast types, ms -> timestamp, derive buy/sell side, truncate to minute
clean = (
    bronze
    .withColumn("price",    F.col("price").cast("double"))
    .withColumn("quantity", F.col("quantity").cast("double"))
    .withColumn("ts",       (F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("side",     F.when(F.col("is_buyer_maker") == False, F.lit("BUY")).otherwise(F.lit("SELL")))
    .withColumn("minute",   F.date_trunc("minute", F.col("ts")))
    .filter((F.col("price") > 0) & (F.col("quantity") > 0))
)

# order within each minute to get price changes for realized vol
w = Window.partitionBy("symbol", "minute").orderBy("ts", "trade_id")
clean = (
    clean
    .withColumn("prev_price", F.lag("price").over(w))
    .withColumn("ret", F.col("price") - F.col("prev_price"))
)

# per-minute aggregation
agg = (
    clean.groupBy("symbol", "minute").agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("quantity").alias("volume"),
        F.count("*").alias("trade_count"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.stddev("ret").alias("realized_vol"),
        F.sum(F.when(F.col("side") == "BUY",  F.col("quantity")).otherwise(0)).alias("buy_volume"),
        F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
    )
    .withColumn("realized_vol", F.coalesce(F.col("realized_vol"), F.lit(0.0)))
    .withColumn("signed_volume", F.col("buy_volume") - F.col("sell_volume"))
    .orderBy("minute")
)

agg.show(truncate=False)

# write to silver (direct write avoids the gs:// staging error)
(
    agg.write.format("bigquery")
    .option("table", SILVER)
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)
print("wrote", SILVER)
spark.stop()
