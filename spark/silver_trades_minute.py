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

# order trades within each minute and compute LOG returns between consecutive trades
# note: using all trades here. at high frequency this carries some microstructure
# noise (bid-ask bounce inflates RV). acceptable at this volume; sparse sampling
# is the planned mitigation once streaming gives higher trade density per window.
w = Window.partitionBy("symbol", "minute").orderBy("ts", "trade_id")
clean = (
    clean
    .withColumn("prev_price", F.lag("price").over(w))
    .withColumn("log_ret", F.log(F.col("price") / F.col("prev_price")))
    # |r_i| * |r_{i-1}| term for bipower variation (product of adjacent abs returns)
    .withColumn("abs_ret", F.abs(F.col("log_ret")))
    .withColumn("prev_abs_ret", F.lag(F.abs(F.col("log_ret"))).over(w))
    .withColumn("bipower_term", F.col("abs_ret") * F.col("prev_abs_ret"))
)

# constant for bipower: pi/2 (the scaling that makes BV consistent for integrated variance)
MU = 1.5707963267948966  # pi/2

agg = (
    clean.groupBy("symbol", "minute").agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("quantity").alias("volume"),
        F.count("*").alias("trade_count"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        # realized variance = sum of squared log returns
        F.sum(F.col("log_ret") * F.col("log_ret")).alias("realized_variance"),
        # bipower variation = (pi/2) * sum of |r_i| * |r_{i-1}|
        (F.lit(MU) * F.sum("bipower_term")).alias("bipower_variation"),
        # order flow
        F.sum(F.when(F.col("side") == "BUY",  F.col("quantity")).otherwise(0)).alias("buy_volume"),
        F.sum(F.when(F.col("side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
    )
    .withColumn("realized_variance", F.coalesce(F.col("realized_variance"), F.lit(0.0)))
    .withColumn("bipower_variation", F.coalesce(F.col("bipower_variation"), F.lit(0.0)))
    # realized volatility = sqrt(realized variance)
    .withColumn("realized_vol", F.sqrt(F.col("realized_variance")))
    # jump component = RV - BV, floored at 0 (RV captures jumps, BV is jump-robust,
    # so the positive difference estimates the jump contribution to variance)
    .withColumn("jump_component",
                F.greatest(F.col("realized_variance") - F.col("bipower_variation"), F.lit(0.0)))
    # order-flow imbalance: net buy pressure
    .withColumn("signed_volume", F.col("buy_volume") - F.col("sell_volume"))
    # normalized OFI: signed volume as a fraction of total volume (scale-free, -1..1)
    .withColumn("ofi",
                F.when(F.col("volume") > 0,
                       (F.col("buy_volume") - F.col("sell_volume")) / F.col("volume"))
                 .otherwise(F.lit(0.0)))
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
