"""
spark_unified.py  ──  Unified Spark job: Structured Streaming + Periodic Historical Summary
                       Function B: real-time per-batch aggregation → performance_summary
                       Function A: periodic full-history aggregation → node_summary

Run on COMP4442-EMR:
  spark-submit \
    --jars /usr/share/aws/emr/instance-controller/lib/mysql-connector-java.jar \
    spark_unified.py <db_password>
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType
)

# ── Initialise Spark ──────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LLM_Unified_Monitor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

db_password   = sys.argv[1]
RDS_URL       = "jdbc:mysql://database-comp4442.cdseymmwam07.ap-east-1.rds.amazonaws.com:3306/monitor_db"
S3_INPUT_PATH = "s3a://comp4442-llm-monitor-bucket/raw_logs/"

DB_PROPS = {
    "user":     "admin",
    "password": db_password,
    "driver":   "com.mysql.cj.jdbc.Driver",
}

# ── How often to refresh node_summary (Function A) ───────────────────────────
# Every SUMMARY_INTERVAL micro-batches = every (SUMMARY_INTERVAL × 5) seconds
# Default: every 12 batches = every 60 seconds
SUMMARY_INTERVAL = 12

# ── Explicit schema — avoids full-directory scan at startup ───────────────────
json_schema = StructType([
    StructField("timestamp",                   StringType(),  True),
    StructField("instance_id",                 StringType(),  True),
    StructField("gpu_model",                   StringType(),  True),
    StructField("vram_total_gb",               DoubleType(),  True),
    StructField("vram_usage_percent",          DoubleType(),  True),
    StructField("kv_cache_hit_rate",           DoubleType(),  True),
    StructField("kv_cache_evictions",          IntegerType(), True),
    StructField("request_queue_len",           IntegerType(), True),
    StructField("active_requests",             IntegerType(), True),
    StructField("requests_completed",          IntegerType(), True),
    StructField("ttft_ms",                     DoubleType(),  True),
    StructField("tpot_ms",                     DoubleType(),  True),
    StructField("throughput_tokens_s",         LongType(),    True),
    StructField("pcie_bandwidth_gbps",         DoubleType(),  True),
    StructField("pcie_util_percent",           DoubleType(),  True),
    StructField("cpu_ram_used_gb",             DoubleType(),  True),
    StructField("gpu_compute_util",            DoubleType(),  True),
    StructField("cpu_gpu_pipeline_latency_ms", DoubleType(),  True),
])

# ── Rate-limited S3 streaming reader ─────────────────────────────────────────
df_stream = spark.readStream \
    .schema(json_schema) \
    .format("json") \
    .option("multiline", "true") \
    .option("maxFilesPerTrigger", 100) \
    .load(S3_INPUT_PATH)

# ── Batch counter (mutable via list to survive closure capture) ───────────────
_counter = [0]


def refresh_node_summary():
    """
    Function A: read the full performance_summary history from RDS,
    compute cumulative per-node statistics, and overwrite node_summary.
    This runs inside the Spark driver, so we use spark.read (batch) within
    the streaming foreachBatch callback.
    """
    print("📊 Refreshing node_summary (Function A)...")

    # Read all historical micro-batch records produced by the streaming job
    hist_df = spark.read \
        .format("jdbc") \
        .option("url",      RDS_URL) \
        .option("dbtable",  "performance_summary") \
        .option("driver",   "com.mysql.cj.jdbc.Driver") \
        .option("user",     "admin") \
        .option("password", db_password) \
        .load()

    if hist_df.count() == 0:
        print("⚠️  performance_summary is empty, skipping node_summary refresh.")
        return

    # ── Cumulative aggregation across all micro-batch records ─────────────────
    #
    #  Field mapping (analogous to driving behaviour requirements):
    #    vram_exceed_count      ← overspeed count    (VRAM > 90%)
    #    kv_eviction_count      ← fatigue count       (KV cache eviction events)
    #    vram_high_duration_min ← overspeed duration  (each record ≈ 5 s → /12 for minutes)
    #    idle_duration_min      ← neutral-slide time  (VRAM < 30%, low utilisation)
    #
    summary_df = hist_df.groupBy("instance_id").agg(
    F.max("gpu_model").alias("gpu_model"),

        # ── Throughput ───────────────────────────────────────────────────────
        F.round(F.sum("avg_throughput"), 0)
         .alias("total_tokens"),

        F.round(F.avg("avg_throughput"), 2)
         .alias("avg_throughput"),

        # ── VRAM overload (analogous to overspeed) ───────────────────────────
        F.sum(F.when(F.col("peak_vram_usage") > 90, 1).otherwise(0))
         .alias("vram_exceed_count"),

        F.round(
            F.sum(F.when(F.col("peak_vram_usage") > 90, 1).otherwise(0)) / 12.0, 1
        ).alias("vram_high_duration_min"),          # each record ≈ 5 s; /12 → minutes

        F.round(F.max("peak_vram_usage"), 2)
         .alias("peak_vram_usage"),

        F.round(F.avg("avg_vram_usage"), 2)
         .alias("avg_vram_usage"),

        # ── KV cache eviction (analogous to fatigue driving) ─────────────────
        F.sum(F.when(F.col("kv_evictions_in_batch") > 0, 1).otherwise(0))
         .alias("kv_eviction_count"),

        F.sum("kv_evictions_in_batch")
         .alias("total_kv_evictions"),

        F.round(F.avg("avg_kv_hit_rate"), 3)
         .alias("avg_kv_hit_rate"),

        # ── Idle duration (analogous to neutral slide) ────────────────────────
        F.round(
            F.sum(F.when(F.col("avg_vram_usage") < 30, 1).otherwise(0)) / 12.0, 1
        ).alias("idle_duration_min"),

        # ── Latency ──────────────────────────────────────────────────────────
        F.round(F.avg("avg_ttft_ms"), 2)
         .alias("avg_ttft_ms"),

        F.round(F.max("avg_ttft_ms"), 2)
         .alias("max_ttft_ms"),

        F.round(F.avg("avg_pipeline_latency"), 2)
         .alias("avg_pipeline_latency"),

        # ── Compute utilisation ───────────────────────────────────────────────
        F.round(F.avg("avg_gpu_util"), 2)
         .alias("avg_gpu_util"),

        F.sum("requests_completed")
         .alias("total_requests_completed"),

        F.count("*").alias("record_count"),

        F.min("last_update_time").alias("data_start"),
        F.max("last_update_time").alias("data_end"),
    )

    # ── Composite health score H ∈ [0, 100] ───────────────────────────────────
    #
    #  H = min(100,  60 × (vram_overload_rate)
    #               + 30 × (kv_eviction_rate)
    #               + max(0, (avg_ttft − 200) / 10))
    #
    #  Consistent with the formula documented in the project report.
    #
    summary_df = summary_df.withColumn(
        "health_score",
        F.round(
            F.least(
                F.lit(100.0),
                (F.col("vram_exceed_count") / F.col("record_count")) * 60.0
                + (F.col("kv_eviction_count") / F.col("record_count")) * 30.0
                + F.greatest(
                    F.lit(0.0),
                    (F.col("avg_ttft_ms") - F.lit(200.0)) / F.lit(10.0)
                )
            ), 1
        )
    )

    # ── Write to node_summary (overwrite → always reflects latest full history)
    summary_df.write \
        .format("jdbc") \
        .option("url",      RDS_URL) \
        .option("driver",   "com.mysql.cj.jdbc.Driver") \
        .option("dbtable",  "node_summary") \
        .option("user",     "admin") \
        .option("password", db_password) \
        .mode("overwrite") \
        .save()

    print(f"✅ node_summary refreshed: {summary_df.count()} nodes written.")


def process_and_write_batch(batch_df, batch_id):
    """
    foreachBatch callback — called every 5 seconds.

    Step 1 (every batch):  per-batch aggregation → performance_summary  (Function B)
    Step 2 (every SUMMARY_INTERVAL batches):  refresh node_summary      (Function A)
    """
    if batch_df.isEmpty():
        return

    # ── Step 1: real-time per-batch aggregation (Function B) ─────────────────
    agg_df = batch_df.groupBy("instance_id", "gpu_model").agg(
        F.round(F.avg("throughput_tokens_s"), 2)        .alias("avg_throughput"),
        F.round(F.max("vram_usage_percent"), 2)          .alias("peak_vram_usage"),
        F.round(F.avg("vram_usage_percent"), 2)          .alias("avg_vram_usage"),
        F.round(F.avg("cpu_gpu_pipeline_latency_ms"), 2) .alias("avg_pipeline_latency"),
        F.round(F.avg("ttft_ms"), 2)                     .alias("avg_ttft_ms"),
        F.round(F.avg("kv_cache_hit_rate"), 3)           .alias("avg_kv_hit_rate"),
        F.sum("kv_cache_evictions")                      .alias("kv_evictions_in_batch"),
        F.round(F.avg("gpu_compute_util"), 2)            .alias("avg_gpu_util"),
        F.sum("requests_completed")                      .alias("requests_completed"),
        F.max("timestamp")                               .alias("last_update_time"),
    )

    agg_df.write \
        .format("jdbc") \
        .option("url",      RDS_URL) \
        .option("driver",   "com.mysql.cj.jdbc.Driver") \
        .option("dbtable",  "performance_summary") \
        .option("user",     "admin") \
        .option("password", db_password) \
        .mode("append") \
        .save()

    print(f"✅ Batch {batch_id}: {agg_df.count()} node records appended to performance_summary.")

    # ── Step 2: periodic historical summary refresh (Function A) ─────────────
    _counter[0] += 1
    if _counter[0] % SUMMARY_INTERVAL == 0:
        refresh_node_summary()


# ── Start the unified streaming query ─────────────────────────────────────────
query = df_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_and_write_batch) \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation",
            "s3a://comp4442-llm-monitor-bucket/checkpoints_multi_node/") \
    .start()

query.awaitTermination()
