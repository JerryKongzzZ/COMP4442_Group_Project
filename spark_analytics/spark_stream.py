"""
spark_stream.py  ──  Spark Structured Streaming（對應功能 B：實時監控）

運行方式（在 EMR 上）：
  spark-submit \
    --jars /usr/share/aws/emr/instance-controller/lib/mysql-connector-java.jar \
    spark_stream.py <db_password>
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType
)

spark = SparkSession.builder \
    .appName("LLM_MultiNode_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

db_password   = sys.argv[1]
RDS_URL       = "jdbc:mysql://database-comp4442.cdseymmwam07.ap-east-1.rds.amazonaws.com:3306/monitor_db"
S3_INPUT_PATH = "s3a://comp4442-llm-monitor-bucket/raw_logs/"

# ── 手動定義 Schema（避免 Spark 掃描全量文件，實現秒級冷啟動） ─────────────
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

# ── 監聽 S3，限流避免過載 ──────────────────────────────────────────────────
df_stream = spark.readStream \
    .schema(json_schema) \
    .format("json") \
    .option("multiline", "true") \
    .option("maxFilesPerTrigger", 100) \
    .load(S3_INPUT_PATH)

# ── 每批次：按節點聚合，寫入 RDS performance_summary 表 ──────────────────
def process_and_write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    agg_df = batch_df.groupBy("instance_id", "gpu_model").agg(
        F.round(F.avg("throughput_tokens_s"), 2)         .alias("avg_throughput"),
        F.round(F.max("vram_usage_percent"), 2)           .alias("peak_vram_usage"),
        F.round(F.avg("vram_usage_percent"), 2)           .alias("avg_vram_usage"),
        F.round(F.avg("cpu_gpu_pipeline_latency_ms"), 2)  .alias("avg_pipeline_latency"),
        F.round(F.avg("ttft_ms"), 2)                      .alias("avg_ttft_ms"),
        F.round(F.avg("kv_cache_hit_rate"), 3)            .alias("avg_kv_hit_rate"),
        F.sum("kv_cache_evictions")                       .alias("kv_evictions_in_batch"),
        F.round(F.avg("gpu_compute_util"), 2)             .alias("avg_gpu_util"),
        F.sum("requests_completed")                       .alias("requests_completed"),
        F.max("timestamp")                                .alias("last_update_time"),
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

    print(f"✅ Batch {batch_id} 已寫入 RDS，共 {agg_df.count()} 個節點的聚合結果")

# ── 啟動流處理（5 秒微批） ────────────────────────────────────────────────
query = df_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_and_write_batch) \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation",
            "s3a://comp4442-llm-monitor-bucket/checkpoints_multi_node/") \
    .start()

query.awaitTermination()
