"""
spark_job.py  ──  Spark 批處理分析（對應功能 A：歷史摘要表格）

運行方式（在 EMR 上）：
  spark-submit \
    --jars /usr/share/aws/emr/instance-controller/lib/mysql-connector-java.jar \
    spark_job.py <db_user> <db_password>
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── 初始化 ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LLM_Batch_Summary_Analyzer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

db_user     = sys.argv[1]
db_password = sys.argv[2]

S3_INPUT_PATH = "s3a://comp4442-llm-monitor-bucket/raw_logs/*.json"
JDBC_URL      = "jdbc:mysql://database-comp4442.cdseymmwam07.ap-east-1.rds.amazonaws.com:3306/monitor_db"
DB_PROPS = {
    "user":     db_user,
    "password": db_password,
    "driver":   "com.mysql.cj.jdbc.Driver"
}

# ── 1. 從 S3 讀取全量 JSON 日誌 ──────────────────────────────────────────
df = spark.read \
    .option("multiline", "true") \
    .json(S3_INPUT_PATH)

print(f"✅ 讀取原始日誌完成，共 {df.count()} 條記錄")
df.printSchema()

# ── 2. 核心聚合：按節點分組，計算所有摘要指標 ─────────────────────────────
#
#  對照原始項目（駕駛行為）的欄位映射：
#    vram_exceed_count      ← 超速次數（VRAM > 90%）
#    kv_eviction_count      ← 疲勞駕駛次數（KV Cache 驅逐次數）
#    vram_high_duration_min ← 超速總時長（每條記錄 = 1 秒，/60 = 分鐘）
#    idle_duration_min      ← 空檔滑行時長（VRAM < 30%，低負載）
#
summary_df = df.groupBy("instance_id", "gpu_model").agg(

    # ── Token 吞吐量 ──────────────────────────────────────────────────
    F.sum("throughput_tokens_s")
     .alias("total_tokens"),

    F.round(F.avg("throughput_tokens_s"), 2)
     .alias("avg_throughput"),

    # ── VRAM 顯存超標（類比超速）────────────────────────────────────
    F.sum(F.when(F.col("vram_usage_percent") > 90, 1).otherwise(0))
     .alias("vram_exceed_count"),                   # 超標次數

    F.round(
        F.sum(F.when(F.col("vram_usage_percent") > 90, 1).otherwise(0)) / 60.0, 1
    ).alias("vram_high_duration_min"),               # 超標持續分鐘數

    F.round(F.max("vram_usage_percent"), 2)
     .alias("peak_vram_usage"),

    F.round(F.avg("vram_usage_percent"), 2)
     .alias("avg_vram_usage"),

    # ── KV Cache 溢出（類比疲勞駕駛）───────────────────────────────
    F.sum(F.when(F.col("kv_cache_evictions") > 0, 1).otherwise(0))
     .alias("kv_eviction_count"),                   # KV 驅逐事件次數

    F.sum("kv_cache_evictions")
     .alias("total_kv_evictions"),                  # 驅逐總量

    F.round(F.avg("kv_cache_hit_rate"), 3)
     .alias("avg_kv_hit_rate"),                     # 平均命中率

    # ── 節點空閒時長（類比空檔滑行）────────────────────────────────
    F.round(
        F.sum(F.when(F.col("vram_usage_percent") < 30, 1).otherwise(0)) / 60.0, 1
    ).alias("idle_duration_min"),

    # ── 延遲指標 ─────────────────────────────────────────────────────
    F.round(F.avg("ttft_ms"), 2)
     .alias("avg_ttft_ms"),

    F.round(F.max("ttft_ms"), 2)
     .alias("max_ttft_ms"),

    F.round(F.avg("cpu_gpu_pipeline_latency_ms"), 2)
     .alias("avg_pipeline_latency"),

    # ── PCIe 瓶頸次數 ────────────────────────────────────────────────
    F.sum(F.when(F.col("pcie_util_percent") > 85, 1).otherwise(0))
     .alias("pcie_bottleneck_count"),

    # ── GPU 整體計算利用率 ───────────────────────────────────────────
    F.round(F.avg("gpu_compute_util"), 2)
     .alias("avg_gpu_util"),

    # ── 處理的請求總數 ───────────────────────────────────────────────
    F.sum("requests_completed")
     .alias("total_requests_completed"),

    # ── 記錄筆數（用於換算時間） ─────────────────────────────────────
    F.count("*").alias("record_count"),

    # ── 數據時間範圍 ─────────────────────────────────────────────────
    F.min("timestamp").alias("data_start"),
    F.max("timestamp").alias("data_end"),
)

# ── 3. 計算健康評分（0–100，越低越健康） ─────────────────────────────────
summary_df = summary_df.withColumn(
    "health_score",
    F.round(
        F.least(F.lit(100.0),
            F.col("vram_exceed_count")  * 0.8 +   # 每次超標扣 0.8 分
            F.col("kv_eviction_count")  * 0.5 +   # 每次驅逐扣 0.5 分
            F.col("pcie_bottleneck_count") * 0.3 + # 每次瓶頸扣 0.3 分
            (F.col("avg_ttft_ms") - 100) * 0.05   # 延遲懲罰
        ), 1
    )
)

# ── 4. 預覽結果 ──────────────────────────────────────────────────────────
print("📊 Spark 聚合結果預覽：")
summary_df.show(truncate=False)

# ── 5. 寫入 RDS（node_summary 表，覆蓋模式保持最新） ─────────────────────
#
#  建表 SQL（首次運行前在 RDS 執行）：
#  CREATE TABLE IF NOT EXISTS node_summary (
#    id INT AUTO_INCREMENT PRIMARY KEY,
#    instance_id VARCHAR(50), gpu_model VARCHAR(50),
#    total_tokens BIGINT, avg_throughput DOUBLE,
#    vram_exceed_count INT, vram_high_duration_min DOUBLE,
#    peak_vram_usage DOUBLE, avg_vram_usage DOUBLE,
#    kv_eviction_count INT, total_kv_evictions INT, avg_kv_hit_rate DOUBLE,
#    idle_duration_min DOUBLE, avg_ttft_ms DOUBLE, max_ttft_ms DOUBLE,
#    avg_pipeline_latency DOUBLE, pcie_bottleneck_count INT,
#    avg_gpu_util DOUBLE, total_requests_completed BIGINT,
#    record_count BIGINT, health_score DOUBLE,
#    data_start VARCHAR(50), data_end VARCHAR(50),
#    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#  );
#
summary_df.write.jdbc(
    url=JDBC_URL,
    table="node_summary",
    mode="overwrite",          # 每次批處理覆蓋，保持最新摘要
    properties=DB_PROPS
)

print("✅ Spark 批處理完成，結果已寫入 RDS node_summary 表。")
spark.stop()
