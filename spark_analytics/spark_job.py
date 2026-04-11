from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, col

# 初始化 Spark
spark = SparkSession.builder \
    .appName("LLM_Performance_Analyzer") \
    .getOrCreate()

# AWS S3 与 RDS MySQL 配置
S3_INPUT_PATH = "s3a://comp4442-llm-monitor-bucket/raw_logs/*.json"
JDBC_URL = "jdbc:mysql://your-rds-endpoint.amazonaws.com:3306/monitor_db"
DB_PROPERTIES = {
    "user": "admin",
    "password": "yourpassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 1. 从 S3 读取 JSON 日志
df = spark.read.json(S3_INPUT_PATH)

# 2. 核心聚合计算 (计算行为摘要)
summary_df = df.agg(
    avg("throughput_tokens_s").alias("avg_throughput"),
    max("vram_usage_percent").alias("peak_vram_usage"),
    avg("cpu_gpu_pipeline_latency_ms").alias("avg_pipeline_latency")
)

summary_df.show()

# 3. 将结果写入云端关系型数据库 (RDS)
summary_df.write.jdbc(
    url=JDBC_URL,
    table="performance_summary",
    mode="append",
    properties=DB_PROPERTIES
)

print("Spark 批处理分析完成，已写入 RDS。")
spark.stop()