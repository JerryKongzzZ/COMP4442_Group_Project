from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max as spark_max
# 🌟 新增：导入 Schema 类型
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import sys

spark = SparkSession.builder \
    .appName("LLM_MultiNode_Streaming") \
    .getOrCreate()  # 删掉了自动推断的代码

db_password = sys.argv[1]
RDS_URL = "jdbc:mysql://database-comp4442.cdseymmwam07.ap-east-1.rds.amazonaws.com:3306/monitor_db"
S3_INPUT_PATH = "s3a://comp4442-llm-monitor-bucket/raw_logs/"

# 🌟 核心升级：手动定义 JSON 的表结构 (Schema)
# 这样 Spark 就不需要去扫描 S3 里的历史文件了，实现 1 秒极速冷启动！
json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("instance_id", StringType(), True),
    StructField("vram_usage_percent", DoubleType(), True),
    StructField("throughput_tokens_s", LongType(), True),
    StructField("cpu_gpu_pipeline_latency_ms", DoubleType(), True)
])

# 监听 S3，挂上 schema 和限流阀
df_stream = spark.readStream \
    .schema(json_schema) \
    .format("json") \
    .option("multiline", "true") \
    .option("maxFilesPerTrigger", 100) \
    .load(S3_INPUT_PATH)

def process_and_write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        # 精准聚合这 5 秒内的数据，并提取真实的“事件时间”！
        agg_df = batch_df.groupBy("instance_id").agg(
            avg("throughput_tokens_s").alias("avg_throughput"),
            spark_max("vram_usage_percent").alias("peak_vram_usage"), 
            avg("cpu_gpu_pipeline_latency_ms").alias("avg_pipeline_latency"),
            # 🌟 新增：提取该批次内，该节点最新的一条日志时间！
            spark_max("timestamp").alias("last_update_time") 
        )
        
        agg_df.write \
            .format("jdbc") \
            .option("url", RDS_URL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "performance_summary") \
            .option("user", "admin") \
            .option("password", db_password) \
            .mode("append") \
            .save()

# 使用全新的 checkpoint 文件夹，避免旧数据冲突
query = df_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(process_and_write_batch) \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "s3a://comp4442-llm-monitor-bucket/checkpoints_multi_node/") \
    .start()

query.awaitTermination()