from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max as spark_max
import sys

spark = SparkSession.builder \
    .appName("LLM_MultiNode_Streaming") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

db_password = sys.argv[1]
# 🚨 请确保这个 URL 正确
RDS_URL = "jdbc:mysql://database-comp4442.cdseymmwam07.ap-east-1.rds.amazonaws.com:3306/monitor_db"
S3_INPUT_PATH = "s3a://comp4442-llm-monitor-bucket/raw_logs/"

df_stream = spark.readStream \
    .format("json") \
    .option("multiline", "true") \
    .load(S3_INPUT_PATH)

def process_and_write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        # 🌟 核心修改：按 instance_id 分组计算！
        agg_df = batch_df.groupBy("instance_id").agg(
            avg("throughput_tokens_s").alias("avg_throughput"),
            spark_max("vram_usage_percent").alias("peak_vram_usage"), 
            avg("cpu_gpu_pipeline_latency_ms").alias("avg_pipeline_latency")
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