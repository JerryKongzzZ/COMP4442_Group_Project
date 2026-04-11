import boto3
import json
import time
import random
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# AWS S3 配置 (请替换为你们真实的凭证和桶名)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
LOCAL_LOG_DIR = "logs"

os.makedirs(LOCAL_LOG_DIR, exist_ok=True)

# 初始化 S3 客户端
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1'
)

def generate_mock_log():
    """
    在这里接入你们真实的 cuSZp 压缩与 vLLM 日志。
    现在为了跑通流程，先使用随机数模拟显存和延迟波动。
    """
    return {
        "timestamp": datetime.now().isoformat(),
        "vram_usage_percent": round(random.uniform(70.0, 98.0), 2),  # 模拟显存占用，有爆满风险
        "throughput_tokens_s": random.randint(1200, 2500),         # 吞吐量
        "cpu_gpu_pipeline_latency_ms": round(random.uniform(10.5, 45.0), 2) # 数据管道延迟
    }

print("开始实时推送大模型性能日志到 AWS S3...")

while True:
    log_data = generate_mock_log()
    file_name = f"log_{int(time.time())}.json"
    file_path = os.path.join(LOCAL_LOG_DIR, file_name)

    # 将日志写入指定文件夹
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)

    try:
        # 上传到 S3 的指定路径
        s3_client.upload_file(file_path, BUCKET_NAME, f"raw_logs/{file_name}")
        print(f"[{log_data['timestamp']}] 成功推送: VRAM {log_data['vram_usage_percent']}%")
    except Exception as e:
        print(f"上传失败: {e}")

    time.sleep(5)  # 模拟每5秒产生一条聚合日志