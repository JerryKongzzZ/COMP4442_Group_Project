import boto3
import json
import time
import random
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
LOCAL_LOG_DIR = "logs"

os.makedirs(LOCAL_LOG_DIR, exist_ok=True)

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='ap-east-1' # 确保和你的 S3 区域一致
)

def generate_mock_log():
    # 🌟 核心修改：模拟两个 LLM 节点！
    nodes = ["vLLM-Node-1", "vLLM-Node-2"]
    return {
        "timestamp": datetime.now().isoformat(),
        "instance_id": random.choice(nodes), # 随机分配给两台机器
        "vram_usage_percent": round(random.uniform(50.0, 98.0), 2),
        "throughput_tokens_s": random.randint(1200, 2500),
        "cpu_gpu_pipeline_latency_ms": round(random.uniform(10.5, 45.0), 2)
    }

print("开始实时推送多节点大模型性能日志到 AWS S3...")

while True:
    log_data = generate_mock_log()
    file_name = f"log_{int(time.time()*1000)}.json" # 加毫秒防止重名
    file_path = os.path.join(LOCAL_LOG_DIR, file_name)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)

    try:
        s3_client.upload_file(file_path, BUCKET_NAME, f"raw_logs/{file_name}")
        print(f"[{log_data['timestamp']}] 成功推送 {log_data['instance_id']} 数据")
    except Exception as e:
        print(f"上传失败: {e}")

    time.sleep(0.2)