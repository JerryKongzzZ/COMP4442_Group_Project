import boto3
import json
import time
import random
from datetime import datetime
import os
from collections import deque
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
    region_name='ap-east-1' 
)

# 🌟 核心升级：维护一个 S3 文件名记忆队列
MAX_S3_FILES = 5000
s3_file_queue = deque()

def generate_mock_log():
    nodes = ["vLLM-Node-1", "vLLM-Node-2"]
    return {
        "timestamp": datetime.now().isoformat(),
        "instance_id": random.choice(nodes), 
        "vram_usage_percent": round(random.uniform(50.0, 98.0), 2),
        "throughput_tokens_s": random.randint(1200, 2500),
        "cpu_gpu_pipeline_latency_ms": round(random.uniform(10.5, 45.0), 2)
    }

print(f"开始实时推送日志到 AWS S3 (动态维持最新的 {MAX_S3_FILES} 个文件)...")

while True:
    log_data = generate_mock_log()
    file_name = f"log_{int(time.time()*1000)}.json"
    file_path = os.path.join(LOCAL_LOG_DIR, file_name)
    s3_key = f"raw_logs/{file_name}"

    # 1. 写入本地
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)

    try:
        # 2. 上传到 S3
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"[{log_data['timestamp']}] 成功推送 {log_data['instance_id']}")
        
        # 3. 阅后即焚：立刻删除 EC2 本地文件，保证本地 0 占用
        os.remove(file_path)

        # 4. 🌟 S3 容量控制：将刚上传的文件名加入记忆队列
        s3_file_queue.append(s3_key)
        
        # 如果队列里的文件数超过了 5000 个
        if len(s3_file_queue) > MAX_S3_FILES:
            # 弹出最老的一个文件名
            oldest_s3_key = s3_file_queue.popleft()
            # 精准删除 S3 上的这个老文件
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=oldest_s3_key)
            print(f"   🧹 S3 容量控制: 已自动删除过期日志 {oldest_s3_key}")

    except Exception as e:
        print(f"操作失败: {e}")

    time.sleep(1)