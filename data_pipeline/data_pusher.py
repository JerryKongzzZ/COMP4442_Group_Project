import boto3
import json
import time
import random
import math
from datetime import datetime
import os
from collections import deque
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME    = os.getenv("BUCKET_NAME")
LOCAL_LOG_DIR  = "logs"

os.makedirs(LOCAL_LOG_DIR, exist_ok=True)

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='ap-east-1'
)

s3_file_queue = deque()

# ── 10 個節點，各有不同的基礎負載和 GPU 型號 ──────────────────────────────
NODES = [
    {"id": "vLLM-Node-01", "gpu": "A100-80GB", "vram_total": 80.0, "base_load": 0.72},
    {"id": "vLLM-Node-02", "gpu": "A100-80GB", "vram_total": 80.0, "base_load": 0.85},
    {"id": "vLLM-Node-03", "gpu": "A100-80GB", "vram_total": 80.0, "base_load": 0.60},
    {"id": "vLLM-Node-04", "gpu": "H100-80GB", "vram_total": 80.0, "base_load": 0.93},  # 高負載
    {"id": "vLLM-Node-05", "gpu": "A100-80GB", "vram_total": 80.0, "base_load": 0.55},
    {"id": "vLLM-Node-06", "gpu": "H100-80GB", "vram_total": 80.0, "base_load": 0.78},
    {"id": "vLLM-Node-07", "gpu": "A100-40GB", "vram_total": 40.0, "base_load": 0.88},  # 40GB 易溢出
    {"id": "vLLM-Node-08", "gpu": "A100-40GB", "vram_total": 40.0, "base_load": 0.42},  # 低負載
    {"id": "vLLM-Node-09", "gpu": "A100-80GB", "vram_total": 80.0, "base_load": 0.69},
    {"id": "vLLM-Node-10", "gpu": "H100-80GB", "vram_total": 80.0, "base_load": 0.76},
]

# ── 生成單條模擬日誌 ───────────────────────────────────────────────────────
def generate_mock_log(node: dict, t: float) -> dict:
    """
    模擬真實 vLLM 推理節點的指標：
      - 負載隨時間正弦波動（模擬請求高峰）
      - 高負載 → VRAM 高 → KV Cache 命中率下降 → 驅逐增加 → 延遲上升
    """
    load = node["base_load"]

    # 週期性負載波動（~2 分鐘一個週期）+ 隨機噪聲
    wave   = math.sin(t / 120.0 * math.pi) * 0.08
    noise  = random.gauss(0, 0.03)
    eff    = min(max(load + wave + noise, 0.10), 0.99)

    vram_pct        = round(eff * 100 + random.gauss(0, 1.5), 2)
    vram_pct        = min(max(vram_pct, 5.0), 99.9)

    # KV Cache：負載越高命中率越低，驅逐越多
    kv_hit          = round(max(0.45, 0.96 - eff * 0.45 + random.gauss(0, 0.03)), 3)
    kv_evictions    = max(0, int((eff - 0.75) * 80 + random.gauss(0, 4))) if eff > 0.75 else 0

    # 請求隊列：負載越高隊列越長
    queue_len       = max(0, int(eff * 24 + random.gauss(0, 3)))
    active_req      = max(1, int(eff * 20 + random.gauss(0, 2)))
    completed_req   = max(0, int((1.0 - eff) * 35 + random.gauss(0, 3)))

    # 延遲：TTFT 隨負載線性增長
    ttft_ms         = round(100 + eff * 380 + random.gauss(0, 20), 2)
    tpot_ms         = round(12  + eff * 14  + random.gauss(0, 1.5), 2)
    throughput      = max(200, int(2500 * (1 - eff * 0.45) + random.gauss(0, 60)))

    # CPU-GPU 管道
    pcie_bw         = round(eff * 14.5 + random.gauss(0, 0.6), 2)
    pcie_util       = round(eff * 90   + random.gauss(0, 3.0), 2)
    cpu_ram         = round(28  + eff * 52  + random.gauss(0, 2.5), 2)
    gpu_util        = round(eff * 100  + random.gauss(0, 2.5), 2)
    gpu_util        = min(max(gpu_util, 0), 100)

    return {
        "timestamp":                    datetime.now().isoformat(),
        "instance_id":                  node["id"],
        "gpu_model":                    node["gpu"],
        "vram_total_gb":                node["vram_total"],
        "vram_usage_percent":           vram_pct,
        "kv_cache_hit_rate":            kv_hit,
        "kv_cache_evictions":           kv_evictions,
        "request_queue_len":            queue_len,
        "active_requests":              active_req,
        "requests_completed":           completed_req,
        "ttft_ms":                      ttft_ms,
        "tpot_ms":                      tpot_ms,
        "throughput_tokens_s":          throughput,
        "pcie_bandwidth_gbps":          pcie_bw,
        "pcie_util_percent":            pcie_util,
        "cpu_ram_used_gb":              cpu_ram,
        "gpu_compute_util":             gpu_util,
        "cpu_gpu_pipeline_latency_ms":  round(pcie_bw * 2.8 + random.gauss(0, 1.5), 2),
    }


# ── 主循環 ────────────────────────────────────────────────────────────────
print("🚀 開始推送模擬 LLM 指標到 AWS S3...")
t = 0.0

while True:
    for node in NODES:
        log_data  = generate_mock_log(node, t)
        file_name = f"log_{node['id']}_{int(time.time() * 1000)}.json"
        file_path = os.path.join(LOCAL_LOG_DIR, file_name)
        s3_key    = f"raw_logs/{file_name}"

        # 寫本地
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False)

        try:
            # 上傳 S3
            s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
            print(f"[{log_data['timestamp'][:19]}] {node['id']} | "
                  f"VRAM={log_data['vram_usage_percent']:.1f}% | "
                  f"KV_hit={log_data['kv_cache_hit_rate']:.2f} | "
                  f"TTFT={log_data['ttft_ms']:.0f}ms | "
                  f"Evict={log_data['kv_cache_evictions']}")

            # 阅后即焚
            os.remove(file_path)
            s3_file_queue.append(s3_key)

        except Exception as e:
            print(f"❌ 操作失敗 [{node['id']}]: {e}")

    t += 1.0
    time.sleep(1)
