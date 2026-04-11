from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import random
from datetime import datetime

app = FastAPI(title="LLM Monitor API")

# 解决跨域问题，允许前端 HTML 直接调用
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/summary")
def get_summary():
    """
    提供项目要求 (a) 的行为摘要数据。
    联调阶段：请在这里使用 pymysql 连接 AWS RDS 替换下面的假数据。
    """
    return {
        "status": "success",
        "data": {
            "model_instance": "vLLM-Node-01",
            "total_requests": 14520,
            "avg_throughput": 1850.5,
            "peak_vram_usage": 98.2,
            "avg_pipeline_latency": 22.4
        }
    }

@app.get("/api/realtime")
def get_realtime_metrics():
    """
    提供项目要求 (b) 的实时监控图表数据 (每30秒刷新)。
    """
    # 模拟实时显存波动，有 20% 的概率触发爆满警告 (>95%)
    current_vram = random.choice([round(random.uniform(70, 85), 2), round(random.uniform(95.1, 99.0), 2)])
    return {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "vram_percent": current_vram,
        "latency_ms": round(random.uniform(15.0, 35.0), 2)
    }

# 运行命令: uvicorn main:app --host 0.0.0.0 --port 8000