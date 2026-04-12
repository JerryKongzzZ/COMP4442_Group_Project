import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymysql
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)), 
    "cursorclass": pymysql.cursors.DictCursor
}

@app.get("/api/performance")
def get_llm_performance():
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            # 取出 instance_id
            sql = "SELECT instance_id, avg_throughput, peak_vram_usage, avg_pipeline_latency FROM performance_summary"
            cursor.execute(sql)
            all_results = cursor.fetchall()
            
            if not all_results:
                return {"status": "error", "message": "Database is empty"}
                
            # 🌟 核心算法：提取每个节点最新的一条数据
            # 为了防止内存溢出，我们只看最后 200 条记录
            recent_results = all_results[-200:]
            latest_nodes_data = {}
            
            # 倒序遍历，第一次碰到的 instance_id 就是最新的
            for row in reversed(recent_results):
                # 兼容旧数据（如果 instance_id 是空的，默认为 Node-1）
                node_id = row.get("instance_id") or "vLLM-Node-1" 
                if node_id not in latest_nodes_data:
                    latest_nodes_data[node_id] = row
            
        connection.close()
        
        return {
            "status": "success",
            "data": list(latest_nodes_data.values()) # 返回数组
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }