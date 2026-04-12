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
            # 🌟 新增了 last_update_time 字段，并且使用倒序，取最新 200 条
            sql = "SELECT instance_id, avg_throughput, peak_vram_usage, avg_pipeline_latency, last_update_time FROM performance_summary ORDER BY id DESC LIMIT 200"
            cursor.execute(sql)
            all_results = cursor.fetchall()
            
            if not all_results:
                return {"status": "error", "message": "Database is empty"}
                
            # all_results 已经是 [最新, 较老, 最老] 的顺序了
            recent_results = all_results[-200:]
            latest_nodes_data = {}
            
            # 正序遍历，因为排在最前面的就是最新的！
            for row in recent_results:
                # 兼容旧数据（如果 instance_id 是空的，默认为 Node-1）
                node_id = row.get("instance_id") or "vLLM-Node-1" 
                
                # 第一次碰到的 instance_id（也就是最新的那条），就存下来
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