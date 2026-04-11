import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymysql
from dotenv import load_dotenv

# 👇 这一行极其重要！它会自动读取同目录下的 .env 文件
load_dotenv()

app = FastAPI()

# 允许跨域（前端调用必备）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🛠️ 数据库配置：现在全部从 .env 文件安全读取！
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    # 注意：.env 读出来的都是字符串，port 需要转成整数
    "port": int(os.getenv("DB_PORT", 3306)), 
    "cursorclass": pymysql.cursors.DictCursor
}

@app.get("/api/performance")
def get_llm_performance():
    """
    提供给前端大屏调用的接口
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = "SELECT avg_throughput, peak_vram_usage, avg_pipeline_latency FROM performance_summary"
            cursor.execute(sql)
            result = cursor.fetchone() 
            
        connection.close()
        
        return {
            "status": "success",
            "data": result
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }