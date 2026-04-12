import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pymysql
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
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
            sql = "SELECT instance_id, avg_throughput, peak_vram_usage, avg_pipeline_latency, last_update_time FROM performance_summary ORDER BY id DESC LIMIT 200"
            cursor.execute(sql)
            all_results = cursor.fetchall()

            if not all_results:
                return {"status": "error", "message": "Database is empty"}

            recent_results = all_results[-200:]
            latest_nodes_data = {}

            for row in recent_results:
                node_id = row.get("instance_id") or "vLLM-Node-1"
                if node_id not in latest_nodes_data:
                    latest_nodes_data[node_id] = row

        connection.close()

        return {
            "status": "success",
            "data": list(latest_nodes_data.values())
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@app.get("/api/history")
def get_node_history(
    instance_id: str = Query(..., description="Node instance ID"),
    limit: int = Query(0, description="Max records, 0 = all")
):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            if limit > 0:
                sql = """
                    SELECT id, instance_id, avg_throughput, peak_vram_usage,
                           avg_pipeline_latency, last_update_time
                    FROM performance_summary
                    WHERE instance_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                """
                cursor.execute(sql, (instance_id, limit))
            else:
                sql = """
                    SELECT id, instance_id, avg_throughput, peak_vram_usage,
                           avg_pipeline_latency, last_update_time
                    FROM performance_summary
                    WHERE instance_id = %s
                    ORDER BY id DESC
                """
                cursor.execute(sql, (instance_id,))

            results = cursor.fetchall()
        connection.close()

        if not results:
            return {"status": "error", "message": f"No data found for {instance_id}"}

        results.reverse()

        for row in results:
            if row.get("last_update_time"):
                row["last_update_time"] = str(row["last_update_time"])

        return {
            "status": "success",
            "instance_id": instance_id,
            "count": len(results),
            "data": results
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/nodes")
def get_all_nodes():
    """
    返回数据库中所有已知的 instance_id 列表。
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = "SELECT DISTINCT instance_id FROM performance_summary ORDER BY instance_id"
            cursor.execute(sql)
            results = cursor.fetchall()
        connection.close()

        nodes = [r["instance_id"] for r in results if r.get("instance_id")]
        return {"status": "success", "nodes": nodes}
    except Exception as e:
        return {"status": "error", "message": str(e)}