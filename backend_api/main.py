import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pymysql
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="LLM Cluster Monitor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "host":        os.getenv("DB_HOST"),
    "user":        os.getenv("DB_USER"),
    "password":    os.getenv("DB_PASSWORD"),
    "database":    os.getenv("DB_NAME"),
    "port":        int(os.getenv("DB_PORT", 3306)),
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_conn():
    return pymysql.connect(**DB_CONFIG)


# ── 功能 B 用：最新一條流式聚合數據（實時監控）─────────────────────────────
@app.get("/api/performance")
def get_llm_performance():
    """
    返回每個節點最新的實時聚合指標，供實時圖表（功能 B）使用。
    從 performance_summary 流水表讀取最新一條。
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT instance_id, gpu_model,
                       avg_throughput, peak_vram_usage, avg_vram_usage,
                       avg_pipeline_latency, avg_ttft_ms,
                       avg_kv_hit_rate, kv_evictions_in_batch,
                       avg_gpu_util, requests_completed, last_update_time
                FROM performance_summary
                ORDER BY id DESC
                LIMIT 200
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return {"status": "error", "message": "Database is empty"}

        # 每個節點只取最新一條
        latest = {}
        for r in rows:
            nid = r.get("instance_id") or "vLLM-Node-Unknown"
            if nid not in latest:
                latest[nid] = r
                if r.get("last_update_time"):
                    r["last_update_time"] = str(r["last_update_time"])

        return {"status": "success", "data": list(latest.values())}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── 功能 A 用：Spark 批處理的歷史摘要（每節點一行）──────────────────────────
@app.get("/api/summary")
def get_cluster_summary():
    """
    返回 Spark 批處理生成的每節點完整摘要，供摘要表格（功能 A）使用。
    從 node_summary 表讀取，每個節點只有一行彙總數據。
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    instance_id, gpu_model,
                    total_tokens, avg_throughput,
                    vram_exceed_count, vram_high_duration_min,
                    peak_vram_usage, avg_vram_usage,
                    kv_eviction_count, total_kv_evictions, avg_kv_hit_rate,
                    idle_duration_min,
                    avg_ttft_ms, max_ttft_ms,
                    avg_pipeline_latency,
                    pcie_bottleneck_count,
                    avg_gpu_util,
                    total_requests_completed,
                    health_score,
                    data_start, data_end
                FROM node_summary
                ORDER BY health_score DESC
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return {"status": "error", "message": "No summary data yet. Run spark_job.py first."}

        for r in rows:
            for k in ("data_start", "data_end"):
                if r.get(k):
                    r[k] = str(r[k])

        return {"status": "success", "count": len(rows), "data": rows}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── 單節點歷史趨勢（實時監控頁面歷史回顧）───────────────────────────────────
@app.get("/api/history")
def get_node_history(
    instance_id: str = Query(..., description="Node instance ID"),
    limit: int       = Query(0,   description="Max records, 0 = all")
):
    """
    返回指定節點的流式聚合歷史記錄，用於 index.html 的趨勢圖。
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if limit > 0:
                cur.execute("""
                    SELECT id, instance_id, gpu_model,
                           avg_throughput, peak_vram_usage, avg_vram_usage,
                           avg_pipeline_latency, avg_ttft_ms,
                           avg_kv_hit_rate, kv_evictions_in_batch,
                           avg_gpu_util, last_update_time
                    FROM performance_summary
                    WHERE instance_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                """, (instance_id, limit))
            else:
                cur.execute("""
                    SELECT id, instance_id, gpu_model,
                           avg_throughput, peak_vram_usage, avg_vram_usage,
                           avg_pipeline_latency, avg_ttft_ms,
                           avg_kv_hit_rate, kv_evictions_in_batch,
                           avg_gpu_util, last_update_time
                    FROM performance_summary
                    WHERE instance_id = %s
                    ORDER BY id DESC
                """, (instance_id,))
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return {"status": "error", "message": f"No data for {instance_id}"}

        rows.reverse()
        for r in rows:
            if r.get("last_update_time"):
                r["last_update_time"] = str(r["last_update_time"])

        return {
            "status":      "success",
            "instance_id": instance_id,
            "count":       len(rows),
            "data":        rows,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── 節點列表 ─────────────────────────────────────────────────────────────
@app.get("/api/nodes")
def get_all_nodes():
    """返回數據庫中所有節點的 ID 列表。"""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT instance_id
                FROM performance_summary
                ORDER BY instance_id
            """)
            rows = cur.fetchall()
        conn.close()

        nodes = [r["instance_id"] for r in rows if r.get("instance_id")]
        return {"status": "success", "nodes": nodes}

    except Exception as e:
        return {"status": "error", "message": str(e)}
