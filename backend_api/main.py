import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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


# ── Static file hosting ───────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="/home/ubuntu/static"), name="static")

@app.get("/")
def root():
    return FileResponse("/home/ubuntu/static/index.html")

@app.get("/history.html")
def history_page():
    return FileResponse("/home/ubuntu/static/history.html")

@app.get("/history")
def history_page2():
    return FileResponse("/home/ubuntu/static/history.html")


# ── Function B: real-time per-node monitoring data ───────────────────────────
@app.get("/api/performance")
def get_llm_performance():
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


# ── Function A: historical cluster summary (Spark-computed, read from node_summary) ──
@app.get("/api/summary")
def get_cluster_summary():
    """
    Returns the pre-computed historical summary written by the Spark unified job.
    spark_unified.py refreshes node_summary every ~60 seconds via a full
    aggregation over performance_summary; this endpoint simply reads the result.
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT instance_id, gpu_model,
                       total_tokens, avg_throughput,
                       vram_exceed_count, vram_high_duration_min,
                       peak_vram_usage, avg_vram_usage,
                       kv_eviction_count, total_kv_evictions, avg_kv_hit_rate,
                       idle_duration_min,
                       avg_ttft_ms, max_ttft_ms, avg_pipeline_latency,
                       avg_gpu_util, total_requests_completed,
                       record_count, health_score,
                       data_start, data_end
                FROM node_summary
                WHERE instance_id LIKE 'vLLM-Node-%%'
                ORDER BY health_score DESC
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return {"status": "error", "message": "No summary data yet. "
                    "The Spark job refreshes node_summary every 60 seconds."}

        for r in rows:
            for k in ("data_start", "data_end"):
                if r.get(k):
                    r[k] = str(r[k])

        return {"status": "success", "count": len(rows), "data": rows}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── Per-node historical trend ─────────────────────────────────────────────────
@app.get("/api/history")
def get_node_history(
    instance_id: str = Query(...),
    limit: int = Query(0)
):
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
                    ORDER BY id DESC LIMIT %s
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

        return {"status": "success", "instance_id": instance_id,
                "count": len(rows), "data": rows}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── Node list ─────────────────────────────────────────────────────────────────
@app.get("/api/nodes")
def get_all_nodes():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT instance_id FROM performance_summary "
                "ORDER BY instance_id"
            )
            rows = cur.fetchall()
        conn.close()
        nodes = [r["instance_id"] for r in rows if r.get("instance_id")]
        return {"status": "success", "nodes": nodes}

    except Exception as e:
        return {"status": "error", "message": str(e)}
