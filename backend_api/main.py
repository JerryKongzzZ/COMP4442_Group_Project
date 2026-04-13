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


# ── 静态网页托管 ──────────────────────────────────────────────────────────
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


# ── 功能 B：实时监控数据 ──────────────────────────────────────────────────
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


# ── 功能 A：实时汇总摘要（直接从数据库计算，不需要 spark_job）────────────────
@app.get("/api/summary")
def get_cluster_summary():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    instance_id,
                    MAX(gpu_model) as gpu_model,
                    COUNT(*) as record_count,
                    ROUND(SUM(avg_throughput), 0) as total_tokens,
                    ROUND(AVG(avg_throughput), 2) as avg_throughput,
                    SUM(CASE WHEN peak_vram_usage > 90 THEN 1 ELSE 0 END) as vram_exceed_count,
                    ROUND(SUM(CASE WHEN peak_vram_usage > 90 THEN 1 ELSE 0 END) / 60.0, 1) as vram_high_duration_min,
                    ROUND(MAX(peak_vram_usage), 2) as peak_vram_usage,
                    ROUND(AVG(avg_vram_usage), 2) as avg_vram_usage,
                    SUM(CASE WHEN kv_evictions_in_batch > 0 THEN 1 ELSE 0 END) as kv_eviction_count,
                    SUM(kv_evictions_in_batch) as total_kv_evictions,
                    ROUND(AVG(avg_kv_hit_rate), 3) as avg_kv_hit_rate,
                    ROUND(SUM(CASE WHEN avg_vram_usage < 30 THEN 1 ELSE 0 END) / 60.0, 1) as idle_duration_min,
                    ROUND(AVG(avg_ttft_ms), 2) as avg_ttft_ms,
                    ROUND(MAX(avg_ttft_ms), 2) as max_ttft_ms,
                    ROUND(AVG(avg_pipeline_latency), 2) as avg_pipeline_latency,
                    ROUND(AVG(avg_gpu_util), 2) as avg_gpu_util,
                    SUM(requests_completed) as total_requests_completed,
                    MIN(last_update_time) as data_start,
                    MAX(last_update_time) as data_end,
                    ROUND(LEAST(100,
                        (SUM(CASE WHEN peak_vram_usage > 90 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 60 +
                        (SUM(CASE WHEN kv_evictions_in_batch > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 30 +
                        GREATEST(0, (AVG(avg_ttft_ms) - 200) / 10)
                    ), 1) as health_score
                FROM performance_summary
                WHERE instance_id LIKE 'vLLM-Node-%'
                GROUP BY instance_id
                ORDER BY health_score DESC
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            return {"status": "error", "message": "No summary data yet."}

        for r in rows:
            for k in ("data_start", "data_end"):
                if r.get(k):
                    r[k] = str(r[k])

        return {"status": "success", "count": len(rows), "data": rows}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── 单节点历史趋势 ────────────────────────────────────────────────────────
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

        return {"status": "success", "instance_id": instance_id, "count": len(rows), "data": rows}

    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── 节点列表 ──────────────────────────────────────────────────────────────
@app.get("/api/nodes")
def get_all_nodes():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT instance_id FROM performance_summary ORDER BY instance_id")
            rows = cur.fetchall()
        conn.close()
        nodes = [r["instance_id"] for r in rows if r.get("instance_id")]
        return {"status": "success", "nodes": nodes}

    except Exception as e:
        return {"status": "error", "message": str(e)}
