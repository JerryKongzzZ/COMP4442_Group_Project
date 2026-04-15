-- Database initialisation script for LLM Cluster Monitor System
-- Run once on a fresh RDS instance:
--   mysql -h <RDS_HOST> -u admin -p monitor_db < init_db.sql

-- ── Function B table: one row per node per 5-second micro-batch ──────────────
-- Written by spark_unified.py (foreachBatch, append mode) every 5 seconds.
CREATE TABLE IF NOT EXISTS performance_summary (
    id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
    instance_id               VARCHAR(50)    NOT NULL,
    gpu_model                 VARCHAR(50),
    avg_throughput            DOUBLE,
    peak_vram_usage           DOUBLE,
    avg_vram_usage            DOUBLE,
    avg_pipeline_latency      DOUBLE,
    avg_ttft_ms               DOUBLE,
    avg_kv_hit_rate           DOUBLE,
    kv_evictions_in_batch     INT,
    avg_gpu_util              DOUBLE,
    requests_completed        BIGINT,
    last_update_time          VARCHAR(50),
    INDEX idx_instance_id (instance_id),
    INDEX idx_id_desc (id DESC)
);

-- ── Function A table: one row per node, overwritten every 60 seconds ─────────
-- Written by spark_unified.py (refresh_node_summary, overwrite mode).
-- Columns exactly match the fields selected in spark_unified.py's summary_df.
CREATE TABLE IF NOT EXISTS node_summary (
    id                        INT AUTO_INCREMENT PRIMARY KEY,
    instance_id               VARCHAR(50)    NOT NULL,
    gpu_model                 VARCHAR(50),
    total_tokens              BIGINT,
    avg_throughput            DOUBLE,
    vram_exceed_count         INT,
    vram_high_duration_min    DOUBLE,
    peak_vram_usage           DOUBLE,
    avg_vram_usage            DOUBLE,
    kv_eviction_count         INT,
    total_kv_evictions        INT,
    avg_kv_hit_rate           DOUBLE,
    idle_duration_min         DOUBLE,
    avg_ttft_ms               DOUBLE,
    max_ttft_ms               DOUBLE,
    avg_pipeline_latency      DOUBLE,
    avg_gpu_util              DOUBLE,
    total_requests_completed  BIGINT,
    record_count              BIGINT,
    health_score              DOUBLE,
    data_start                VARCHAR(50),
    data_end                  VARCHAR(50),
    computed_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_instance (instance_id)
);
