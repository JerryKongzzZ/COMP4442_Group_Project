-- 在 RDS MySQL 執行此 SQL，建立 node_summary 表（供功能 A 使用）
-- 執行方式：mysql -h <RDS_HOST> -u admin -p monitor_db < init_db.sql

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
    pcie_bottleneck_count     INT,
    avg_gpu_util              DOUBLE,
    total_requests_completed  BIGINT,
    record_count              BIGINT,
    health_score              DOUBLE,
    data_start                VARCHAR(50),
    data_end                  VARCHAR(50),
    computed_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_instance (instance_id)
);

-- 同時確保 performance_summary 表的新欄位存在（若已有舊表，執行以下 ALTER）
ALTER TABLE performance_summary
    ADD COLUMN IF NOT EXISTS gpu_model              VARCHAR(50),
    ADD COLUMN IF NOT EXISTS avg_vram_usage         DOUBLE,
    ADD COLUMN IF NOT EXISTS avg_ttft_ms            DOUBLE,
    ADD COLUMN IF NOT EXISTS avg_kv_hit_rate        DOUBLE,
    ADD COLUMN IF NOT EXISTS kv_evictions_in_batch  INT,
    ADD COLUMN IF NOT EXISTS avg_gpu_util           DOUBLE,
    ADD COLUMN IF NOT EXISTS requests_completed     BIGINT;
