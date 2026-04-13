# COMP4442 Group Project: LLM Multi-Node Performance Monitor

## Project Overview
This project is an end-to-end, real-time data processing and monitoring pipeline designed to track the performance of Large Language Model (LLM) inference nodes (e.g., vLLM instances). The system simulates, processes, and visualizes key performance metrics—such as VRAM usage, token throughput, and pipeline latency—across multiple distributed nodes. The entire data architecture is deployed on **AWS (Amazon Web Services)**.

## Architecture & Data Flow

1. **Data Generation & Ingestion (Local → AWS S3)**
   - The `data_pipeline` runs locally, generating simulated real-time logs for LLM nodes (`vLLM-Node-1`, `vLLM-Node-2`).
   - These JSON logs are pushed continuously to an **AWS S3 bucket** (`comp4442-llm-monitor-bucket`).
2. **Stream Processing (AWS EMR)**
   - The `spark_analytics` module runs on **AWS EMR** (`COMP4442-EMR`).
   - Using **PySpark Structured Streaming**, it consumes the raw logs from the S3 bucket in real-time.
   - It performs time-windowed aggregations (e.g., average throughput, peak VRAM, average latency) and writes the summarized results via JDBC to an **AWS RDS (MySQL)** database.
3. **Backend API (AWS EC2)**
   - The `backend_api` is a **FastAPI** application hosted on an **AWS EC2 instance**.
   - It connects to the AWS RDS database and exposes RESTful endpoints (`/api/performance`, `/api/history`) to serve the latest aggregated metrics to the frontend.
4. **Frontend Dashboard (AWS S3 Static Website)**
   - The `frontend_ui` consists of static HTML, Tailwind CSS, and ECharts.
   - It is hosted as a static website on a new **AWS S3 bucket**: [http://llm-dashboard-web.s3-website.ap-east-1.amazonaws.com/](http://llm-dashboard-web.s3-website.ap-east-1.amazonaws.com/).
   - The dashboard dynamically fetches data from the Backend API to visualize the performance metrics in real-time.

## Tech Stack

- **Cloud Platform**: Amazon Web Services (S3, EMR, RDS MySQL, EC2)
- **Data Engineering**: PySpark (Structured Streaming)
- **Backend**: Python, FastAPI, PyMySQL
- **Frontend**: HTML5, Tailwind CSS, ECharts (JavaScript)
- **Data Format**: JSON

## Directory Structure

```text
COMP4442_Group_Project/
│
├── data_pipeline/               # Local data generator & S3 pusher
│   ├── data_pusher.py           # Script to simulate logs and upload to S3
│   └── logs/                    # Temporary local storage for generated logs
│
├── spark_analytics/             # PySpark streaming jobs for AWS EMR
│   ├── spark_stream.py          # Main streaming job reading from S3 to RDS
│   ├── spark_job.py             # Additional Spark processing definitions
│   └── key/                     # SSH keys (e.g., comp4442-key.ppk) for EMR access
│
├── backend_api/                 # FastAPI backend hosted on AWS EC2
│   ├── main.py                  # API endpoints (/api/performance, /api/history)
│   └── requirements.txt         # Python dependencies (fastapi, pymysql, etc.)
│
├── frontend_ui/                 # Web dashboard hosted on AWS S3 Static Website
│   ├── index.html               # Main real-time monitoring dashboard
│   └── history.html             # Historical data visualization view
│
└── README.md                    # Project documentation
```

## Module Details

### 1. Data Pipeline (`data_pipeline`)
- **File**: `data_pusher.py`
- **Function**: Acts as a mock vLLM server. It generates JSON logs containing `timestamp`, `instance_id`, `vram_usage_percent`, `throughput_tokens_s`, and `cpu_gpu_pipeline_latency_ms`. It uploads these files to `s3://comp4442-llm-monitor-bucket/raw_logs/` and immediately deletes local copies to prevent disk overflow.

### 2. Spark Analytics (`spark_analytics`)
- **File**: `spark_stream.py`
- **Function**: Deployed on AWS EMR. It sets up a Spark Structured Streaming job with a predefined JSON schema for fast cold starts. It groups data by `instance_id` every 5 seconds, calculating `avg_throughput`, `peak_vram_usage`, and `avg_pipeline_latency`. The aggregated batch is then appended to the AWS RDS database `monitor_db.performance_summary`.

### 3. Backend API (`backend_api`)
- **File**: `main.py`
- **Function**: Deployed on AWS EC2. Built with FastAPI, it provides CORS-enabled REST endpoints to retrieve the latest 200 records or historical trends for a specific node instance. It securely connects to the AWS RDS instance using credentials managed via environment variables.

### 4. Frontend UI (`frontend_ui`)
- **Files**: `index.html`, `history.html`
- **Function**: Hosted on AWS S3. It uses Tailwind CSS for modern styling and ECharts for rich, interactive data visualization. The UI periodically polls the EC2 backend API to update the real-time node performance charts.