# COMP4442 Group Project: LLM 多节点性能实时监控系统

## 🌟 项目整体架构与业务流转
本项目是一个端到端的**多节点 LLM（大语言模型）性能实时监控系统**。我们将一整套大数据流处理及可视化方案完美部署在了 **AWS 云生态**中。

整个数据流向如下：
**本地模拟数据源 (Data Pipeline) ➡️ AWS S3 (数据湖) ➡️ AWS EMR (Spark 实时计算) ➡️ AWS RDS (数据仓库) ➡️ AWS EC2 (后端 API) ➡️ AWS S3 (前端静态托管)**

---

## 📂 项目各模块细节拆解

### 1. 数据采集与推送层 (`data_pipeline`)
*   **部署位置**：本地运行
*   **核心逻辑 (`data_pusher.py`)**：脚本充当了模拟的 vLLM 节点服务器（如 `vLLM-Node-1`, `vLLM-Node-2`）。它会以每秒一次的频率生成包含**时间戳**、**VRAM 显存占用率**、**吞吐量 (Tokens/s)** 以及 **CPU/GPU 管道延迟**的 JSON 格式日志文件。
*   **云端交互**：利用 `boto3` 将生成的日志实时推送到 AWS S3 的 `comp4442-llm-monitor-bucket/raw_logs/` 目录下。为了防止本地磁盘被打满，推送成功后会立即触发“阅后即焚”，删除本地 JSON 文件。

### 2. 大数据实时计算层 (`spark_analytics`)
*   **部署位置**：AWS EMR (Elastic MapReduce)，集群名 `COMP4442-EMR`
*   **核心逻辑 (`spark_stream.py`)**：使用 **PySpark Structured Streaming**。脚本中精巧地手动定义了 JSON Schema（表结构），这样 Spark 就不需要扫描 S3 的全量历史文件去推断格式，实现了秒级的极速冷启动。
*   **处理机制**：通过流式读取 S3 上的数据（限流控制），开启了一个时间窗口为 **5秒** 的微批处理（Micro-batch）。在 5 秒的周期内，按照 `instance_id` 进行分组聚合，算出：
    *   `avg_throughput` (平均吞吐量)
    *   `peak_vram_usage` (显存占用峰值)
    *   `avg_pipeline_latency` (平均延迟)
*   **数据落地**：将聚合好的一批干净、核心的指标数据，通过 JDBC 驱动，追加写 (Append) 入到 AWS RDS 的 MySQL 数据库表 `performance_summary` 中。

### 3. 后端服务层 (`backend_api`)
*   **部署位置**：AWS EC2 (弹性计算云)，项目名为 `LLM-Monitor-Backend`
*   **核心逻辑 (`main.py`)**：基于 Python 的极速 Web 框架 **FastAPI** 搭建，并且配置了 CORS 跨域中间件以便前端能顺利请求。
*   **API 接口**：
    *   `/api/performance`：去 RDS 数据库拉取最新的整体节点表现，并提取出各个节点最新的一条聚合数据以供前端总览展示。
    *   `/api/history`：根据指定的 Node ID，拉取该节点历史的各项性能指标序列数据，以便绘制历史趋势折线图。

### 4. 前端交互展示层 (`frontend_ui`)
*   **部署位置**：全新的 AWS S3 桶 (`llm-dashboard-web`)，配置为**静态网页托管 (Static Website Hosting)**
*   **访问地址**：[http://llm-dashboard-web.s3-website.ap-east-1.amazonaws.com/](http://llm-dashboard-web.s3-website.ap-east-1.amazonaws.com/)
*   **核心技术 (`index.html` / `history.html`)**：
    *   界面没有使用繁重的现代框架搭建，而是纯 HTML + Tailwind CSS，页面实现了磨砂玻璃拟物风和发光特效，十分美观和轻量。
    *   数据可视化使用了 **ECharts**，它会定期发起 HTTP 请求访问位于 EC2 的后端接口，动态渲染多节点 vLLM 的实时健康度和性能指标图表。

整个系统设计模块解耦非常清晰，充分利用了 AWS 的 Serverless (S3)、Managed Hadoop (EMR) 与关系型数据库 (RDS)，是一个非常成熟、标准的现代流式数据处理与监控架构。