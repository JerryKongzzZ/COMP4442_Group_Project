# COMP4442_Group_Project
COMP4442 Group Project

### Group Structure
COMP4442_Group_Project/
│
├── data/
│   └── raw_logs.csv             # [成员A] 存放你们现成的 vLLM 实验数据底座
│
├── data_pipeline/
│   └── data_pusher.py           # [成员A] 模拟数据生成与推流脚本
│
├── spark_analytics/
│   └── spark_job.py             # [成员B] AWS EMR 上的大数据聚合脚本
│
├── backend_api/
│   ├── main.py                  # [成员C] FastAPI 后端接口
│   └── requirements.txt         # 记录后端需要的依赖库
│
└── frontend_ui/
    └── index.html               # [成员C] 监控大屏前端页面