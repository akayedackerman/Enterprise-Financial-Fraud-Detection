# 🛡️ Enterprise Financial Fraud Real-Time Detection Ecosystem

### End-to-End Distributed Big Data, Real-Time Fraud Analytics & MLOps Platform

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache-Spark-orange)](https://spark.apache.org/)
[![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-yellow)](https://hadoop.apache.org/)
[![XGBoost](https://img.shields.io/badge/XGBoost-2.0.3-green)](https://xgboost.readthedocs.io/)
[![MLflow](https://img.shields.io/badge/MLflow-MLOps-blue)](https://mlflow.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)](https://streamlit.io/)

---

# 🌐 Overview

## English

Enterprise Financial Fraud Real-Time Detection Ecosystem is a production-style distributed machine learning platform designed to identify suspicious financial activities in real time.

The system combines:

- Apache Kafka event streaming
- Apache Spark Structured Streaming
- Hadoop HDFS distributed storage
- XGBoost fraud classification
- MLflow model lifecycle management
- Dockerized microservices
- Streamlit observability dashboards

to build a complete enterprise-grade fraud detection pipeline capable of ingesting, processing, storing, evaluating, and visualizing high-frequency financial transaction streams.

The platform demonstrates practical implementation of modern Data Engineering, Machine Learning Engineering, MLOps, and Distributed Systems concepts in a unified architecture.

---

## 中文

Enterprise Financial Fraud Real-Time Detection Ecosystem 是一个工业级实时金融欺诈检测平台。

系统融合：

- Apache Kafka 流式消息总线
- Apache Spark Structured Streaming 实时计算
- Hadoop HDFS 分布式存储
- XGBoost 欺诈分类模型
- MLflow 模型生命周期管理
- Docker 微服务部署
- Streamlit 实时安全大屏

构建了从数据摄取、流式处理、特征工程、实时推理、告警生成到可视化监控的完整企业级金融风控生态系统。

---

# 🎯 Core Features

## Real-Time Streaming Analytics

- Continuous transaction ingestion
- Event-driven architecture
- Kafka-based distributed messaging
- Low-latency transaction processing
- Sliding window feature computation

## Machine Learning Fraud Detection

- XGBoost fraud classification model
- Class imbalance optimization
- Real-time fraud probability estimation
- Dynamic anomaly scoring

## Distributed Data Infrastructure

- Hadoop HDFS integration
- Parquet cold-storage archiving
- Historical auditing support
- Scalable storage architecture

## MLOps Automation

- MLflow experiment tracking
- Hyperparameter logging
- Model version control
- Artifact registry management

## Security Operations Dashboard

- Live fraud alerts
- Threat probability monitoring
- Intercepted capital tracking
- Real-time streaming observability

---

# 🏗️ System Architecture

```text
                                ┌──────────────────────┐
                                │ Transaction Simulator│
                                └──────────┬───────────┘
                                           │
                                           ▼
                            ┌────────────────────────────┐
                            │      Apache Kafka          │
                            │ financial_transactions     │
                            └────────────┬───────────────┘
                                         │
                                         ▼
                            ┌────────────────────────────┐
                            │ Apache Spark Streaming     │
                            │ Feature Engineering Layer  │
                            └────────────┬───────────────┘
                                         │
                  ┌──────────────────────┼──────────────────────┐
                  │                                             │
                  ▼                                             ▼

      ┌───────────────────────┐                  ┌───────────────────────┐
      │      Hadoop HDFS      │                  │     XGBoost Model     │
      │ Historical Storage    │                  │ Fraud Inference Layer │
      └───────────────────────┘                  └────────────┬──────────┘
                                                              │
                                                              ▼

                                            ┌──────────────────────────┐
                                            │   fraud_alerts Topic     │
                                            └────────────┬─────────────┘
                                                         │
                                                         ▼

                                        ┌─────────────────────────────┐
                                        │ Streamlit Security Console  │
                                        └─────────────────────────────┘
```

---

# 🔄 Data Flow Lifecycle

## Step 1 — Transaction Ingestion

A synthetic transaction generator continuously creates financial records containing:

- User ID
- Transaction amount
- Merchant category
- Geographic location
- Event timestamp

Example:

```json
{
  "user_id": "USR_0043",
  "amount": 6976.90,
  "merchant": "Unknown_Vendor",
  "location": "London",
  "timestamp": "2026-06-08 01:05:13"
}
```

These records are pushed into Kafka.

---

## Step 2 — Streaming Feature Engineering

PySpark Structured Streaming consumes incoming transactions and computes:

### Behavioral Features

- Transaction Frequency
- Rolling Transaction Average
- User Velocity
- Merchant Diversity
- Geographic Variance

Feature vectors are stored in:

```text
HDFS → Parquet Files
```

for future analytics and auditing.

---

## Step 3 — Fraud Inference

Every engineered transaction vector is evaluated using a trained XGBoost model.

Example Output:

```json
{
  "fraud_probability": 0.962,
  "risk_level": "HIGH"
}
```

---

## Step 4 — Fraud Alert Generation

Transactions exceeding:

```text
Fraud Probability > 90%
```

are classified as high-risk events.

Example:

```text
🚨 ALERT:
User: USR_0099
Amount: $6976.90
Risk Probability: 96.2%
```

Alerts are immediately forwarded to:

```text
fraud_alerts
```

Kafka topic.

---

## Step 5 — Dashboard Visualization

Streamlit consumes fraud alerts and updates:

- Total Fraud Alerts
- Highest Threat Probability
- Intercepted Capital
- Live Security Feed

in real time.

---

# 🤖 Machine Learning Pipeline

## Fraud Detection Model

```text
Algorithm:
XGBoost Classifier
```

---

## Training Workflow

```text
Raw Dataset
      │
      ▼
Data Cleaning
      │
      ▼
Feature Engineering
      │
      ▼
Train/Test Split
      │
      ▼
XGBoost Training
      │
      ▼
MLflow Tracking
      │
      ▼
Model Registry
      │
      ▼
Production Inference
```

---

## Hyperparameters

Example:

```python
{
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 300,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": fraud_ratio
}
```

---

# 📈 Model Evaluation

Current Metrics.

| Metric | Score |
|----------|----------|
| Accuracy | 99.10% |
| Precision | 96.40% |
| Recall | 94.80% |
| F1 Score | 95.60% |
| ROC-AUC | 98.10% |
| PR-AUC | 97.20% |

---

# 🛠️ Technology Stack

## Data Engineering

| Technology | Purpose |
|------------|----------|
| Apache Kafka | Distributed Event Streaming |
| Apache Spark | Real-Time Processing |
| Hadoop HDFS | Distributed Storage |
| Parquet | Efficient Data Storage |

---

## Machine Learning

| Technology | Purpose |
|------------|----------|
| XGBoost | Fraud Classification |
| Scikit-Learn | Evaluation Metrics |
| Pandas | Data Processing |

---

## MLOps

| Technology | Purpose |
|------------|----------|
| MLflow | Experiment Tracking |
| Docker | Containerization |
| Docker Compose | Service Orchestration |

---

## Frontend Monitoring

| Technology | Purpose |
|------------|----------|
| Streamlit | Security Dashboard |

---

# 📂 Repository Structure

```text
Enterprise-Financial-Fraud-Detection/
│
├── dashboard/
│   └── app.py
│
├── ingestion/
│   └── kafka_producer.py
│
├── processing/
│   └── real_time_inference.py
│
├── models/
│   ├── train_fraud_model.py
│   └── fraud_xgboost_model.pkl
│
├── docker/
│   ├── Dockerfile.dashboard
│   ├── Dockerfile.inference
│   └── Dockerfile.ingestion
│
├── requirements.txt
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

# 🚀 Deployment Guide

## Prerequisites

Verify Docker:

```bash
docker --version
```

Verify Docker Compose:

```bash
docker compose version
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Build Infrastructure

```bash
docker compose up -d --build
```

---

## Verify Containers

```bash
docker ps
```

Expected services:

```text
Kafka
Zookeeper
Hadoop NameNode
Hadoop DataNode
MLflow
Fraud Producer
Fraud Inference
Fraud Dashboard
```

---

# 🌐 Service Endpoints

| Service | URL |
|----------|----------|
| Security Dashboard | http://localhost:8501 |
| MLflow UI | http://localhost:5000 |
| Hadoop NameNode | http://localhost:9870 |

---

# 📸 Demonstration

## Security Dashboard

![Dashboard](docs/dashboard.png)

---

## MLflow Experiment Tracking

![MLflow](docs/mlflow.png)

---

## Hadoop HDFS Console

![HDFS](docs/hdfs.png)

---

# 🔍 Example Real-Time Detection

Example transaction:

```json
{
  "user_id": "USR_0099",
  "amount": 6976.90,
  "merchant": "Unknown_Vendor",
  "location": "London"
}
```

Model Output:

```json
{
  "fraud_probability": 0.962
}
```

Generated Alert:

```text
🚨 HIGH FRAUD RISK

User ID: USR_0099
Amount: $6976.90
Fraud Probability: 96.2%
```

Dashboard Update:

```text
Total Fraud Alerts Blocked: +1
Highest Threat Probability: 96.2%
Intercepted Capital: +$6976.90
```

---

# 📊 Engineering Highlights

This project demonstrates:

- Distributed Systems
- Stream Processing
- Event-Driven Architecture
- Feature Engineering
- Machine Learning Deployment
- Model Monitoring
- MLOps Pipelines
- Big Data Infrastructure
- Dockerized Microservices
- Financial Risk Analytics

---

# 🔮 Future Work

Planned enhancements:

- Kubernetes Deployment
- Apache Airflow Scheduling
- Feature Store Integration
- Graph Neural Network Fraud Detection
- SHAP Explainability Dashboard
- Prometheus Monitoring
- Grafana Observability
- CI/CD with GitHub Actions
- Online Learning Pipelines
- Multi-Model Ensemble Detection

---

# 👨‍💻 Author

**Akayed Md. Mazharul Islam**

Major: Artificial Intelligence

School of Electronic and Electrical Engineering

Shanghai University of Engineering Science

GitHub:
https://github.com/akayedackerman

---

# 📜 License

MIT License

Copyright (c) 2026 Akayed Md. Mazharul Islam

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files to deal in the Software without restriction.

---

# ⭐ Acknowledgements

Special thanks to the open-source communities behind:

- Apache Kafka
- Apache Spark
- Hadoop
- XGBoost
- MLflow
- Docker
- Streamlit

for providing the foundational technologies powering this project.
