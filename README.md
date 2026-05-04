# 🛡️ Real-Time Fraud Detection System

A production-grade, end-to-end fraud detection system using **LightGBM**, **Apache Kafka**, **FastAPI**, and **Streamlit**.  
Detects fraudulent transactions in **<50ms** latency with a full streaming pipeline.

---

## 🏗️ Architecture

```
[Transaction Source]
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  STREAMING LAYER                                                  │
│  Option A: Python Simulator  →  streaming/pipeline.py            │
│  Option B: Apache Kafka      →  kafka_producer → kafka_consumer  │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  PREPROCESSING LAYER                                             │
│  StandardScaler · Log-transform Amount · Drop Time              │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  MODEL LAYER                                                     │
│  LightGBM (GBDT) · SMOTE balancing · Auto threshold selection   │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  API LAYER  (FastAPI)                                            │
│  POST /predict  ·  POST /predict/batch  ·  GET /stats           │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  STORAGE LAYER                                                   │
│  SQLite (default) · PostgreSQL (production) · Redis (cache)     │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  DASHBOARD LAYER  (Streamlit)                                    │
│  Live charts · KPIs · Fraud alerts · Transaction table          │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📁 Folder Structure

```
fraud_detection/
│
├── data/                         ← Place creditcard.csv here
│   └── fraud_detection.db        ← SQLite DB (auto-created)
│
├── models/
│   ├── train.py                  ← Train LightGBM model
│   ├── predictor.py              ← Inference wrapper (singleton)
│   ├── lgbm_fraud_model.joblib   ← Saved model (after training)
│   ├── scaler.joblib             ← Saved scaler
│   └── model_meta.json           ← Metrics + threshold
│
├── streaming/
│   ├── simulator.py              ← Python-only stream (no Kafka)
│   ├── pipeline.py               ← Full local pipeline
│   ├── kafka_producer.py         ← Kafka producer
│   └── kafka_consumer.py         ← Kafka consumer + inference
│
├── api/
│   ├── main.py                   ← FastAPI app
│   └── database.py               ← SQLAlchemy ORM + helpers
│
├── dashboard/
│   └── app.py                    ← Streamlit live dashboard
│
├── utils/
│   └── preprocessing.py          ← Data cleaning, scaling, SMOTE
│
├── notebooks/
│   └── eda_and_evaluation.ipynb  ← EDA + model evaluation
│
├── docker/
│   └── docker-compose.yml        ← Kafka + Postgres + Redis
│
├── Dockerfile                    ← Container for API
├── requirements.txt
└── .env                          ← Config (DB URLs, ports, etc.)
```

---

## ⚡ Quick Start (5 minutes)

### Step 1 — Install dependencies

```bash
cd fraud_detection
pip install -r requirements.txt
```

### Step 2 — Get the dataset

1. Go to: https://www.kaggle.com/mlg-ulb/creditcardfraud
2. Download `creditcard.csv`
3. Place it in the `data/` folder

> **No dataset?** No problem — the simulator auto-generates synthetic data.

### Step 3 — Train the model

```bash
python -m models.train
```

Expected output:
```
[INFO] Loading dataset ...
[INFO] After SMOTE → fraud=227451, normal=227451
[INFO] Training LightGBM …
[INFO] Best threshold = 0.4231
  ROC-AUC           : 0.9987
  Average Precision : 0.8612
[INFO] Model saved → models/lgbm_fraud_model.joblib
```

### Step 4 — Run the local pipeline (Python only, no Kafka)

```bash
python -m streaming.pipeline --rate 10
```

You'll see live predictions:
```
✅ OK    | TXN-1718123456-4521 |  $  23.50 | P=0.002 [LOW]  | 4.2ms
🚨 FRAUD | TXN-1718123457-8834 |  $1847.00 | P=0.934 [HIGH] | 3.8ms
✅ OK    | TXN-1718123457-2219 |  $   8.99 | P=0.011 [LOW]  | 3.9ms
```

### Step 5 — Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Open your browser at: **http://localhost:8501**

---

## 🚀 Production Setup (with Kafka)

### Start infrastructure

```bash
docker-compose -f docker/docker-compose.yml up -d
```

This starts:
- **Kafka** on port `9092`
- **Zookeeper** on port `2181`
- **Kafka UI** on port `8080`  ← view topics visually
- **PostgreSQL** on port `5432`
- **Redis** on port `6379`

### Start Kafka producer (terminal 1)

```bash
python -m streaming.kafka_producer --rate 20
```

### Start Kafka consumer + inference (terminal 2)

```bash
python -m streaming.kafka_consumer
```

### Start FastAPI (terminal 3)

```bash
uvicorn api.main:app --reload --port 8000
```

API docs available at: **http://localhost:8000/docs**

### Start dashboard (terminal 4)

```bash
streamlit run dashboard/app.py
```

---

## 🔌 API Reference

### `POST /predict`

Predict a single transaction.

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 2500.00,
    "V1": -1.359807, "V2": -0.072781, "V3": 2.536347,
    "V4": 1.378155,  "V14": -0.311678
  }'
```

Response:
```json
{
  "transaction_id": "TXN-1718123456789-4521",
  "is_fraud": false,
  "probability": 0.0231,
  "confidence": "LOW",
  "threshold": 0.4231,
  "latency_ms": 4.82,
  "timestamp": "2024-06-11T14:23:01Z"
}
```

### `GET /stats`

```bash
curl http://localhost:8000/stats
```

```json
{
  "total": 15420,
  "fraud_count": 87,
  "normal_count": 15333,
  "fraud_rate": 0.56,
  "avg_latency_ms": 4.71
}
```

### `GET /transactions?limit=50`

Returns recent stored predictions.

---

## 📊 Model Performance

| Metric            | Value  |
|-------------------|--------|
| ROC-AUC           | ~0.998 |
| Average Precision | ~0.86  |
| Avg Latency       | <10ms  |
| Throughput        | >500 txn/s |

> Results vary based on train/test split and SMOTE randomness.

---

## 🔧 Configuration

Edit `.env` to configure:

```env
POSTGRES_URL=postgresql://postgres:password@localhost:5432/fraud_db
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions
MODEL_PATH=models/lgbm_fraud_model.joblib
FRAUD_THRESHOLD=0.5
```

---

## 🧪 Run Notebook (EDA + Evaluation)

```bash
pip install jupyter
jupyter notebook notebooks/eda_and_evaluation.ipynb
```

---

## 🌟 Extra Features to Add (Next Steps)

| Feature              | Description                                      |
|----------------------|--------------------------------------------------|
| Online learning      | Update model with new fraud patterns via River   |
| Email/SMS alerts     | Twilio / SendGrid for high-confidence fraud      |
| Anomaly detection    | Add Isolation Forest as second-stage detector    |
| Model versioning     | MLflow for tracking experiments                  |
| Cloud deployment     | AWS Lambda / GCP Cloud Run for API               |
| Grafana dashboard    | Professional metrics dashboard via Prometheus    |

---

## 🧑‍💻 Author

Built as a production-grade ML portfolio project.  
Stack: Python · LightGBM · Kafka · FastAPI · Streamlit · SQLAlchemy · Docker
