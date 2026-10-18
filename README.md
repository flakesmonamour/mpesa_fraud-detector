# M-Pesa Real-Time Fraud Detection Pipeline

A real-time data engineering pipeline that ingests mobile money transactions, scores them for fraud as they happen, and stores the results for alerts and analysis.

Built by **Mark Wambugu** | GitHub: [flakesmonamour](https://github.com/flakesmonamour)

---

## Overview

Mobile money fraud costs Kenyan users and providers billions of shillings every year. Without real-time detection, fraud is found hours or days later, after the money is gone.

This project processes every transaction as an event, applies fraud rules within seconds, and flags suspicious activity immediately. Raw and scored data is then loaded into an analytics warehouse for reporting.

## Fraud Types Targeted

- **SIM swap fraud:** an attacker swaps a SIM and drains the account
- **Account takeover:** unauthorized access followed by large transfers
- **Unusual location:** transactions from an unfamiliar city or country
- **Velocity fraud:** many small transactions sent quickly to avoid detection
- **Dormant account activity:** sudden large transactions on long-inactive accounts

## Architecture

```
Transaction Event -> Ingestion Script -> Kafka Topic -> Spark Streaming
   -> Fraud Score -> PostgreSQL -> Snowflake -> Alert / Dashboard
```

1. **Ingestion:** a Python script polls the transaction API every 5 seconds and publishes new transactions to the Kafka topic `mpesa-transactions`.
2. **Streaming:** Kafka holds the events reliably, so nothing is lost if downstream processing slows down.
3. **Scoring:** a Spark Streaming job reads the topic and applies fraud rules to each transaction.
4. **Storage:** every scored transaction, fraudulent or clean, is saved to PostgreSQL. Flagged ones trigger an alert.
5. **Batch jobs:** Airflow runs nightly to sync data to S3 and Snowflake and generate a daily fraud report.
6. **Analytics:** analysts query Snowflake to find fraud by zone, time of day, and account type.

## Tech Stack

- **Python:** ingestion script
- **Apache Kafka:** message queue that decouples ingestion from processing
- **Apache Spark Streaming:** real-time fraud scoring
- **PostgreSQL:** operational store for scored transactions and alerts
- **AWS S3:** raw data lake for audit and model retraining
- **Snowflake:** analytics warehouse
- **Apache Airflow:** orchestration of nightly batch jobs
- **Docker:** containerizes Kafka, Spark, Airflow, and PostgreSQL

## Fraud Detection Rules

- **Large transaction:** amount over KES 100,000
- **Velocity:** more than 10 transactions in 1 hour
- **New device:** transaction from an unrecognized device ID
- **Unusual location:** different county or country than usual
- **Dormant account:** activity after more than 90 days of inactivity
- **Night transaction:** large transaction between 2am and 4am

## Database Schema

A star schema centered on `fact_transactions`.

**fact_transactions**

- `transaction_id` (PK): unique transaction identifier
- `customer_id` (FK): links to `dim_customer`
- `date_id` (FK): links to `dim_date`
- `location_id` (FK): links to `dim_location`
- `amount`: transaction amount in KES
- `txn_type`: send, receive, withdraw, or paybill
- `is_fraud`: 1 if flagged, 0 if clean
- `fraud_score`: 0.0 to 1.0, how suspicious the transaction is
- `fraud_reason`: why it was flagged, e.g. `large_amount`, `new_device`
- `processed_at`: when Spark scored the transaction

**Dimension tables:** `dim_customer`, `dim_date`, `dim_location`, `dim_device`

## Project Structure

```
mpesa_fraud_pipeline/
├── ingestion/
│   └── producer.py          # fetches transactions, pushes to Kafka
├── streaming/
│   └── fraud_detector.py    # Spark Streaming fraud scoring
├── dags/
│   └── fraud_pipeline.py    # Airflow DAG for nightly batch jobs
├── sql/
│   ├── create_schema.sql    # PostgreSQL star schema
│   └── fraud_queries.sql    # analyst queries
├── docker-compose.yml       # Kafka, Spark, Airflow, PostgreSQL
├── requirements.txt
└── README.md
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ with `confluent-kafka`, `pyspark`, and `psycopg2`
- AWS credentials configured for S3
- Snowflake account credentials

### Installation

```bash
git clone https://github.com/flakesmonamour/<repo-name>.git
cd mpesa_fraud_pipeline
pip install -r requirements.txt
```

### Start the infrastructure

```bash
docker-compose up -d
docker ps   # verify Kafka, Spark, Postgres, and Airflow are running
```

### Run the pipeline

```bash
# 1. Start the ingestion producer
python ingestion/producer.py

# 2. Start the Spark Streaming fraud detector
spark-submit streaming/fraud_detector.py
```

Airflow runs the nightly batch jobs automatically. Open the Airflow UI at http://localhost:8080.

## Airflow DAG

The `fraud_pipeline` DAG runs daily at 01:00:

```
sync_to_s3 >> load_snowflake >> daily_fraud_report
```

## Business Value

- Fraud is flagged within seconds instead of hours or days
- Every transaction is scored automatically, with no manual review needed
- Every decision is stored with its reason, giving a full audit trail
- Daily fraud reports are generated automatically

## Future Improvements

- Replace fixed rule thresholds with a trained ML model
- Add a live dashboard for flagged transactions
- Add automated tests and data quality checks

## Author

**Mark Wambugu**, Data Science and Analytics student, Zetech University, Kenya
GitHub: [flakesmonamour](https://github.com/flakesmonamour)
