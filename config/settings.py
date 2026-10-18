# config/settings.py
# Connection settings for the fraud pipeline
# MinIO is running locally (not Docker) at localhost:9000

# ─── KAFKA ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS  = 'localhost:9092'
KAFKA_TOPIC_TRANSACTIONS = 'mpesa-transactions'
KAFKA_TOPIC_ALERTS       = 'fraud-alerts'
KAFKA_GROUP_ID           = 'fraud-detector-group'

# ─── POSTGRESQL ─────────────────────────────────────────────────
POSTGRES_HOST     = 'localhost'
POSTGRES_PORT     = 5432
POSTGRES_DB       = 'mpesa_fraud'
POSTGRES_USER     = 'mark'
POSTGRES_PASSWORD = 'fraud2025'

POSTGRES_CONN_STRING = (
    f"host={POSTGRES_HOST} "
    f"port={POSTGRES_PORT} "
    f"dbname={POSTGRES_DB} "
    f"user={POSTGRES_USER} "
    f"password={POSTGRES_PASSWORD}"
)

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ─── MINIO (local install, not Docker) ──────────────────────────
# Start command:
#   minio server ~/minio-data --console-address ":9001"
#
# API : http://localhost:9000   ← boto3 endpoint_url
# UI  : http://localhost:9001   ← browser, login minioadmin/minioadmin

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_CONSOLE    = "http://localhost:9001"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET     = "mpesa-fraud-pipeline"

MINIO_RAW_PREFIX       = "raw/transactions/"
MINIO_PROCESSED_PREFIX = "processed/transactions/"
MINIO_REPORTS_PREFIX   = "reports/"

# Use this dict in every boto3.client() call
S3_CLIENT_KWARGS = {
    "endpoint_url":          MINIO_ENDPOINT,
    "aws_access_key_id":     MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
}

# ─── SNOWFLAKE ──────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = 'UGZTVPA-QS94058'
SNOWFLAKE_USER      = 'mark'
SNOWFLAKE_PASSWORD  = 'your_snowflake_password'
SNOWFLAKE_DATABASE  = 'FRAUD_DB'
SNOWFLAKE_SCHEMA    = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

# ─── FRAUD RULES ────────────────────────────────────────────────
FRAUD_RULES = {
    'large_amount_threshold':      100_000,
    'velocity_1hr_threshold':      10,
    'velocity_24hr_threshold':     50,
    'dormant_days_threshold':      90,
    'night_start_hour':            2,
    'night_end_hour':              4,
    'night_large_amount':          50_000,
    'fraud_score_alert_threshold': 0.75,
}
