# config/config.py
# ─── All settings in one place ─────────────────────────────────

# ── KAFKA ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS   = "localhost:9092"
KAFKA_TOPIC_TRANSACTIONS  = "mpesa-transactions"
KAFKA_TOPIC_FRAUD_ALERTS  = "mpesa-fraud-alerts"
KAFKA_GROUP_ID            = "fraud-detector-group"

# ── POSTGRESQL ─────────────────────────────────────────────────
POSTGRES_HOST     = "localhost"
POSTGRES_PORT     = 5432
POSTGRES_DB       = "mpesa_fraud"
POSTGRES_USER     = "mark"
POSTGRES_PASSWORD = "fraud2025"
POSTGRES_CONN     = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ── MINIO (running locally — not Docker) ───────────────────────
# Start MinIO locally:
#   minio server ~/minio-data --console-address ":9001"
# API  : http://localhost:9000
# UI   : http://localhost:9001  (minioadmin / minioadmin)

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_CONSOLE    = "http://localhost:9001"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET     = "mpesa-fraud-pipeline"

MINIO_RAW_PREFIX       = "raw/transactions/"
MINIO_PROCESSED_PREFIX = "processed/transactions/"
MINIO_REPORTS_PREFIX   = "reports/"

# Pass these to boto3.client('s3', **S3_CLIENT_KWARGS)
S3_CLIENT_KWARGS = {
    "endpoint_url":          MINIO_ENDPOINT,
    "aws_access_key_id":     MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
}

# ── SNOWFLAKE ──────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = "UGZTVPA-QS94058"
SNOWFLAKE_USER      = "mark"
SNOWFLAKE_PASSWORD  = "your_snowflake_password"
SNOWFLAKE_DATABASE  = "FRAUD_DB"
SNOWFLAKE_SCHEMA    = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# ── FRAUD RULES ────────────────────────────────────────────────
FRAUD_LARGE_AMOUNT    = 100_000   # KES 100,000
FRAUD_VELOCITY_COUNT  = 10        # >10 txns/hr
FRAUD_DORMANT_DAYS    = 90        # inactive 90 days
FRAUD_NIGHT_START     = 2         # 2 AM
FRAUD_NIGHT_END       = 4         # 4 AM
FRAUD_HIGH_RISK_SCORE = 0.75      # alert threshold
