"""
sync_to_s3.py — Airflow Task 1
================================
Exports transactions from PostgreSQL to MinIO (S3-compatible).
Saves as Parquet partitioned by date for efficient querying.

MinIO path: s3://mpesa-fraud-pipeline/raw/transactions/year=YYYY/month=MM/day=DD/
MinIO UI  : http://localhost:9001  (flakes / Mw0722352336)
MinIO API : http://localhost:9000
"""

import boto3
import psycopg2
import psycopg2.extras
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from botocore.client import Config
import logging

log = logging.getLogger(__name__)

# ── PostgreSQL ─────────────────────────────────────────────────────
DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'mpesa_fraud',
    'user':     'mark',
    'password': 'fraud2025',
}

# ── MinIO ──────────────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "flakes"
MINIO_SECRET_KEY = "Mw0722352336"
MINIO_BUCKET     = "mpesa-fraud-pipeline"


def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config                = Config(signature_version='s3v4'),
        region_name           = 'us-east-1',
    )


def ensure_bucket(s3_client, bucket: str):
    try:
        s3_client.head_bucket(Bucket=bucket)
        log.info(f"[MINIO SYNC] Bucket '{bucket}' exists.")
    except Exception:
        s3_client.create_bucket(Bucket=bucket)
        log.info(f"[MINIO SYNC] Created bucket '{bucket}'.")


def fetch_transactions(target_date):
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Direct query — no JOINs, reads fact_transactions only
    cur.execute("""
        SELECT
            transaction_id,
            customer_id,
            amount,
            txn_type,
            channel,
            is_fraud,
            fraud_score,
            fraud_reason,
            txn_count_1hr,
            txn_count_24hr,
            total_amount_1hr,
            txn_timestamp,
            processed_at
        FROM fact_transactions
        WHERE DATE(txn_timestamp) = %s
        ORDER BY txn_timestamp
    """, (target_date,))
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    # Fallback — last 24 hours
    if not rows:
        log.warning(f"[MINIO SYNC] No rows for {target_date}, trying last 24 hours...")
        cur.execute("""
            SELECT
                transaction_id,
                customer_id,
                amount,
                txn_type,
                channel,
                is_fraud,
                fraud_score,
                fraud_reason,
                txn_count_1hr,
                txn_count_24hr,
                total_amount_1hr,
                txn_timestamp,
                processed_at
            FROM fact_transactions
            WHERE txn_timestamp >= NOW() - INTERVAL '24 hours'
            ORDER BY txn_timestamp
        """)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()


def sync_to_s3(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).date()

    log.info(f"[MINIO SYNC] Syncing transactions for {target_date}...")

    df = fetch_transactions(target_date)

    if df.empty:
        log.warning("[MINIO SYNC] No transactions found. Make sure fraud_detector.py is running.")
        return

    # Use actual date from data for correct partitioning
    actual_date = target_date
    if 'full_date' in df.columns and not df['full_date'].isna().all():
        actual_date = pd.to_datetime(df['full_date'].iloc[0]).date()

    log.info(f"[MINIO SYNC] Found {len(df):,} rows. Uploading to MinIO...")

    s3 = get_minio_client()
    ensure_bucket(s3, MINIO_BUCKET)

    partition = (
        f"year={actual_date.year}/"
        f"month={actual_date.month:02d}/"
        f"day={actual_date.day:02d}"
    )

    # ── All transactions ───────────────────────────────────────────
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    raw_key = f"raw/transactions/{partition}/transactions_{actual_date}.parquet"
    s3.put_object(Bucket=MINIO_BUCKET, Key=raw_key, Body=buffer.getvalue())
    log.info(f"[MINIO SYNC] ✓ Raw     → s3://{MINIO_BUCKET}/{raw_key}")

    # ── Fraud-only slice ───────────────────────────────────────────
    fraud_df = df[df['is_fraud'] == True]
    if not fraud_df.empty:
        fraud_buffer = BytesIO()
        fraud_df.to_parquet(fraud_buffer, index=False, engine='pyarrow')
        fraud_buffer.seek(0)
        fraud_key = f"processed/fraud_only/{partition}/fraud_{actual_date}.parquet"
        s3.put_object(Bucket=MINIO_BUCKET, Key=fraud_key, Body=fraud_buffer.getvalue())
        log.info(f"[MINIO SYNC] ✓ Fraud   → s3://{MINIO_BUCKET}/{fraud_key}  ({len(fraud_df)} rows)")
    else:
        log.info("[MINIO SYNC] No fraud transactions in this batch.")

    log.info(f"[MINIO SYNC] Done. {len(df):,} total | {len(fraud_df) if not fraud_df.empty else 0} fraud rows.")


if __name__ == '__main__':
    import sys
    from datetime import date

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if len(sys.argv) > 1:
        arg = sys.argv[1]
        target = date.today() if arg == 'today' else date.fromisoformat(arg)
    else:
        target = date.today() - timedelta(days=1)

    sync_to_s3(target_date=target)