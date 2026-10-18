"""
load_snowflake.py — Airflow Task 2
=====================================
Loads Parquet file from MinIO into Snowflake.
Reads the file that sync_to_s3.py already wrote to MinIO.

Run:
    python load_snowflake.py          # loads yesterday
    python load_snowflake.py today    # loads today
    python load_snowflake.py 2026-08-23  # loads specific date
"""

import boto3
import snowflake.connector
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta, date
from botocore.client import Config
import logging
import sys

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── MinIO ──────────────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "flakes"
MINIO_SECRET_KEY = "Mw0722352336"
MINIO_BUCKET     = "mpesa-fraud-pipeline"

# ── Snowflake ──────────────────────────────────────────────────────
# NOTE: Do NOT use USER$wabkencmptrs — personal databases are read-only.
# MPESA_FRAUD_DB is a real database created by setup_snowflake.py
SF_CONFIG = {
    "account":       "EAZGUJO-KN15054",
    "user":          "WABKENCMPTRS@GMAIL.COM",
    "password":      "Mw42784695*1234",
    "warehouse":     "SNOWFLAKE_LEARNING_WH",
    "database":      "MPESA_FRAUD_DB",
    "schema":        "PUBLIC",
    "insecure_mode": True,
    "login_timeout": 60,
    "network_timeout": 60,
}


# ── MinIO helpers ──────────────────────────────────────────────────

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config                = Config(signature_version='s3v4'),
        region_name           = 'us-east-1',
    )


def read_from_minio(target_date):
    """
    Try target_date first, then today, then yesterday.
    Returns a DataFrame or empty DataFrame if nothing found.
    """
    s3 = get_minio_client()
    dates_to_try = list(dict.fromkeys([
        target_date,
        date.today(),
        date.today() - timedelta(days=1),
    ]))

    for d in dates_to_try:
        key = (
            f"raw/transactions/"
            f"year={d.year}/"
            f"month={d.month:02d}/"
            f"day={d.day:02d}/"
            f"transactions_{d}.parquet"
        )
        try:
            obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
            df  = pd.read_parquet(BytesIO(obj['Body'].read()))
            log.info(f"[SNOWFLAKE] Read {len(df):,} rows from MinIO → {key}")
            return df
        except Exception:
            log.warning(f"[SNOWFLAKE] No file at {key}, trying next...")

    return pd.DataFrame()


# ── Snowflake helpers ──────────────────────────────────────────────

def get_snowflake_connection():
    """Connect and explicitly set warehouse, database, schema."""
    sf_conn = snowflake.connector.connect(**SF_CONFIG)
    cur     = sf_conn.cursor()
    cur.execute(f"USE WAREHOUSE {SF_CONFIG['warehouse']}")
    cur.execute(f"USE DATABASE {SF_CONFIG['database']}")
    cur.execute(f"USE SCHEMA {SF_CONFIG['schema']}")
    log.info(
        f"[SNOWFLAKE] Connected | "
        f"WH: {SF_CONFIG['warehouse']} | "
        f"DB: {SF_CONFIG['database']} | "
        f"Schema: {SF_CONFIG['schema']}"
    )
    return sf_conn, cur


def ensure_table(cur):
    """Create mpesa_fact_transactions if it does not exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mpesa_fact_transactions (
            transaction_id   VARCHAR(100),
            customer_id      VARCHAR(50),
            phone_number     VARCHAR(20),
            account_tier     VARCHAR(20),
            amount           FLOAT,
            txn_type         VARCHAR(30),
            channel          VARCHAR(20),
            county           VARCHAR(50),
            town             VARCHAR(50),
            full_date        DATE,
            hour             INT,
            day_name         VARCHAR(12),
            is_fraud         BOOLEAN,
            fraud_score      FLOAT,
            fraud_reason     VARCHAR(200),
            txn_count_1hr    INT,
            total_amount_1hr FLOAT,
            txn_timestamp    TIMESTAMP_NTZ,
            processed_at     TIMESTAMP_NTZ
        )
    """)
    log.info("[SNOWFLAKE] Table mpesa_fact_transactions ready.")


def insert_records(cur, df):
    """Insert DataFrame rows into Snowflake using executemany."""
    cols = [
        'transaction_id', 'customer_id', 'phone_number', 'account_tier',
        'amount', 'txn_type', 'channel', 'county', 'town',
        'full_date', 'hour', 'day_name', 'is_fraud', 'fraud_score',
        'fraud_reason', 'txn_count_1hr', 'total_amount_1hr',
        'txn_timestamp', 'processed_at',
    ]
    available = [c for c in cols if c in df.columns]
    df_clean  = df[available].copy()

    # ── Convert all timestamp/date columns to strings ──────────────
    # Snowflake pyformat binding does not accept pandas Timestamp or
    # Python datetime objects — must be ISO format strings.
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif str(df_clean[col].dtype) == 'object':
            # catch any Timestamp objects hiding as object dtype
            df_clean[col] = df_clean[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                if hasattr(x, 'strftime') else x
            )

    # Convert date columns (full_date) to string
    for col in ['full_date']:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col]).dt.strftime('%Y-%m-%d')

    # Replace NaN/NaT with None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    records      = [tuple(row) for _, row in df_clean.iterrows()]
    placeholders = ', '.join(['%s'] * len(available))
    col_list     = ', '.join(available)
    sql          = f"INSERT INTO mpesa_fact_transactions ({col_list}) VALUES ({placeholders})"

    log.info(f"[SNOWFLAKE] Inserting {len(records):,} rows...")
    cur.executemany(sql, records)
    return len(records)
# ── Main ───────────────────────────────────────────────────────────

def load_snowflake(target_date=None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    log.info(f"[SNOWFLAKE] Starting load for {target_date}...")

    # 1. Read Parquet from MinIO
    df = read_from_minio(target_date)
    if df.empty:
        log.error(
            "[SNOWFLAKE] No data found in MinIO. "
            "Run sync_to_s3.py first: python sync_to_s3.py today"
        )
        return

    # 2. Connect to Snowflake
    try:
        sf_conn, cur = get_snowflake_connection()
    except Exception as e:
        log.error(f"[SNOWFLAKE] Connection failed: {e}")
        return

    try:
        # 3. Ensure table exists
        ensure_table(cur)

        # 4. Insert data
        inserted = insert_records(cur, df)

        # 5. Commit
        sf_conn.commit()
        log.info(f"[SNOWFLAKE] ✓ Done. {inserted:,} rows loaded into MPESA_FRAUD_DB.PUBLIC.mpesa_fact_transactions")

    except Exception as e:
        log.error(f"[SNOWFLAKE] Error during load: {e}")
        sf_conn.rollback()

    finally:
        cur.close()
        sf_conn.close()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        arg = sys.argv[1].strip()
        target = date.today() if arg == 'today' else date.fromisoformat(arg)
    else:
        target = date.today() - timedelta(days=1)

    load_snowflake(target_date=target)