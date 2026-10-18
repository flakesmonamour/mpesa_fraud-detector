# ============================================================
# scripts/sync_to_snowflake.py
# Pulls fraud data from PostgreSQL and loads into Snowflake
# Run by Airflow every night
# Author: Mark | Zetech University | DDA-01-0142/2025
# ============================================================

import psycopg2
import psycopg2.extras
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta, date
import logging
import sys

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── PostgreSQL ─────────────────────────────────────────────────────
PG_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'mpesa_fraud',
    'user':     'mark',
    'password': 'fraud2025',
}

# ── Snowflake ──────────────────────────────────────────────────────
SF_CONFIG = {
    "account":        "EAZGUJO-KN15054",
    "user":           "WABKENCMPTRS@GMAIL.COM",
    "password":       "Mw42784695*1234",
    "warehouse":      "SNOWFLAKE_LEARNING_WH",
    "database":       "MPESA_FRAUD_DB",
    "schema":         "PUBLIC",
    "insecure_mode":  True,
    "login_timeout":  60,
    "network_timeout":60,
}


def fetch_from_postgres(target_date):
    """
    Fetch transactions from PostgreSQL.
    FIX: county comes from dim_location via JOIN — it does not
         exist directly in fact_transactions.
    """
    conn = psycopg2.connect(**PG_CONFIG)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # county comes from dim_location JOIN — not directly in fact_transactions
    SQL = """
        SELECT
            f.transaction_id,
            f.customer_id,
            f.amount,
            f.txn_type,
            f.receiver_id,
            f.channel,
            l.county,
            f.device_id,
            f.is_fraud,
            f.fraud_score,
            f.fraud_reason,
            f.txn_count_1hr,
            f.txn_count_24hr,
            f.total_amount_1hr,
            f.txn_timestamp,
            f.processed_at
        FROM fact_transactions f
        LEFT JOIN dim_location l ON f.location_id = l.location_id
    """

    # Try exact date first
    cur.execute(
        SQL + " WHERE DATE(f.txn_timestamp) = %s ORDER BY f.txn_timestamp",
        (target_date,)
    )
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    # Fallback to last 24 hours if no rows found for that date
    if not rows:
        log.warning(f"No rows for {target_date}, falling back to last 24 hours...")
        cur.execute(
            SQL + " WHERE f.txn_timestamp >= NOW() - INTERVAL '24 hours' ORDER BY f.txn_timestamp"
        )
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=cols)


def connect_snowflake():
    """Connect and set warehouse, database, schema."""
    sf_conn = snowflake.connector.connect(**SF_CONFIG)
    cur     = sf_conn.cursor()
    cur.execute(f"USE WAREHOUSE {SF_CONFIG['warehouse']}")
    cur.execute(f"USE DATABASE {SF_CONFIG['database']}")
    cur.execute(f"USE SCHEMA {SF_CONFIG['schema']}")
    log.info(f"[SNOWFLAKE] Connected | DB: {SF_CONFIG['database']} | Schema: {SF_CONFIG['schema']}")
    return sf_conn, cur


def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_transactions_raw (
            transaction_id   VARCHAR(100),
            customer_id      VARCHAR(50),
            amount           FLOAT,
            txn_type         VARCHAR(30),
            receiver_id      VARCHAR(50),
            channel          VARCHAR(20),
            county           VARCHAR(50),
            device_id        VARCHAR(100),
            is_fraud         BOOLEAN,
            fraud_score      FLOAT,
            fraud_reason     VARCHAR(200),
            txn_count_1hr    INT,
            txn_count_24hr   INT,
            total_amount_1hr FLOAT,
            txn_timestamp    TIMESTAMP_NTZ,
            processed_at     TIMESTAMP_NTZ
        )
    """)
    log.info("[SNOWFLAKE] Table fact_transactions_raw ready.")


def insert_to_snowflake(cur, df):
    """Convert timestamps to strings and insert using executemany."""
    df_clean = df.copy()

    # Convert datetime columns to ISO strings
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif df_clean[col].dtype == object:
            df_clean[col] = df_clean[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if hasattr(x, 'strftime') else x
            )

    # Replace NaN with None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    cols         = list(df_clean.columns)
    placeholders = ', '.join(['%s'] * len(cols))
    col_list     = ', '.join(cols)
    sql          = f"INSERT INTO fact_transactions_raw ({col_list}) VALUES ({placeholders})"
    records      = [tuple(row) for _, row in df_clean.iterrows()]

    log.info(f"[SNOWFLAKE] Inserting {len(records):,} rows...")
    cur.executemany(sql, records)
    return len(records)


def sync_transactions(target_date=None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    log.info(f"[SYNC] Starting Snowflake sync for {target_date}...")

    # 1. Fetch from PostgreSQL
    df = fetch_from_postgres(target_date)
    if df.empty:
        log.warning("[SYNC] No transactions found. Make sure fraud_detector.py is running.")
        return

    log.info(f"[SYNC] Pulled {len(df):,} transactions from PostgreSQL.")

    # 2. Connect to Snowflake
    try:
        sf_conn, cur = connect_snowflake()
    except Exception as e:
        log.error(f"[SNOWFLAKE] Connection failed: {e}")
        return

    try:
        ensure_table(cur)
        inserted = insert_to_snowflake(cur, df)
        sf_conn.commit()
        log.info(f"[SYNC] ✓ Done. {inserted:,} rows synced to Snowflake.")

    except Exception as e:
        log.error(f"[SNOWFLAKE] Error: {e}")
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

    sync_transactions(target_date=target)