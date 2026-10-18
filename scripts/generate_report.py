"""
generate_report.py — Airflow Task 3
=====================================
Generates daily fraud summary report and uploads it to MinIO.
"""

import boto3
import psycopg2
import psycopg2.extras
import json
from io import StringIO
from datetime import datetime, timedelta
from botocore.client import Config
import logging

log = logging.getLogger(__name__)

DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'mpesa_fraud',
    'user':     'mark',
    'password': 'fraud2025',
}

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


def run_query(cursor, sql, params):
    """Run query and return list of dicts."""
    cursor.execute(sql, params)
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def generate_report():
    yesterday = (datetime.now() - timedelta(days=1)).date()
    log.info(f"[REPORT] Generating fraud report for {yesterday}...")

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ── KPIs ────────────────────────────────────────────────────
    # Fix 1: ROUND(double precision, int) fails in PostgreSQL
    # Cast to numeric first: ROUND(value::numeric, decimals)
    cursor.execute("""
        SELECT
            COUNT(*)                                               AS total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)             AS fraud_count,
            ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END)::numeric * 100, 2)
                                                                   AS fraud_rate_pct,
            ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END)::numeric, 2)
                                                                   AS kes_at_risk,
            ROUND(AVG(fraud_score)::numeric, 4)                   AS avg_fraud_score
        FROM fact_transactions
        WHERE DATE(txn_timestamp) = %s
    """, (yesterday,))
    kpi_row = cursor.fetchone()
    kpis = dict(kpi_row) if kpi_row else {}

    # ── Top fraud reasons ────────────────────────────────────────
    reasons = run_query(cursor, """
        SELECT fraud_reason, COUNT(*) AS cnt
        FROM fact_transactions
        WHERE is_fraud = TRUE AND DATE(txn_timestamp) = %s
        GROUP BY fraud_reason ORDER BY cnt DESC LIMIT 10
    """, (yesterday,))

    # ── Fraud by county ──────────────────────────────────────────
    by_county = run_query(cursor, """
        SELECT l.county,
               COUNT(*)         AS fraud_count,
               SUM(f.amount)    AS kes_at_risk
        FROM fact_transactions f
        JOIN dim_location l ON f.location_id = l.location_id
        WHERE f.is_fraud = TRUE AND DATE(f.txn_timestamp) = %s
        GROUP BY l.county ORDER BY fraud_count DESC LIMIT 10
    """, (yesterday,))

    # ── Fraud by hour ────────────────────────────────────────────
    by_hour = run_query(cursor, """
        SELECT EXTRACT(HOUR FROM txn_timestamp)::INT AS hour,
               COUNT(*) AS fraud_count
        FROM fact_transactions
        WHERE is_fraud = TRUE AND DATE(txn_timestamp) = %s
        GROUP BY 1 ORDER BY 1
    """, (yesterday,))

    cursor.close()
    conn.close()

    # ── Build report dict ────────────────────────────────────────
    report = {
        "report_date":     str(yesterday),
        "generated_at":    datetime.now().isoformat(),
        "kpis":            kpis,
        "top_reasons":     reasons,
        "fraud_by_county": by_county,
        "fraud_by_hour":   by_hour,
    }

    log.info(f"[REPORT] KPIs: {kpis}")

    # ── Upload JSON to MinIO ─────────────────────────────────────
    s3 = get_minio_client()
    report_key = f"reports/{yesterday}/fraud_report_{yesterday}.json"
    s3.put_object(
        Bucket      = MINIO_BUCKET,
        Key         = report_key,
        Body        = json.dumps(report, indent=2, default=str).encode('utf-8'),
        ContentType = 'application/json',
    )
    log.info(f"[REPORT] Uploaded → s3://{MINIO_BUCKET}/{report_key}")

    # ── Also upload CSV summary ──────────────────────────────────
    csv_buffer = StringIO()
    csv_buffer.write(','.join(kpis.keys()) + '\n')
    csv_buffer.write(','.join(str(v) for v in kpis.values()) + '\n')
    s3.put_object(
        Bucket      = MINIO_BUCKET,
        Key         = f"reports/{yesterday}/fraud_kpis_{yesterday}.csv",
        Body        = csv_buffer.getvalue().encode('utf-8'),
        ContentType = 'text/csv',
    )
    log.info(f"[REPORT] Done for {yesterday}.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    generate_report()