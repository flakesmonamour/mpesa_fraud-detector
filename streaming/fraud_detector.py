"""
fraud_detector.py — Spark Streaming (Step 2 of the pipeline)
=============================================================
Reads transactions from Kafka in real time.
Applies fraud rules to each transaction.
Saves scored transactions to PostgreSQL.
Sends high-score transactions to fraud_alerts table.
"""

import logging
from datetime import datetime
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, IntegerType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SPARK] %(message)s')
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC     = 'mpesa-transactions'

DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'mpesa_fraud',
    'user':     'mark',
    'password': 'fraud2025',
}

TXN_SCHEMA = StructType([
    StructField("transaction_id",  StringType(),  True),
    StructField("customer_id",     StringType(),  True),
    StructField("amount",          FloatType(),   True),
    StructField("txn_type",        StringType(),  True),
    StructField("receiver_id",     StringType(),  True),
    StructField("channel",         StringType(),  True),
    StructField("county",          StringType(),  True),
    StructField("device_id",       StringType(),  True),
    StructField("txn_timestamp",   StringType(),  True),
    StructField("txn_count_1hr",   IntegerType(), True),
    StructField("txn_count_24hr",  IntegerType(), True),
    StructField("txn_amount_1hr",  FloatType(),   True),
    StructField("fraud_type_hint", StringType(),  True),
])


def compute_fraud_score(amount, txn_count_1hr, total_amount_1hr,
                        device_id, county, customer_id):
    score, reasons = 0.0, []
    if amount and amount > 10000:
        score += 0.4; reasons.append("large_amount")
    if txn_count_1hr and txn_count_1hr > 10:
        score += 0.3; reasons.append("velocity")
    if total_amount_1hr and total_amount_1hr > 200000:
        score += 0.2; reasons.append("high_hourly_total")
    if device_id and 'NEW' in str(device_id).upper():
        score += 0.3; reasons.append("new_device")
    if county and county.lower() == "unknown":
        score += 0.05; reasons.append("unknown_location")
    if customer_id and customer_id == 'CUST004':
        score += 0.1; reasons.append("unverified_customer")
    return min(score, 1.0), (','.join(reasons) if reasons else 'clean')


# ── SELECT-first helpers — zero ON CONFLICT, zero constraint dependency ──────

def get_or_create_date_id(cursor, txn_dt):
    cursor.execute(
        "SELECT date_id FROM dim_date WHERE full_date = %s LIMIT 1",
        (txn_dt.date(),)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("""
        INSERT INTO dim_date (full_date, year, month, day, hour, day_name, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING date_id
    """, (txn_dt.date(), txn_dt.year, txn_dt.month, txn_dt.day,
          txn_dt.hour, txn_dt.strftime("%A"), txn_dt.weekday() >= 5))
    return cursor.fetchone()[0]


def get_or_create_location_id(cursor, county):
    county = county or 'Unknown'
    cursor.execute(
        "SELECT location_id FROM dim_location WHERE county = %s LIMIT 1",
        (county,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("""
        INSERT INTO dim_location (county, town, region, country)
        VALUES (%s, %s, 'Unknown', 'Kenya') RETURNING location_id
    """, (county, county))
    return cursor.fetchone()[0]


def get_or_create_device(cursor, device_id):
    cursor.execute(
        "SELECT device_id FROM dim_device WHERE device_id = %s LIMIT 1",
        (device_id,)
    )
    if cursor.fetchone():
        return
    cursor.execute("""
        INSERT INTO dim_device (device_id, device_type, is_new_device)
        VALUES (%s, 'mobile', %s)
    """, (device_id, 'NEW' in str(device_id).upper()))


def get_or_create_customer(cursor, customer_id, county):
    cursor.execute(
        "SELECT customer_id FROM dim_customer WHERE customer_id = %s LIMIT 1",
        (customer_id,)
    )
    if cursor.fetchone():
        return
    cursor.execute("""
        INSERT INTO dim_customer
            (customer_id, phone_number, account_tier, account_age_days, county, is_verified)
        VALUES (%s, '0700000000', 'basic', 30, %s, false)
    """, (customer_id, county or 'Unknown'))


def transaction_exists(cursor, transaction_id):
    cursor.execute(
        "SELECT 1 FROM fact_transactions WHERE transaction_id = %s LIMIT 1",
        (transaction_id,)
    )
    return cursor.fetchone() is not None


def alert_exists(cursor, transaction_id):
    cursor.execute(
        "SELECT 1 FROM fraud_alerts WHERE transaction_id = %s LIMIT 1",
        (transaction_id,)
    )
    return cursor.fetchone() is not None


# ── Main batch processor ─────────────────────────────────────────────────────

def save_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    conn = None
    try:
        conn   = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        saved  = 0
        alerts = 0

        for row in rows:
            try:
                score, reason = compute_fraud_score(
                    row['amount'], row['txn_count_1hr'], row['txn_amount_1hr'],
                    row['device_id'], row['county'], row['customer_id']
                )
                is_fraud = score >= 0.5
                txn_dt   = datetime.fromisoformat(row['txn_timestamp'])

                # dim_date
                date_id = get_or_create_date_id(cursor, txn_dt)

                # dim_location
                location_id = get_or_create_location_id(cursor, row['county'])

                # dim_device
                get_or_create_device(cursor, row['device_id'])

                # dim_customer
                get_or_create_customer(cursor, row['customer_id'], row['county'])

                # fact_transactions — skip if already exists
                if not transaction_exists(cursor, row['transaction_id']):
                    cursor.execute("""
                        INSERT INTO fact_transactions (
                            transaction_id, customer_id, date_id, location_id, device_id,
                            amount, txn_type, receiver_id, channel,
                            is_fraud, fraud_score, fraud_reason,
                            txn_count_1hr, txn_count_24hr, total_amount_1hr,
                            txn_timestamp, processed_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    """, (
                        row['transaction_id'], row['customer_id'], date_id, location_id,
                        row['device_id'], row['amount'], row['txn_type'],
                        row['receiver_id'], row['channel'],
                        is_fraud, score, reason,
                        row['txn_count_1hr'], row['txn_count_24hr'], row['txn_amount_1hr'],
                        row['txn_timestamp']
                    ))
                    saved += 1

                # fraud_alerts — skip if already exists
                if is_fraud and not alert_exists(cursor, row['transaction_id']):
                    cursor.execute("""
                        INSERT INTO fraud_alerts
                            (transaction_id, customer_id, amount,
                             fraud_score, fraud_reason, alert_status)
                        VALUES (%s, %s, %s, %s, %s, 'open')
                    """, (
                        row['transaction_id'], row['customer_id'],
                        row['amount'], score, reason
                    ))
                    alerts += 1
                    log.warning(
                        f"FRAUD ALERT | {row['customer_id']} | "
                        f"KES {row['amount']:,.2f} | Score {score:.2f} | {reason}"
                    )

            except Exception as row_err:
                # Log the bad row and continue — never let one row kill the batch
                log.error(f"Row error ({row['transaction_id']}): {row_err}")
                conn.rollback()
                # Re-open cursor after rollback
                cursor = conn.cursor()
                continue

        conn.commit()
        log.info(f"Batch {batch_id} | Saved: {saved} | Fraud alerts: {alerts}")

    except Exception as e:
        log.error(f"Batch {batch_id} fatal error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


def run_fraud_detector():
    spark = SparkSession.builder \
        .appName("MpesaFraudDetector") \
        .config('spark.sql.shuffle.partitions', '4') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Reading from Kafka topic: mpesa-transactions")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option('startingOffsets', 'latest') \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), TXN_SCHEMA).alias("d")) \
        .select("d.*")

    query = parsed.writeStream \
        .foreachBatch(save_to_postgres) \
        .option("checkpointLocation", "/tmp/sfraud_checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    log.info("Fraud detector running....")
    query.awaitTermination()


if __name__ == "__main__":
    run_fraud_detector()