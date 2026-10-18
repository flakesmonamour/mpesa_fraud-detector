# scripts/setup_snowflake.py — run once to bootstrap Snowflake
import snowflake.connector, logging
logging.basicConfig(level=logging.INFO)

conn = snowflake.connector.connect(
    account="EAZGUJO-KN15054", user="WABKENCMPTRS@GMAIL.COM",
    password="Mw42784695*1234", warehouse="SNOWFLAKE_LEARNING_WH",
    insecure_mode=True, login_timeout=60,
)
cur = conn.cursor()
cur.execute("USE WAREHOUSE SNOWFLAKE_LEARNING_WH")
cur.execute("CREATE DATABASE IF NOT EXISTS MPESA_FRAUD_DB")
cur.execute("USE DATABASE MPESA_FRAUD_DB")
cur.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
conn.commit()
cur.close(); conn.close()
print("✓ MPESA_FRAUD_DB ready.")