from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# your actual scripts path on your ThinkPad
SCRIPTS = '/home/flakesmonamour/Documents/mpesa_fraud_pipeline/scripts'

default_args = {
    'owner':            'mark',
    'depends_on_past':  False,
    'email_on_failure': False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id       = 'mpesa_fraud_pipeline',
    description  = 'Nightly batch jobs for M-Pesa fraud detection pipeline',
    default_args = default_args,
    schedule     = '0 1 * * *',
    start_date   = datetime(2025, 1, 1),
    catchup      = False,
    tags         = ['fraud', 'mpesa', 'fintech']
) as dag:

    sync_to_s3 = BashOperator(
        task_id      = 'sync_to_s3',
        bash_command = f'python {SCRIPTS}/sync_to_s3.py',
    )

    load_snowflake = BashOperator(
        task_id      = 'load_snowflake',
        bash_command = f'python {SCRIPTS}/sync_to_snowflake.py',
    )

    fraud_report = BashOperator(
        task_id      = 'generate_fraud_report',
        bash_command = f'python {SCRIPTS}/generate_report.py',
    )

    cleanup_alerts = BashOperator(
        task_id      = 'cleanup_old_alerts',
        bash_command = """python -c "
import psycopg2
conn = psycopg2.connect(host='localhost',dbname='mpesa_fraud',user='mark',password='fraud2025')
cur  = conn.cursor()
cur.execute(\\\"\\\"\\\"
    UPDATE fraud_alerts
    SET alert_status='auto_closed', reviewed_at=NOW()
    WHERE alert_status='open'
    AND   alerted_at < NOW() - INTERVAL '7 days'
\\\"\\\"\\\")
print(f'Auto-closed {cur.rowcount} old alerts')
conn.commit(); cur.close(); conn.close()
"
        """
    )

    sync_to_s3 >> load_snowflake >> fraud_report >> cleanup_alerts