"""
producer.py — Ingestion Script (Step 1 of the pipeline)
========================================================
1. Generates / fetches new M-Pesa transactions
2. Pushes each transaction as JSON into Kafka topic
3. Runs in a continuous loop

In a real company replace generate_batch() with
a call to the actual M-Pesa API or internal DB.
"""

import json
import time
import logging
from confluent_kafka import Producer, KafkaError
from generate_transactions import generate_batch

logging.basicConfig(level=logging.INFO, format='%(asctime)s [PRODUCER] %(message)s')
log = logging.getLogger(__name__)

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'mpesa-producer',
    'message.timeout.ms': 10000,
}

TOPIC = 'mpesa-transactions'


def delivery_report(err, msg):
    """
    Kafka calls this after each message — tells us if it was delivered.

    IMPORTANT: confluent_kafka always invokes this as callback(err, msg),
    error first. If the parameters here are ever swapped to (msg, err),
    `err` will silently hold the Message object instead — which is always
    truthy — so EVERY delivery gets logged as "FAILED" even when messages
    are actually landing fine. The isinstance check below turns that class
    of mistake into a loud, obvious warning instead of a silent false
    "FAILED" flood.
    """
    if err is not None and not isinstance(err, KafkaError):
        log.warning(
            f"delivery_report received an unexpected err type "
            f"({type(err).__name__}) — check that delivery_report(err, msg) "
            f"parameters haven't been swapped."
        )
        return
    if err:
        log.error(f"FAILED: {err}")
    else:
        log.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def run_producer(interval_seconds=5, batch_size=10):
    """
    Every interval_seconds:
      - Generate a batch of transactions
      - Push each one to Kafka
      - Wait and repeat
    """
    producer = Producer(KAFKA_CONFIG)
    log.info(f"Producer started. Topic: '{TOPIC}' | Interval: {interval_seconds}s")

    total_sent = 0
    total_fraud = 0

    try:
        while True:
            # Step 1 — get new transactions (15% fraud rate for testing)
            transactions = generate_batch(total=batch_size, fraud_rate=0.15)

            # Step 2 — push each one into Kafka
            for txn in transactions:
                message = json.dumps(txn).encode('utf-8')
                # Use customer_id as key so all txns from one customer
                # go to the same partition — needed for velocity checks
                key = txn['customer_id'].encode('utf-8')

                producer.produce(
                    topic=TOPIC,
                    key=key,
                    value=message,
                    callback=delivery_report
                )
                # Serve delivery callbacks without blocking (non-zero
                # timeout would stall the loop; poll(0) just drains
                # whatever's already ready).
                producer.poll(0)

                total_sent += 1
                if txn.get('fraud_type_hint'):
                    total_fraud += 1

            # Step 3 — flush ensures messages are actually sent
            producer.flush()

            log.info(
                f"Sent {batch_size} transactions | "
                f"Total: {total_sent} | "
                f"Fraud injected: {total_fraud}"
            )

            # Step 4 — wait before next batch
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        log.info(f"Producer stopped. Total sent: {total_sent}")
    finally:
        producer.flush()


if __name__ == '__main__':
    run_producer(interval_seconds=5, batch_size=10)