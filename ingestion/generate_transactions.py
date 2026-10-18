"""
generate_transactions.py
========================
Simulates real M-Pesa transaction events.
In a real company this would be replaced by the actual
M-Pesa API or internal transaction database.

Run this to generate fake transactions for testing.
"""

import random
import json
import uuid
from datetime import datetime

# ─── SAMPLE DATA POOLS ──────────────────────────────────────────────
CUSTOMERS = ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005']
COUNTIES  = ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Eldoret', 'Unknown']
DEVICES   = ['DEV_A1', 'DEV_B2', 'DEV_C3', 'DEV_NEW_999']  # DEV_NEW_999 = suspicious new device
TXN_TYPES = ['send', 'receive', 'withdraw', 'paybill', 'buy_goods']
CHANNELS  = ['ussd', 'app', 'agent']


def generate_normal_transaction():
    """Generate a normal, clean transaction"""
    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id':    random.choice(CUSTOMERS[:3]),       # known customers
        'amount':         round(random.uniform(50, 5000), 2), # small amounts
        'txn_type':       random.choice(TXN_TYPES),
        'receiver_id':    f"CUST00{random.randint(1,5)}",
        'channel':        random.choice(CHANNELS),
        'county':         random.choice(COUNTIES[:4]),        # known counties
        'device_id':      random.choice(DEVICES[:3]),         # known devices
        'txn_timestamp':  datetime.now().isoformat(),
        'txn_count_1hr':  random.randint(1, 3),               # low activity
        'txn_count_24hr': random.randint(1, 10),
        'total_amount_1hr': round(random.uniform(100, 5000), 2)
    }


def generate_fraud_transaction(fraud_type='random'):
    """Generate a suspicious transaction"""
    fraud_types = ['large_amount', 'velocity', 'new_device', 'unknown_location', 'dormant']

    if fraud_type == 'random':
        fraud_type = random.choice(fraud_types)

    base = generate_normal_transaction()

    if fraud_type == 'large_amount':
        # Very large transaction — unusual for most customers
        base['amount'] = round(random.uniform(150000, 500000), 2)
        base['fraud_type_hint'] = 'large_amount'

    elif fraud_type == 'velocity':
        # Too many transactions in a short time
        base['txn_count_1hr']     = random.randint(15, 30)
        base['total_amount_1hr']  = round(random.uniform(50000, 200000), 2)
        base['amount']            = round(random.uniform(1000, 5000), 2)
        base['fraud_type_hint']   = 'velocity'

    elif fraud_type == 'new_device':
        # Transaction from a device never seen before
        base['device_id']       = 'DEV_NEW_999'
        base['amount']          = round(random.uniform(20000, 100000), 2)
        base['fraud_type_hint'] = 'new_device'

    elif fraud_type == 'unknown_location':
        # Transaction from an unknown or foreign location
        base['county']          = 'Unknown'
        base['amount']          = round(random.uniform(10000, 80000), 2)
        base['fraud_type_hint'] = 'unknown_location'

    elif fraud_type == 'dormant':
        # Use a customer account that has been inactive (simulated)
        base['customer_id']     = 'CUST004'  # new, unverified account
        base['amount']          = round(random.uniform(50000, 200000), 2)
        base['fraud_type_hint'] = 'dormant_account'

    return base


def generate_batch(total=100, fraud_rate=0.15):
    """
    Generate a batch of transactions.
    fraud_rate = 0.15 means 15% are fraudulent (realistic for testing)
    """
    transactions = []
    fraud_count  = int(total * fraud_rate)
    clean_count  = total - fraud_count

    for _ in range(clean_count):
        transactions.append(generate_normal_transaction())

    for _ in range(fraud_count):
        transactions.append(generate_fraud_transaction())

    # Shuffle so fraud is not always at the end
    random.shuffle(transactions)
    return transactions


if __name__ == '__main__':
    # Generate 20 sample transactions and print them
    batch = generate_batch(total=20, fraud_rate=0.20)
    for txn in batch:
        hint = txn.get('fraud_type_hint', 'clean')
        print(f"[{hint.upper():20s}] Customer: {txn['customer_id']} | "
              f"Amount: KES {txn['amount']:>12,.2f} | "
              f"Device: {txn['device_id']} | "
              f"County: {txn['county']}")
