# scripts/retrain_model.py
# ─── Weekly Model Retraining ─────────────────────────────────────
# Runs every Monday via Airflow.
# Reads historical labelled transactions from PostgreSQL,
# retrains a simple fraud detection model,
# saves the new model for Spark to use.

import psycopg2
import psycopg2.extras
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from datetime import datetime

print("Starting model retraining...")

# ── Connect to PostgreSQL ─────────────────────────────────────────
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='mpesa_fraud',
    user='mark',
    password='fraud2025'
)

# ── Load training data using DictCursor (no SQLAlchemy needed) ────
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur.execute("""
    SELECT
        f.amount,
        f.txn_count_1hr,
        f.total_amount_1hr,
        CASE WHEN f.is_fraud THEN 1 ELSE 0 END AS label
    FROM fact_transactions f
    WHERE f.processed_at >= NOW() - INTERVAL '30 days'
""")
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
cur.close()
conn.close()

if not rows:
    print("No data found in the last 30 days. Make sure fraud_detector.py is running.")
    exit(1)

df = pd.DataFrame(rows, columns=cols)

print(f"Loaded {len(df):,} transactions for training")
print(f"Fraud rate: {df['label'].mean():.2%}")

# ── Features and labels ───────────────────────────────────────────
features = ["amount", "txn_count_1hr", "total_amount_1hr"]
X = df[features]
y = df['label']

# ── Train / Test split ────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ── Train model ───────────────────────────────────────────────────
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    class_weight='balanced'
)
model.fit(X_train, y_train)

# ── Evaluate ──────────────────────────────────────────────────────
y_pred = model.predict(X_test)
print("\nModel Performance:")
print(classification_report(y_test, y_pred, target_names=['Clean', 'Fraud']))

# ── Save model ────────────────────────────────────────────────────
model_path = f"/tmp/fraud_model_{datetime.now().strftime('%Y%m%d')}.pkl"
with open(model_path, 'wb') as f:
    pickle.dump(model, f)

print(f"\nModel saved to {model_path}")
print("Retraining complete.")