-- ============================================================
-- M-PESA FRAUD DETECTION — Analyst SQL Queries
-- Run these in PostgreSQL or Snowflake
-- ============================================================

-- Query 1: Daily fraud summary
SELECT
    d.full_date,
    COUNT(*)                                            AS total_transactions,
    SUM(f.amount)                                       AS total_amount_kes,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)        AS fraud_count,
    SUM(CASE WHEN f.is_fraud THEN f.amount ELSE 0 END) AS fraud_amount_kes,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date DESC;

-- Query 2: Top fraud reasons
SELECT fraud_reason, COUNT(*) AS alert_count, SUM(amount) AS total_kes_flagged, AVG(fraud_score) AS avg_score
FROM fact_transactions
WHERE is_fraud = TRUE
GROUP BY fraud_reason
ORDER BY alert_count DESC;

-- Query 3: Fraud by county
SELECT
    l.county,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)        AS fraud_count,
    SUM(CASE WHEN f.is_fraud THEN f.amount ELSE 0 END) AS fraud_amount_kes,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.county
ORDER BY fraud_count DESC;

-- Query 4: Fraud by hour of day
SELECT
    d.hour,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.hour
ORDER BY d.hour;

-- Query 5: Top customers with fraud alerts
SELECT
    f.customer_id, c.phone_number, c.account_tier, c.county,
    COUNT(*) AS fraud_alerts,
    SUM(f.amount) AS total_flagged_kes,
    MAX(f.fraud_score) AS max_fraud_score
FROM fact_transactions f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.is_fraud = TRUE
GROUP BY f.customer_id, c.phone_number, c.account_tier, c.county
ORDER BY fraud_alerts DESC LIMIT 20;

-- Query 6: Fraud by transaction type
SELECT txn_type, COUNT(*) AS total_txns,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(amount),2) AS avg_amount_kes,
    ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fraud_rate_pct
FROM fact_transactions
GROUP BY txn_type ORDER BY fraud_count DESC;

-- Query 7: Open fraud alerts for ops team review
SELECT a.alert_id, a.transaction_id, a.customer_id, a.amount,
    a.fraud_reason, a.fraud_score, a.alert_status, a.alerted_at,
    EXTRACT(EPOCH FROM (NOW()-a.alerted_at))/3600 AS hours_open
FROM fraud_alerts a
WHERE a.alert_status = 'open'
ORDER BY a.fraud_score DESC, a.alerted_at ASC;

-- Query 8: Fraud trend last 30 days
SELECT d.full_date, COUNT(*) AS total_txns,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN f.is_fraud THEN f.amount ELSE 0 END) AS fraud_kes,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date ORDER BY d.full_date;

-- Query 9: Near-miss transactions (scored 0.3-0.49, just below fraud threshold)
SELECT transaction_id, customer_id, amount, fraud_score, fraud_reason, txn_timestamp
FROM fact_transactions
WHERE is_fraud = FALSE AND fraud_score BETWEEN 0.3 AND 0.49
ORDER BY fraud_score DESC LIMIT 50;

-- Query 10: All new device transactions
SELECT f.transaction_id, f.customer_id, f.amount, f.is_fraud, f.fraud_score,
    d2.device_id, d2.is_new_device, f.txn_timestamp
FROM fact_transactions f
JOIN dim_device d2 ON f.device_id = d2.device_id
WHERE d2.is_new_device = TRUE
ORDER BY f.txn_timestamp DESC LIMIT 100;
