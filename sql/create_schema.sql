-- ============================================================
-- M-PESA FRAUD DETECTION PIPELINE — PostgreSQL Star Schema
-- Author: Mark | Zetech University
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id       VARCHAR(50)  PRIMARY KEY,
    phone_number      VARCHAR(15)  NOT NULL,
    account_tier      VARCHAR(20),
    account_age_days  INTEGER,
    county            VARCHAR(50),
    is_verified       BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL   PRIMARY KEY,
    full_date   DATE     NOT NULL,
    year        INTEGER,
    month       INTEGER,
    day         INTEGER,
    hour        INTEGER,
    day_name    VARCHAR(10),
    is_weekend  BOOLEAN,
    is_holiday  BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id        SERIAL      PRIMARY KEY,
    county             VARCHAR(50),
    town               VARCHAR(100),
    is_known_location  BOOLEAN DEFAULT TRUE,
    country            VARCHAR(50) DEFAULT 'Kenya'
);

CREATE TABLE IF NOT EXISTS dim_device (
    device_id      VARCHAR(100) PRIMARY KEY,
    device_type    VARCHAR(50),
    os             VARCHAR(50),
    is_new_device  BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id    VARCHAR(50)   PRIMARY KEY,
    customer_id       VARCHAR(50)   REFERENCES dim_customer(customer_id),
    date_id           INTEGER       REFERENCES dim_date(date_id),
    location_id       INTEGER       REFERENCES dim_location(location_id),
    device_id         VARCHAR(100)  REFERENCES dim_device(device_id),
    amount            DECIMAL(15,2) NOT NULL,
    txn_type          VARCHAR(30),
    receiver_id       VARCHAR(50),
    channel           VARCHAR(30),
    is_fraud          BOOLEAN       DEFAULT FALSE,
    fraud_score       FLOAT         DEFAULT 0.0,
    fraud_reason      VARCHAR(200),
    txn_count_1hr     INTEGER       DEFAULT 0,
    txn_count_24hr    INTEGER       DEFAULT 0,
    total_amount_1hr  DECIMAL(15,2) DEFAULT 0,
    txn_timestamp     TIMESTAMP     NOT NULL,
    processed_at      TIMESTAMP     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id        SERIAL        PRIMARY KEY,
    transaction_id  VARCHAR(50)   REFERENCES fact_transactions(transaction_id),
    customer_id     VARCHAR(50),
    amount          DECIMAL(15,2),
    fraud_reason    VARCHAR(200),
    fraud_score     FLOAT,
    alert_status    VARCHAR(20)   DEFAULT 'open',
    alerted_at      TIMESTAMP     DEFAULT NOW(),
    reviewed_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_txn_customer   ON fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_timestamp  ON fact_transactions(txn_timestamp);
CREATE INDEX IF NOT EXISTS idx_txn_is_fraud   ON fact_transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_alerts_status  ON fraud_alerts(alert_status);

INSERT INTO dim_location (county, town, is_known_location, country) VALUES
('Nairobi', 'Westlands',  TRUE,  'Kenya'),
('Nairobi', 'CBD',        TRUE,  'Kenya'),
('Mombasa', 'Nyali',      TRUE,  'Kenya'),
('Kisumu',  'Kisumu CBD', TRUE,  'Kenya'),
('Nakuru',  'Nakuru Town',TRUE,  'Kenya'),
('Unknown', 'Unknown',    FALSE, 'Unknown');

INSERT INTO dim_customer (customer_id, phone_number, account_tier, account_age_days, county, is_verified) VALUES
('CUST001', '0712345678', 'premium', 730,  'Nairobi', TRUE),
('CUST002', '0723456789', 'basic',   45,   'Mombasa', TRUE),
('CUST003', '0734567890', 'business',1200, 'Nairobi', TRUE),
('CUST004', '0745678901', 'basic',   10,   'Kisumu',  FALSE),
('CUST005', '0756789012', 'premium', 365,  'Nakuru',  TRUE);

SELECT 'Schema created successfully' AS status;
