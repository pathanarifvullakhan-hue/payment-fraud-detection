CREATE DATABASE IF NOT EXISTS frauddb;
USE frauddb;

CREATE TABLE IF NOT EXISTS fraud_events (
  event_id VARCHAR(30) PRIMARY KEY,
  transaction_id VARCHAR(30) NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  customer_id VARCHAR(20) NOT NULL,
  card_token VARCHAR(40) NOT NULL,
  masked_card VARCHAR(20) NOT NULL,
  amount DECIMAL(14,2) NOT NULL,
  currency CHAR(3) NOT NULL,
  merchant_id VARCHAR(20),
  merchant_country CHAR(2),
  geo_country CHAR(2),
  risk_score INT NOT NULL,
  risk_band VARCHAR(10) NOT NULL,
  decision VARCHAR(10) NOT NULL,
  alert_status VARCHAR(20),
  correlation_id VARCHAR(50) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_rule_hits (
  hit_seq INT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(30) NOT NULL,
  rule_code VARCHAR(30) NOT NULL,
  rule_name VARCHAR(120) NOT NULL,
  rule_weight INT NOT NULL,
  CONSTRAINT fk_hits_event FOREIGN KEY (event_id) REFERENCES fraud_events (event_id)
);

-- Reference data used by the scoring consumer (profile enrichment)
CREATE TABLE IF NOT EXISTS customer_risk_profile (
  customer_id VARCHAR(20) PRIMARY KEY,
  risk_profile VARCHAR(20) NOT NULL DEFAULT 'STANDARD',
  usual_hour_start INT DEFAULT 6,
  usual_hour_end INT DEFAULT 23
);

-- Dead-letter store: the "recorded reason" the brief demands for events that could not be processed
CREATE TABLE IF NOT EXISTS dead_letter_events (
  dlq_id INT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(30),
  correlation_id VARCHAR(50),
  source_queue VARCHAR(60),
  reason VARCHAR(1000) NOT NULL,
  transient_retries INT DEFAULT 0,
  original_payload TEXT,
  status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
  failed_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fe_alert_status ON fraud_events (alert_status, created_at);
CREATE INDEX idx_fe_band_created ON fraud_events (risk_band, created_at);

INSERT IGNORE INTO customer_risk_profile VALUES
 ('CUST-77219','STANDARD',6,23),
 ('CUST-10001','TRUSTED',5,23),
 ('CUST-10002','WATCHLIST',8,20);

-- Least-privilege application user
CREATE USER IF NOT EXISTS 'fraud_app'@'%' IDENTIFIED BY 'ChangeMe!123';
GRANT SELECT, INSERT, UPDATE, DELETE ON frauddb.* TO 'fraud_app'@'%';
