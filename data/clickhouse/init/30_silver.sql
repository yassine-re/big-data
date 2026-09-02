CREATE TABLE IF NOT EXISTS silver.dim_patients
(
    run_id UUID,
    patient_sk FixedString(64),
    birth_year UInt16,
    sex LowCardinality(String),
    region_code String,
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, patient_sk);

CREATE TABLE IF NOT EXISTS silver.dim_services
(
    run_id UUID,
    service_code String,
    service_label String,
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, service_code);

CREATE TABLE IF NOT EXISTS silver.dim_cim10
(
    run_id UUID,
    code_cim10 String,
    diagnosis_label String,
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, code_cim10);

CREATE TABLE IF NOT EXISTS silver.fact_stays
(
    run_id UUID,
    stay_sk FixedString(64),
    patient_sk FixedString(64),
    service_code String,
    admission_ts DateTime64(6, 'UTC'),
    discharge_ts Nullable(DateTime64(6, 'UTC')),
    age_at_admission_approx UInt8,
    admission_mode LowCardinality(String),
    discharge_mode Nullable(String),
    stay_count UInt8,
    is_ongoing UInt8,
    length_of_stay_minutes Nullable(UInt32),
    is_readmission_30d UInt8,
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, stay_sk);

CREATE TABLE IF NOT EXISTS silver.fact_stay_diagnoses
(
    run_id UUID,
    stay_sk FixedString(64),
    patient_sk FixedString(64),
    code_cim10 String,
    diagnosis_type LowCardinality(String),
    diagnosis_count UInt8,
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, stay_sk, code_cim10, diagnosis_type);

CREATE TABLE IF NOT EXISTS silver.fact_monitoring
(
    run_id UUID,
    stay_sk FixedString(64),
    ts DateTime64(6, 'UTC'),
    heart_rate UInt16,
    spo2 UInt8,
    temp_c Decimal32(2),
    reading_count UInt8,
    heart_rate_alert UInt8,
    spo2_alert UInt8,
    temp_alert UInt8,
    is_alert UInt8,
    alert_rule_version LowCardinality(String),
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, stay_sk, ts);

CREATE TABLE IF NOT EXISTS silver.fact_quality_events
(
    run_id UUID,
    source_table LowCardinality(String),
    record_key String,
    rule_code LowCardinality(String),
    severity LowCardinality(String),
    error_message String,
    source_day Date,
    source_file String,
    source_batch_id UUID,
    detected_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (run_id, source_table, record_key, rule_code);

-- Compatibilité avec un volume ClickHouse créé avant l'ajout des enrichissements.
ALTER TABLE silver.fact_stays
    ADD COLUMN IF NOT EXISTS age_at_admission_approx UInt8 AFTER discharge_ts;

ALTER TABLE silver.fact_stay_diagnoses
    ADD COLUMN IF NOT EXISTS patient_sk FixedString(64) AFTER stay_sk;

ALTER TABLE silver.fact_stays
    ADD COLUMN IF NOT EXISTS stay_count UInt8 DEFAULT 1 AFTER discharge_mode;

ALTER TABLE silver.fact_stays
    ADD COLUMN IF NOT EXISTS is_readmission_30d UInt8 DEFAULT 0 AFTER length_of_stay_minutes;

ALTER TABLE silver.fact_stay_diagnoses
    ADD COLUMN IF NOT EXISTS diagnosis_count UInt8 DEFAULT 1 AFTER diagnosis_type;

ALTER TABLE silver.fact_monitoring
    ADD COLUMN IF NOT EXISTS reading_count UInt8 DEFAULT 1 AFTER temp_c;

ALTER TABLE silver.fact_monitoring
    ADD COLUMN IF NOT EXISTS heart_rate_alert UInt8 DEFAULT 0 AFTER reading_count;

ALTER TABLE silver.fact_monitoring
    ADD COLUMN IF NOT EXISTS spo2_alert UInt8 DEFAULT 0 AFTER heart_rate_alert;

ALTER TABLE silver.fact_monitoring
    ADD COLUMN IF NOT EXISTS temp_alert UInt8 DEFAULT 0 AFTER spo2_alert;

ALTER TABLE silver.fact_monitoring
    ADD COLUMN IF NOT EXISTS is_alert UInt8 DEFAULT 0 AFTER temp_alert;

ALTER TABLE silver.fact_monitoring
    ADD COLUMN IF NOT EXISTS alert_rule_version LowCardinality(String)
    DEFAULT 'monitoring-alert-v1' AFTER is_alert;
