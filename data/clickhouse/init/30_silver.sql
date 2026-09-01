CREATE TABLE IF NOT EXISTS silver.patient_versions
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

CREATE TABLE IF NOT EXISTS silver.service_versions
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

CREATE TABLE IF NOT EXISTS silver.cim10_versions
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

CREATE TABLE IF NOT EXISTS silver.stay_versions
(
    run_id UUID,
    stay_sk FixedString(64),
    patient_sk FixedString(64),
    service_code String,
    admission_ts DateTime64(6, 'UTC'),
    discharge_ts Nullable(DateTime64(6, 'UTC')),
    admission_mode LowCardinality(String),
    discharge_mode Nullable(String),
    is_ongoing UInt8,
    length_of_stay_minutes Nullable(UInt32),
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, stay_sk);

CREATE TABLE IF NOT EXISTS silver.stay_diagnosis_versions
(
    run_id UUID,
    stay_sk FixedString(64),
    code_cim10 String,
    diagnosis_type LowCardinality(String),
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, stay_sk, code_cim10, diagnosis_type);

CREATE TABLE IF NOT EXISTS silver.monitoring_versions
(
    run_id UUID,
    stay_sk FixedString(64),
    ts DateTime64(6, 'UTC'),
    heart_rate UInt16,
    spo2 UInt8,
    temp_c Decimal32(2),
    source_day Date,
    source_file String,
    source_batch_id UUID,
    transformed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(transformed_at)
ORDER BY (run_id, stay_sk, ts);

CREATE TABLE IF NOT EXISTS silver.quality_event_versions
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

CREATE VIEW IF NOT EXISTS silver.patients AS
SELECT
    patient_sk,
    birth_year,
    sex,
    region_code,
    source_day,
    source_file,
    source_batch_id,
    transformed_at
FROM silver.patient_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);

CREATE VIEW IF NOT EXISTS silver.services AS
SELECT
    service_code,
    service_label,
    source_day,
    source_file,
    source_batch_id,
    transformed_at
FROM silver.service_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);

CREATE VIEW IF NOT EXISTS silver.cim10 AS
SELECT
    code_cim10,
    diagnosis_label,
    source_day,
    source_file,
    source_batch_id,
    transformed_at
FROM silver.cim10_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);

CREATE VIEW IF NOT EXISTS silver.stays AS
SELECT
    stay_sk,
    patient_sk,
    service_code,
    admission_ts,
    discharge_ts,
    admission_mode,
    discharge_mode,
    is_ongoing,
    length_of_stay_minutes,
    source_day,
    source_file,
    source_batch_id,
    transformed_at
FROM silver.stay_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);

CREATE VIEW IF NOT EXISTS silver.stay_diagnoses AS
SELECT
    stay_sk,
    code_cim10,
    diagnosis_type,
    source_day,
    source_file,
    source_batch_id,
    transformed_at
FROM silver.stay_diagnosis_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);

CREATE VIEW IF NOT EXISTS silver.monitoring AS
SELECT
    stay_sk,
    ts,
    heart_rate,
    spo2,
    temp_c,
    source_day,
    source_file,
    source_batch_id,
    transformed_at
FROM silver.monitoring_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);

CREATE VIEW IF NOT EXISTS silver.quality_events AS
SELECT
    source_table,
    record_key,
    rule_code,
    severity,
    error_message,
    source_day,
    source_file,
    source_batch_id,
    detected_at
FROM silver.quality_event_versions FINAL
WHERE run_id IN (SELECT run_id FROM control.v_latest_successful_silver_run);
