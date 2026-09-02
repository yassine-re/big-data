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

CREATE TABLE IF NOT EXISTS silver.fact_stay_diagnoses
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

CREATE TABLE IF NOT EXISTS silver.fact_monitoring
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
