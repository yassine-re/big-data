CREATE TABLE IF NOT EXISTS bronze.patients
(
    patient_sk FixedString(64),
    birth_year UInt16,
    sex LowCardinality(String),
    region_code String,
    source_day Date,
    source_file String,
    file_checksum FixedString(64),
    batch_id UUID,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (batch_id, patient_sk);

CREATE TABLE IF NOT EXISTS bronze.stays
(
    stay_sk FixedString(64),
    patient_sk FixedString(64),
    service_code String,
    admission_ts_raw String,
    discharge_ts_raw String,
    admission_mode_raw String,
    discharge_mode_raw String,
    source_day Date,
    source_file String,
    file_checksum FixedString(64),
    batch_id UUID,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (batch_id, stay_sk);

CREATE TABLE IF NOT EXISTS bronze.stay_diagnoses
(
    stay_sk FixedString(64),
    code_cim10_raw String,
    diagnosis_type_raw String,
    source_day Date,
    source_file String,
    file_checksum FixedString(64),
    batch_id UUID,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (batch_id, stay_sk, code_cim10_raw, diagnosis_type_raw);

CREATE TABLE IF NOT EXISTS bronze.monitoring
(
    stay_sk FixedString(64),
    ts DateTime64(6, 'UTC'),
    heart_rate Int32,
    spo2 Int16,
    temp_c Float64,
    source_day Date,
    source_file String,
    file_checksum FixedString(64),
    batch_id UUID,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (batch_id, stay_sk, ts);

CREATE TABLE IF NOT EXISTS bronze.services
(
    service_code String,
    service_label String,
    source_day Date,
    source_file String,
    file_checksum FixedString(64),
    batch_id UUID,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (batch_id, service_code);

CREATE TABLE IF NOT EXISTS bronze.cim10
(
    code_cim10 String,
    diagnosis_label String,
    source_day Date,
    source_file String,
    file_checksum FixedString(64),
    batch_id UUID,
    ingested_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (batch_id, code_cim10);
