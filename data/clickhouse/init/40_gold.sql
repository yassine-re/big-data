CREATE TABLE IF NOT EXISTS gold.fact_service_daily_activity
(
    run_id UUID,
    silver_run_id UUID,
    activity_day Date,
    service_code String,
    service_label String,
    admission_count UInt64,
    emergency_admission_count UInt64,
    completed_stay_count UInt64,
    completed_length_of_stay_minutes UInt64,
    average_length_of_stay_days Nullable(Float64),
    readmission_30d_count UInt64,
    readmission_30d_rate_pct Float64,
    generated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (run_id, activity_day, service_code);

CREATE TABLE IF NOT EXISTS gold.fact_monitoring_alerts_daily
(
    run_id UUID,
    silver_run_id UUID,
    monitoring_day Date,
    service_code String,
    service_label String,
    reading_count UInt64,
    alert_count UInt64,
    alert_rate_pct Float64,
    heart_rate_alert_count UInt64,
    spo2_alert_count UInt64,
    temperature_alert_count UInt64,
    alert_rule_version LowCardinality(String),
    generated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (run_id, monitoring_day, service_code, alert_rule_version);

CREATE TABLE IF NOT EXISTS gold.fact_pathology_prevalence
(
    run_id UUID,
    silver_run_id UUID,
    code_cim10 String,
    diagnosis_label String,
    cohort_patient_count Nullable(UInt64),
    diagnosed_stay_count Nullable(UInt64),
    diagnosis_count Nullable(UInt64),
    principal_diagnosis_count Nullable(UInt64),
    associated_diagnosis_count Nullable(UInt64),
    diagnosed_patient_share_pct Nullable(Float64),
    is_suppressed UInt8,
    generated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (run_id, code_cim10);

CREATE TABLE IF NOT EXISTS gold.fact_cohort_distribution
(
    run_id UUID,
    silver_run_id UUID,
    code_cim10 String,
    diagnosis_label String,
    age_group LowCardinality(String),
    sex LowCardinality(String),
    cohort_patient_count Nullable(UInt64),
    diagnosis_count Nullable(UInt64),
    is_suppressed UInt8,
    generated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (run_id, code_cim10, age_group, sex);

-- Compatibilité avec un volume créé avant l'ajout du seuil de confidentialité.
ALTER TABLE gold.fact_pathology_prevalence
MODIFY COLUMN cohort_patient_count Nullable(UInt64);

ALTER TABLE gold.fact_pathology_prevalence
MODIFY COLUMN diagnosed_stay_count Nullable(UInt64);

ALTER TABLE gold.fact_pathology_prevalence
MODIFY COLUMN diagnosis_count Nullable(UInt64);

ALTER TABLE gold.fact_pathology_prevalence
MODIFY COLUMN principal_diagnosis_count Nullable(UInt64);

ALTER TABLE gold.fact_pathology_prevalence
MODIFY COLUMN associated_diagnosis_count Nullable(UInt64);

ALTER TABLE gold.fact_pathology_prevalence
MODIFY COLUMN diagnosed_patient_share_pct Nullable(Float64);

ALTER TABLE gold.fact_pathology_prevalence
ADD COLUMN IF NOT EXISTS is_suppressed UInt8 DEFAULT 0
AFTER diagnosed_patient_share_pct;

ALTER TABLE gold.fact_cohort_distribution
MODIFY COLUMN cohort_patient_count Nullable(UInt64);

ALTER TABLE gold.fact_cohort_distribution
MODIFY COLUMN diagnosis_count Nullable(UInt64);

ALTER TABLE gold.fact_cohort_distribution
ADD COLUMN IF NOT EXISTS is_suppressed UInt8 DEFAULT 0
AFTER diagnosis_count;
