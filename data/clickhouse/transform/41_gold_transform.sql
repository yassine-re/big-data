-- Les agrégations sont exécutées intégralement dans ClickHouse.
-- Python remplace les identifiants des exécutions Gold et Silver.

INSERT INTO gold.fact_service_daily_activity
WITH
    toUUID('{{SILVER_RUN_ID}}') AS silver_run,
    aggregated AS
    (
        SELECT
            toDate(stay.admission_ts) AS activity_day,
            stay.service_code AS service_code,
            service.service_label,
            sum(stay.stay_count) AS admission_count,
            sumIf(stay.stay_count, stay.admission_mode = 'urgence') AS emergency_admission_count,
            countIf(stay.discharge_ts IS NOT NULL) AS completed_stay_count,
            sumIf(
                toUInt64(ifNull(stay.length_of_stay_minutes, 0)),
                stay.discharge_ts IS NOT NULL
            ) AS completed_length_of_stay_minutes,
            sum(stay.is_readmission_30d) AS readmission_30d_count
        FROM silver.fact_stays AS stay FINAL
        INNER JOIN
        (
            SELECT service_code, service_label
            FROM silver.dim_services FINAL
            WHERE run_id = silver_run
        ) AS service ON stay.service_code = service.service_code
        WHERE stay.run_id = silver_run
        GROUP BY activity_day, stay.service_code, service.service_label
    )
SELECT
    toUUID('{{RUN_ID}}'),
    silver_run,
    activity_day,
    service_code,
    service_label,
    admission_count,
    emergency_admission_count,
    completed_stay_count,
    completed_length_of_stay_minutes,
    if(
        completed_stay_count = 0,
        CAST(NULL, 'Nullable(Float64)'),
        round(completed_length_of_stay_minutes / completed_stay_count / 1440, 2)
    ),
    readmission_30d_count,
    round(100 * readmission_30d_count / admission_count, 2),
    now64(6, 'UTC')
FROM aggregated;

INSERT INTO gold.fact_monitoring_alerts_daily
WITH
    toUUID('{{SILVER_RUN_ID}}') AS silver_run,
    aggregated AS
    (
        SELECT
            toDate(monitoring.ts) AS monitoring_day,
            stay.service_code AS service_code,
            service.service_label,
            sum(monitoring.reading_count) AS reading_count,
            sum(monitoring.is_alert) AS alert_count,
            sum(monitoring.heart_rate_alert) AS heart_rate_alert_count,
            sum(monitoring.spo2_alert) AS spo2_alert_count,
            sum(monitoring.temp_alert) AS temperature_alert_count,
            monitoring.alert_rule_version
        FROM silver.fact_monitoring AS monitoring FINAL
        INNER JOIN
        (
            SELECT stay_sk, service_code
            FROM silver.fact_stays FINAL
            WHERE run_id = silver_run
        ) AS stay ON monitoring.stay_sk = stay.stay_sk
        INNER JOIN
        (
            SELECT service_code, service_label
            FROM silver.dim_services FINAL
            WHERE run_id = silver_run
        ) AS service ON stay.service_code = service.service_code
        WHERE monitoring.run_id = silver_run
        GROUP BY
            monitoring_day,
            stay.service_code,
            service.service_label,
            monitoring.alert_rule_version
    )
SELECT
    toUUID('{{RUN_ID}}'),
    silver_run,
    monitoring_day,
    service_code,
    service_label,
    reading_count,
    alert_count,
    round(100 * alert_count / reading_count, 2),
    heart_rate_alert_count,
    spo2_alert_count,
    temperature_alert_count,
    alert_rule_version,
    now64(6, 'UTC')
FROM aggregated;

INSERT INTO gold.fact_pathology_prevalence
WITH
    toUUID('{{SILVER_RUN_ID}}') AS silver_run,
    total AS
    (
        SELECT uniqExact(patient_sk) AS diagnosed_patient_count
        FROM silver.fact_stay_diagnoses FINAL
        WHERE run_id = silver_run
    ),
    aggregated AS
    (
        SELECT
            diagnosis.code_cim10 AS code_cim10,
            cim10.diagnosis_label AS diagnosis_label,
            uniqExact(diagnosis.patient_sk) AS cohort_patient_count,
            uniqExact(diagnosis.stay_sk) AS diagnosed_stay_count,
            sum(diagnosis.diagnosis_count) AS diagnosis_count,
            sumIf(diagnosis.diagnosis_count, diagnosis.diagnosis_type = 'principal')
                AS principal_diagnosis_count,
            sumIf(diagnosis.diagnosis_count, diagnosis.diagnosis_type = 'associe')
                AS associated_diagnosis_count
        FROM silver.fact_stay_diagnoses AS diagnosis FINAL
        INNER JOIN
        (
            SELECT code_cim10, diagnosis_label
            FROM silver.dim_cim10 FINAL
            WHERE run_id = silver_run
        ) AS cim10 ON diagnosis.code_cim10 = cim10.code_cim10
        WHERE diagnosis.run_id = silver_run
        GROUP BY diagnosis.code_cim10, cim10.diagnosis_label
    )
SELECT
    toUUID('{{RUN_ID}}'),
    silver_run,
    code_cim10,
    diagnosis_label,
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), cohort_patient_count),
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), diagnosed_stay_count),
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), diagnosis_count),
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), principal_diagnosis_count),
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), associated_diagnosis_count),
    if(
        cohort_patient_count < 5,
        CAST(NULL, 'Nullable(Float64)'),
        round(100 * cohort_patient_count / total.diagnosed_patient_count, 2)
    ),
    toUInt8(cohort_patient_count < 5),
    now64(6, 'UTC')
FROM aggregated
CROSS JOIN total;

INSERT INTO gold.fact_cohort_distribution
WITH
    toUUID('{{SILVER_RUN_ID}}') AS silver_run,
    enriched AS
    (
        SELECT
            diagnosis.code_cim10 AS code_cim10,
            cim10.diagnosis_label AS diagnosis_label,
            multiIf(
                diagnosis.age_at_diagnosis_approx < 18, '00-17',
                diagnosis.age_at_diagnosis_approx < 40, '18-39',
                diagnosis.age_at_diagnosis_approx < 65, '40-64',
                diagnosis.age_at_diagnosis_approx < 80, '65-79',
                '80+'
            ) AS age_group,
            patient.sex AS sex,
            diagnosis.patient_sk AS patient_sk,
            diagnosis.diagnosis_count AS diagnosis_count
        FROM silver.fact_stay_diagnoses AS diagnosis FINAL
        INNER JOIN
        (
            SELECT patient_sk, sex
            FROM silver.dim_patients FINAL
            WHERE run_id = silver_run
        ) AS patient ON diagnosis.patient_sk = patient.patient_sk
        INNER JOIN
        (
            SELECT code_cim10, diagnosis_label
            FROM silver.dim_cim10 FINAL
            WHERE run_id = silver_run
        ) AS cim10 ON diagnosis.code_cim10 = cim10.code_cim10
        WHERE diagnosis.run_id = silver_run
          AND diagnosis.age_at_diagnosis_approx IS NOT NULL
    ),
    aggregated AS
    (
        SELECT
            code_cim10,
            diagnosis_label,
            age_group,
            sex,
            uniqExact(patient_sk) AS cohort_patient_count,
            sum(diagnosis_count) AS diagnosis_count
        FROM enriched
        GROUP BY code_cim10, diagnosis_label, age_group, sex
    )
SELECT
    toUUID('{{RUN_ID}}'),
    silver_run,
    code_cim10,
    diagnosis_label,
    age_group,
    sex,
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), cohort_patient_count),
    if(cohort_patient_count < 5, CAST(NULL, 'Nullable(UInt64)'), diagnosis_count),
    toUInt8(cohort_patient_count < 5),
    now64(6, 'UTC')
FROM aggregated;
