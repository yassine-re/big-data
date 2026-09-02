-- Ce fichier ne contient que des INSERT ... SELECT exécutés dans ClickHouse.
-- Le pilote Python remplace {{RUN_ID}} par l'identifiant déterministe de l'exécution.

INSERT INTO silver.dim_services
WITH latest AS
(
    SELECT
        trim(service_code) AS service_code,
        trim(service_label) AS service_label,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY trim(service_code)
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.services FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    service_code,
    service_label,
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM latest
WHERE row_num = 1
  AND notEmpty(service_code)
  AND notEmpty(service_label);

INSERT INTO silver.dim_cim10
WITH latest AS
(
    SELECT
        upperUTF8(trim(code_cim10)) AS code_cim10,
        trim(diagnosis_label) AS diagnosis_label,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY upperUTF8(trim(code_cim10))
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.cim10 FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    code_cim10,
    diagnosis_label,
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM latest
WHERE row_num = 1
  AND notEmpty(code_cim10)
  AND notEmpty(diagnosis_label);

INSERT INTO silver.dim_patients
WITH latest AS
(
    SELECT
        patient_sk,
        birth_year,
        upperUTF8(trim(sex)) AS sex,
        trim(region_code) AS region_code,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY patient_sk
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.patients FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    patient_sk,
    birth_year,
    sex,
    region_code,
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM latest
WHERE row_num = 1
  AND birth_year BETWEEN 1900 AND toYear(today())
  AND sex IN ('F', 'M')
  AND notEmpty(region_code);

INSERT INTO silver.fact_quality_events
WITH latest AS
(
    SELECT
        patient_sk,
        birth_year,
        upperUTF8(trim(sex)) AS sex,
        trim(region_code) AS region_code,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY patient_sk
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.patients FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    'bronze.patients',
    toString(patient_sk),
    multiIf(
        birth_year NOT BETWEEN 1900 AND toYear(today()), 'INVALID_BIRTH_YEAR',
        sex NOT IN ('F', 'M'), 'INVALID_SEX',
        'MISSING_REGION_CODE'
    ),
    'ERROR',
    multiIf(
        birth_year NOT BETWEEN 1900 AND toYear(today()), 'Année de naissance hors plage',
        sex NOT IN ('F', 'M'), 'Sexe non reconnu',
        'Code région manquant'
    ),
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM latest
WHERE row_num = 1
  AND
  (
      birth_year NOT BETWEEN 1900 AND toYear(today())
      OR sex NOT IN ('F', 'M')
      OR empty(region_code)
  );

INSERT INTO silver.fact_stays
WITH latest AS
(
    SELECT
        stay_sk,
        patient_sk,
        trim(service_code) AS service_code,
        admission_ts_raw,
        discharge_ts_raw,
        lowerUTF8(trim(admission_mode_raw)) AS admission_mode,
        lowerUTF8(trim(discharge_mode_raw)) AS discharge_mode,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.stays FINAL
),
prepared AS
(
    SELECT
        *,
        parseDateTime64BestEffortOrNull(trim(admission_ts_raw), 6, 'UTC') AS admission_ts,
        parseDateTime64BestEffortOrNull(trim(discharge_ts_raw), 6, 'UTC') AS discharge_ts,
        empty(trim(discharge_ts_raw)) AS discharge_is_empty
    FROM latest
    WHERE row_num = 1
),
valid_stays AS
(
    SELECT
        stay.stay_sk,
        stay.patient_sk,
        stay.service_code,
        stay.admission_ts,
        stay.discharge_ts,
        stay.admission_mode,
        CAST(nullIf(stay.discharge_mode, ''), 'Nullable(String)') AS discharge_mode,
        toUInt8(stay.discharge_ts IS NULL) AS is_ongoing,
        if(
            stay.discharge_ts IS NULL,
            CAST(NULL, 'Nullable(UInt32)'),
            toUInt32(dateDiff('minute', stay.admission_ts, stay.discharge_ts))
        ) AS length_of_stay_minutes,
        stay.source_day,
        stay.source_file,
        stay.batch_id
    FROM prepared AS stay
    INNER JOIN
    (
        SELECT patient_sk, birth_year
        FROM silver.dim_patients FINAL
        WHERE run_id = toUUID('{{RUN_ID}}')
    ) AS patient ON stay.patient_sk = patient.patient_sk
    WHERE stay.admission_ts IS NOT NULL
      AND (stay.discharge_is_empty OR stay.discharge_ts IS NOT NULL)
      AND (stay.discharge_ts IS NULL OR stay.discharge_ts >= stay.admission_ts)
      AND stay.admission_mode IN ('urgence', 'programme', 'mutation')
      AND (empty(stay.discharge_mode) OR stay.discharge_mode IN ('domicile', 'mutation', 'transfert', 'deces'))
      AND toYear(stay.admission_ts) BETWEEN patient.birth_year AND patient.birth_year + 120
      AND stay.service_code IN
          (SELECT service_code FROM silver.dim_services FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
),
ordered_stays AS
(
    SELECT
        *,
        lagInFrame(discharge_ts) OVER
        (
            PARTITION BY patient_sk
            ORDER BY admission_ts, stay_sk
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS previous_discharge_ts
    FROM valid_stays
)
SELECT
    toUUID('{{RUN_ID}}'),
    stay_sk,
    patient_sk,
    service_code,
    admission_ts,
    discharge_ts,
    admission_mode,
    discharge_mode,
    toUInt8(1),
    is_ongoing,
    length_of_stay_minutes,
    if(
        previous_discharge_ts IS NULL,
        toUInt8(0),
        toUInt8(dateDiff('day', previous_discharge_ts, admission_ts) BETWEEN 0 AND 30)
    ),
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM ordered_stays;

INSERT INTO silver.fact_quality_events
WITH latest AS
(
    SELECT
        stay_sk,
        patient_sk,
        trim(service_code) AS service_code,
        admission_ts_raw,
        discharge_ts_raw,
        lowerUTF8(trim(admission_mode_raw)) AS admission_mode,
        lowerUTF8(trim(discharge_mode_raw)) AS discharge_mode,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.stays FINAL
),
prepared AS
(
    SELECT
        *,
        parseDateTime64BestEffortOrNull(trim(admission_ts_raw), 6, 'UTC') AS admission_ts,
        parseDateTime64BestEffortOrNull(trim(discharge_ts_raw), 6, 'UTC') AS discharge_ts,
        empty(trim(discharge_ts_raw)) AS discharge_is_empty
    FROM latest
    WHERE row_num = 1
)
SELECT
    toUUID('{{RUN_ID}}'),
    'bronze.stays',
    toString(stay.stay_sk),
    multiIf(
        stay.admission_ts IS NULL, 'INVALID_ADMISSION_TS',
        NOT stay.discharge_is_empty AND stay.discharge_ts IS NULL, 'INVALID_DISCHARGE_TS',
        stay.discharge_ts IS NOT NULL AND stay.discharge_ts < stay.admission_ts, 'DISCHARGE_BEFORE_ADMISSION',
        stay.admission_mode NOT IN ('urgence', 'programme', 'mutation'), 'INVALID_ADMISSION_MODE',
        NOT empty(stay.discharge_mode) AND stay.discharge_mode NOT IN ('domicile', 'mutation', 'transfert', 'deces'), 'INVALID_DISCHARGE_MODE',
        empty(patient.patient_sk), 'UNKNOWN_PATIENT',
        toYear(stay.admission_ts) NOT BETWEEN patient.birth_year AND patient.birth_year + 120, 'INVALID_AGE_AT_ADMISSION',
        'UNKNOWN_SERVICE'
    ),
    'ERROR',
    multiIf(
        stay.admission_ts IS NULL, 'Date d’admission invalide',
        NOT stay.discharge_is_empty AND stay.discharge_ts IS NULL, 'Date de sortie invalide',
        stay.discharge_ts IS NOT NULL AND stay.discharge_ts < stay.admission_ts, 'Sortie antérieure à l’admission',
        stay.admission_mode NOT IN ('urgence', 'programme', 'mutation'), 'Mode d’admission inconnu',
        NOT empty(stay.discharge_mode) AND stay.discharge_mode NOT IN ('domicile', 'mutation', 'transfert', 'deces'), 'Mode de sortie inconnu',
        empty(patient.patient_sk), 'Patient absent de Silver',
        toYear(stay.admission_ts) NOT BETWEEN patient.birth_year AND patient.birth_year + 120, 'Âge à l’admission incohérent',
        'Service absent de Silver'
    ),
    stay.source_day,
    stay.source_file,
    stay.batch_id,
    now64(6, 'UTC')
FROM prepared AS stay
LEFT JOIN
(
    SELECT patient_sk, birth_year
    FROM silver.dim_patients FINAL
    WHERE run_id = toUUID('{{RUN_ID}}')
) AS patient ON stay.patient_sk = patient.patient_sk
WHERE stay.admission_ts IS NULL
   OR (NOT stay.discharge_is_empty AND stay.discharge_ts IS NULL)
   OR (stay.discharge_ts IS NOT NULL AND stay.discharge_ts < stay.admission_ts)
   OR stay.admission_mode NOT IN ('urgence', 'programme', 'mutation')
   OR (NOT empty(stay.discharge_mode) AND stay.discharge_mode NOT IN ('domicile', 'mutation', 'transfert', 'deces'))
   OR empty(patient.patient_sk)
   OR (
       stay.admission_ts IS NOT NULL
       AND NOT empty(patient.patient_sk)
       AND toYear(stay.admission_ts) NOT BETWEEN patient.birth_year AND patient.birth_year + 120
   )
   OR stay.service_code NOT IN
      (SELECT service_code FROM silver.dim_services FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.fact_quality_events
WITH latest AS
(
    SELECT
        stay_sk,
        patient_sk,
        trim(service_code) AS service_code,
        admission_ts_raw,
        discharge_ts_raw,
        lowerUTF8(trim(admission_mode_raw)) AS admission_mode,
        lowerUTF8(trim(discharge_mode_raw)) AS discharge_mode,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.stays FINAL
),
prepared AS
(
    SELECT
        *,
        parseDateTime64BestEffortOrNull(trim(admission_ts_raw), 6, 'UTC') AS admission_ts,
        parseDateTime64BestEffortOrNull(trim(discharge_ts_raw), 6, 'UTC') AS discharge_ts,
        empty(trim(discharge_ts_raw)) AS discharge_is_empty
    FROM latest
    WHERE row_num = 1
)
SELECT
    toUUID('{{RUN_ID}}'),
    'bronze.stays',
    toString(stay.stay_sk),
    'MISSING_DISCHARGE_MODE',
    'WARNING',
    'Mode de sortie manquant pour un séjour terminé',
    stay.source_day,
    stay.source_file,
    stay.batch_id,
    now64(6, 'UTC')
FROM prepared AS stay
INNER JOIN
(
    SELECT patient_sk, birth_year
    FROM silver.dim_patients FINAL
    WHERE run_id = toUUID('{{RUN_ID}}')
) AS patient ON stay.patient_sk = patient.patient_sk
WHERE stay.discharge_ts IS NOT NULL
  AND empty(stay.discharge_mode)
  AND stay.admission_ts IS NOT NULL
  AND stay.discharge_ts >= stay.admission_ts
  AND stay.admission_mode IN ('urgence', 'programme', 'mutation')
  AND toYear(stay.admission_ts) BETWEEN patient.birth_year AND patient.birth_year + 120
  AND stay.service_code IN
      (SELECT service_code FROM silver.dim_services FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.fact_stay_diagnoses
WITH prepared AS
(
    SELECT
        stay_sk,
        upperUTF8(trim(code_cim10_raw)) AS code_cim10,
        lowerUTF8(trim(diagnosis_type_raw)) AS diagnosis_type,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk, upperUTF8(trim(code_cim10_raw)), lowerUTF8(trim(diagnosis_type_raw))
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.stay_diagnoses FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    diagnosis.stay_sk,
    stay.patient_sk,
    diagnosis.code_cim10,
    diagnosis.diagnosis_type,
    toUInt8(toYear(stay.admission_ts) - patient.birth_year),
    toUInt8(1),
    diagnosis.source_day,
    diagnosis.source_file,
    diagnosis.batch_id,
    now64(6, 'UTC')
FROM prepared AS diagnosis
INNER JOIN
(
    SELECT stay_sk, patient_sk, admission_ts
    FROM silver.fact_stays FINAL
    WHERE run_id = toUUID('{{RUN_ID}}')
) AS stay ON diagnosis.stay_sk = stay.stay_sk
INNER JOIN
(
    SELECT patient_sk, birth_year
    FROM silver.dim_patients FINAL
    WHERE run_id = toUUID('{{RUN_ID}}')
) AS patient ON stay.patient_sk = patient.patient_sk
WHERE diagnosis.row_num = 1
  AND diagnosis.diagnosis_type IN ('principal', 'associe')
  AND diagnosis.code_cim10 IN
      (SELECT code_cim10 FROM silver.dim_cim10 FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.fact_quality_events
WITH prepared AS
(
    SELECT
        stay_sk,
        upperUTF8(trim(code_cim10_raw)) AS code_cim10,
        lowerUTF8(trim(diagnosis_type_raw)) AS diagnosis_type,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk, upperUTF8(trim(code_cim10_raw)), lowerUTF8(trim(diagnosis_type_raw))
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.stay_diagnoses FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    'bronze.stay_diagnoses',
    concat(toString(stay_sk), ':', code_cim10, ':', diagnosis_type),
    multiIf(
        diagnosis_type NOT IN ('principal', 'associe'), 'INVALID_DIAGNOSIS_TYPE',
        stay_sk NOT IN
            (SELECT stay_sk FROM silver.fact_stays FINAL WHERE run_id = toUUID('{{RUN_ID}}')), 'UNKNOWN_STAY',
        'UNKNOWN_CIM10'
    ),
    'ERROR',
    multiIf(
        diagnosis_type NOT IN ('principal', 'associe'), 'Type de diagnostic inconnu',
        stay_sk NOT IN
            (SELECT stay_sk FROM silver.fact_stays FINAL WHERE run_id = toUUID('{{RUN_ID}}')), 'Séjour absent de Silver',
        'Code CIM-10 absent du référentiel Silver'
    ),
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE row_num = 1
  AND
  (
      diagnosis_type NOT IN ('principal', 'associe')
      OR stay_sk NOT IN
          (SELECT stay_sk FROM silver.fact_stays FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
      OR code_cim10 NOT IN
          (SELECT code_cim10 FROM silver.dim_cim10 FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  );

-- Règles métier monitoring-alert-v1 :
-- SpO2 < 92 %, fréquence cardiaque < 50 ou > 100 bpm, température > 38,5 °C.
INSERT INTO silver.fact_monitoring
WITH prepared AS
(
    SELECT
        stay_sk,
        ts,
        heart_rate,
        spo2,
        temp_c,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk, ts
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.monitoring FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    stay_sk,
    ts,
    toUInt16(heart_rate),
    toUInt8(spo2),
    toDecimal32(temp_c, 2),
    toUInt8(1),
    toUInt8(heart_rate < 50 OR heart_rate > 100),
    toUInt8(spo2 < 92),
    toUInt8(temp_c > 38.5),
    toUInt8(heart_rate < 50 OR heart_rate > 100 OR spo2 < 92 OR temp_c > 38.5),
    'monitoring-alert-v1',
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE row_num = 1
  AND heart_rate BETWEEN 20 AND 250
  AND spo2 BETWEEN 50 AND 100
  AND isFinite(temp_c)
  AND temp_c BETWEEN 30 AND 45
  AND stay_sk IN
      (SELECT stay_sk FROM silver.fact_stays FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.fact_quality_events
WITH prepared AS
(
    SELECT
        stay_sk,
        ts,
        heart_rate,
        spo2,
        temp_c,
        source_day,
        source_file,
        batch_id,
        row_number() OVER
        (
            PARTITION BY stay_sk, ts
            ORDER BY source_day DESC, ingested_at DESC, batch_id DESC
        ) AS row_num
    FROM bronze.monitoring FINAL
)
SELECT
    toUUID('{{RUN_ID}}'),
    'bronze.monitoring',
    concat(toString(stay_sk), ':', toString(ts)),
    multiIf(
        heart_rate NOT BETWEEN 20 AND 250, 'INVALID_HEART_RATE',
        spo2 NOT BETWEEN 50 AND 100, 'INVALID_SPO2',
        NOT isFinite(temp_c) OR temp_c NOT BETWEEN 30 AND 45, 'INVALID_TEMPERATURE',
        'UNKNOWN_STAY'
    ),
    'ERROR',
    multiIf(
        heart_rate NOT BETWEEN 20 AND 250, 'Fréquence cardiaque hors plage technique',
        spo2 NOT BETWEEN 50 AND 100, 'SpO2 hors plage technique',
        NOT isFinite(temp_c) OR temp_c NOT BETWEEN 30 AND 45, 'Température hors plage technique',
        'Séjour absent de Silver'
    ),
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE row_num = 1
  AND
  (
      heart_rate NOT BETWEEN 20 AND 250
      OR spo2 NOT BETWEEN 50 AND 100
      OR NOT isFinite(temp_c)
      OR temp_c NOT BETWEEN 30 AND 45
      OR stay_sk NOT IN
          (SELECT stay_sk FROM silver.fact_stays FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  );
