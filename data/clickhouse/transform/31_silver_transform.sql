-- Ce fichier ne contient que des INSERT ... SELECT exécutés dans ClickHouse.
-- Le pilote Python remplace {{RUN_ID}} par l'identifiant déterministe de l'exécution.

INSERT INTO silver.service_versions
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

INSERT INTO silver.cim10_versions
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

INSERT INTO silver.patient_versions
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

INSERT INTO silver.quality_event_versions
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

INSERT INTO silver.stay_versions
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
    stay_sk,
    patient_sk,
    service_code,
    admission_ts,
    discharge_ts,
    admission_mode,
    CAST(nullIf(discharge_mode, ''), 'Nullable(String)'),
    toUInt8(discharge_ts IS NULL),
    if(
        discharge_ts IS NULL,
        CAST(NULL, 'Nullable(UInt32)'),
        toUInt32(dateDiff('minute', admission_ts, discharge_ts))
    ),
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE admission_ts IS NOT NULL
  AND (discharge_is_empty OR discharge_ts IS NOT NULL)
  AND (discharge_ts IS NULL OR discharge_ts >= admission_ts)
  AND admission_mode IN ('urgence', 'programme', 'mutation')
  AND (empty(discharge_mode) OR discharge_mode IN ('domicile', 'mutation', 'transfert', 'deces'))
  AND patient_sk IN
      (SELECT patient_sk FROM silver.patient_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  AND service_code IN
      (SELECT service_code FROM silver.service_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.quality_event_versions
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
    toString(stay_sk),
    multiIf(
        admission_ts IS NULL, 'INVALID_ADMISSION_TS',
        NOT discharge_is_empty AND discharge_ts IS NULL, 'INVALID_DISCHARGE_TS',
        discharge_ts IS NOT NULL AND discharge_ts < admission_ts, 'DISCHARGE_BEFORE_ADMISSION',
        admission_mode NOT IN ('urgence', 'programme', 'mutation'), 'INVALID_ADMISSION_MODE',
        NOT empty(discharge_mode) AND discharge_mode NOT IN ('domicile', 'mutation', 'transfert', 'deces'), 'INVALID_DISCHARGE_MODE',
        patient_sk NOT IN
            (SELECT patient_sk FROM silver.patient_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}')), 'UNKNOWN_PATIENT',
        'UNKNOWN_SERVICE'
    ),
    'ERROR',
    multiIf(
        admission_ts IS NULL, 'Date d’admission invalide',
        NOT discharge_is_empty AND discharge_ts IS NULL, 'Date de sortie invalide',
        discharge_ts IS NOT NULL AND discharge_ts < admission_ts, 'Sortie antérieure à l’admission',
        admission_mode NOT IN ('urgence', 'programme', 'mutation'), 'Mode d’admission inconnu',
        NOT empty(discharge_mode) AND discharge_mode NOT IN ('domicile', 'mutation', 'transfert', 'deces'), 'Mode de sortie inconnu',
        patient_sk NOT IN
            (SELECT patient_sk FROM silver.patient_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}')), 'Patient absent de Silver',
        'Service absent de Silver'
    ),
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE admission_ts IS NULL
   OR (NOT discharge_is_empty AND discharge_ts IS NULL)
   OR (discharge_ts IS NOT NULL AND discharge_ts < admission_ts)
   OR admission_mode NOT IN ('urgence', 'programme', 'mutation')
   OR (NOT empty(discharge_mode) AND discharge_mode NOT IN ('domicile', 'mutation', 'transfert', 'deces'))
   OR patient_sk NOT IN
      (SELECT patient_sk FROM silver.patient_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
   OR service_code NOT IN
      (SELECT service_code FROM silver.service_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.quality_event_versions
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
    toString(stay_sk),
    'MISSING_DISCHARGE_MODE',
    'WARNING',
    'Mode de sortie manquant pour un séjour terminé',
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE discharge_ts IS NOT NULL
  AND empty(discharge_mode)
  AND admission_ts IS NOT NULL
  AND discharge_ts >= admission_ts
  AND admission_mode IN ('urgence', 'programme', 'mutation')
  AND patient_sk IN
      (SELECT patient_sk FROM silver.patient_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  AND service_code IN
      (SELECT service_code FROM silver.service_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.stay_diagnosis_versions
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
    stay_sk,
    code_cim10,
    diagnosis_type,
    source_day,
    source_file,
    batch_id,
    now64(6, 'UTC')
FROM prepared
WHERE row_num = 1
  AND diagnosis_type IN ('principal', 'associe')
  AND stay_sk IN
      (SELECT stay_sk FROM silver.stay_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  AND code_cim10 IN
      (SELECT code_cim10 FROM silver.cim10_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.quality_event_versions
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
            (SELECT stay_sk FROM silver.stay_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}')), 'UNKNOWN_STAY',
        'UNKNOWN_CIM10'
    ),
    'ERROR',
    multiIf(
        diagnosis_type NOT IN ('principal', 'associe'), 'Type de diagnostic inconnu',
        stay_sk NOT IN
            (SELECT stay_sk FROM silver.stay_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}')), 'Séjour absent de Silver',
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
          (SELECT stay_sk FROM silver.stay_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
      OR code_cim10 NOT IN
          (SELECT code_cim10 FROM silver.cim10_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  );

INSERT INTO silver.monitoring_versions
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
      (SELECT stay_sk FROM silver.stay_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'));

INSERT INTO silver.quality_event_versions
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
          (SELECT stay_sk FROM silver.stay_versions FINAL WHERE run_id = toUUID('{{RUN_ID}}'))
  );
