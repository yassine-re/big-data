CREATE TABLE IF NOT EXISTS control.ingested_files
(
    batch_id UUID,
    domain LowCardinality(String),
    bronze_table LowCardinality(String),
    source_file String,
    source_day Date,
    file_checksum FixedString(64),
    status LowCardinality(String),
    rows_loaded UInt64,
    started_at DateTime64(6, 'UTC'),
    finished_at Nullable(DateTime64(6, 'UTC')),
    error_message Nullable(String),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_file, file_checksum);

CREATE VIEW IF NOT EXISTS control.v_ingested_files_current AS
SELECT
    argMax(batch_id, updated_at) AS batch_id,
    argMax(domain, updated_at) AS domain,
    argMax(bronze_table, updated_at) AS bronze_table,
    source_file,
    argMax(source_day, updated_at) AS source_day,
    file_checksum,
    argMax(status, updated_at) AS status,
    argMax(rows_loaded, updated_at) AS rows_loaded,
    argMax(started_at, updated_at) AS started_at,
    argMax(tuple(finished_at), updated_at).1 AS finished_at,
    argMax(tuple(error_message), updated_at).1 AS error_message,
    max(updated_at) AS last_updated_at
FROM control.ingested_files
GROUP BY source_file, file_checksum;

CREATE TABLE IF NOT EXISTS control.silver_runs
(
    run_id UUID,
    transformation_version LowCardinality(String),
    input_fingerprint FixedString(64),
    status LowCardinality(String),
    rows_written UInt64,
    quality_events UInt64,
    started_at DateTime64(6, 'UTC'),
    finished_at Nullable(DateTime64(6, 'UTC')),
    error_message Nullable(String),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (transformation_version, input_fingerprint);

CREATE VIEW IF NOT EXISTS control.v_silver_runs_current AS
SELECT
    argMax(run_id, updated_at) AS run_id,
    transformation_version,
    input_fingerprint,
    argMax(status, updated_at) AS status,
    argMax(rows_written, updated_at) AS rows_written,
    argMax(quality_events, updated_at) AS quality_events,
    argMax(started_at, updated_at) AS started_at,
    argMax(tuple(finished_at), updated_at).1 AS finished_at,
    argMax(tuple(error_message), updated_at).1 AS error_message,
    max(updated_at) AS last_updated_at
FROM control.silver_runs
GROUP BY transformation_version, input_fingerprint;

CREATE VIEW IF NOT EXISTS control.v_latest_successful_silver_run AS
SELECT
    run_id,
    transformation_version,
    input_fingerprint,
    finished_at
FROM control.v_silver_runs_current
WHERE status = 'SUCCESS'
ORDER BY finished_at DESC
LIMIT 1;
