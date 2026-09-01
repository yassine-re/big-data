from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid5


BATCH_NAMESPACE = UUID("da52de80-4ef0-41c6-a55e-7311243cb850")


@dataclass(frozen=True)
class LakeFile:
    domain: str
    bronze_table: str
    path: Path
    relative_path: str
    source_day: date


SOURCE_DEFINITIONS = (
    ("patients", "bronze.patients", "patients/*/patients.csv"),
    ("stays", "bronze.stays", "sejours/*/sejours.csv"),
    ("diagnoses", "bronze.stay_diagnoses", "diagnostics/*/diagnostics.json"),
    ("monitoring", "bronze.monitoring", "monitoring/*/monitoring.parquet"),
    ("services", "bronze.services", "referentiels/*/services.csv"),
    ("cim10", "bronze.cim10", "referentiels/*/cim10.csv"),
)


class ClickHouseClient:
    def __init__(self) -> None:
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
        self.endpoint = f"http://{host}:{port}/"
        self.headers = {
            "X-ClickHouse-User": required_environment("CLICKHOUSE_USER"),
            "X-ClickHouse-Key": required_environment("CLICKHOUSE_PASSWORD"),
        }

    def execute(self, query: str) -> str:
        request = Request(
            self.endpoint,
            data=query.strip().encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=300) as response:
                return response.read().decode("utf-8")
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP {error.code}: {body.strip()}") from error
        except URLError as error:
            raise RuntimeError(f"ClickHouse indisponible: {error.reason}") from error

    def scalar(self, query: str) -> str:
        return self.execute(query).strip().splitlines()[0]

    def insert_json(self, table: str, row: dict[str, object]) -> None:
        payload = (
            f"INSERT INTO {table} FORMAT JSONEachRow\n"
            + json.dumps(row, ensure_ascii=False)
            + "\n"
        )
        self.execute(payload)


def required_environment(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"La variable {name} est obligatoire")
    return value


def sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_lake_files(lake_root: Path) -> list[LakeFile]:
    if not lake_root.is_dir():
        raise FileNotFoundError(f"Lake introuvable: {lake_root}")
    files: list[LakeFile] = []
    for domain, bronze_table, pattern in SOURCE_DEFINITIONS:
        for path in lake_root.glob(pattern):
            if not path.is_file():
                continue
            try:
                source_day = date.fromisoformat(path.parent.name)
            except ValueError as error:
                raise ValueError(f"Répertoire de date invalide: {path.parent}") from error
            files.append(
                LakeFile(
                    domain=domain,
                    bronze_table=bronze_table,
                    path=path,
                    relative_path=path.relative_to(lake_root).as_posix(),
                    source_day=source_day,
                )
            )
    if not files:
        raise FileNotFoundError(f"Aucun fichier reconnu dans {lake_root}")
    return sorted(files, key=lambda item: item.relative_path)


def apply_schema(client: ClickHouseClient, sql_root: Path) -> None:
    for name in ("10_control.sql", "20_bronze.sql"):
        sql_file = sql_root / name
        if not sql_file.is_file():
            raise FileNotFoundError(f"Script SQL introuvable: {sql_file}")
        for statement in sql_file.read_text(encoding="utf-8").split(";"):
            if statement.strip():
                client.execute(statement)


def build_insert_query(file: LakeFile, file_checksum: str, batch_id: UUID) -> str:
    clickhouse_path = sql_string(f"lake/{file.relative_path}")
    source_file = sql_string(file.relative_path)
    source_day = sql_string(file.source_day.isoformat())
    checksum_value = sql_string(file_checksum)
    batch = sql_string(str(batch_id))
    metadata = (
        f"toDate({source_day}), {source_file}, toFixedString({checksum_value}, 64), "
        f"toUUID({batch}), now64(6, 'UTC')"
    )

    if file.domain == "patients":
        return f"""
            INSERT INTO bronze.patients
            SELECT
                toFixedString(patient_sk, 64), birth_year, sex, region_code, {metadata}
            FROM file(
                {clickhouse_path}, 'CSVWithNames',
                'patient_sk String, birth_year UInt16, sex String, region_code String'
            )
        """
    if file.domain == "stays":
        return f"""
            INSERT INTO bronze.stays
            SELECT
                toFixedString(stay_sk, 64), toFixedString(patient_sk, 64),
                service_code, admission_ts, discharge_ts, admission_mode, discharge_mode,
                {metadata}
            FROM file(
                {clickhouse_path}, 'CSVWithNames',
                'stay_sk String, patient_sk String, service_code String, admission_ts String, discharge_ts String, admission_mode String, discharge_mode String'
            )
        """
    if file.domain == "diagnoses":
        return f"""
            INSERT INTO bronze.stay_diagnoses
            SELECT
                toFixedString(stay_sk, 64), diagnosis.code_cim10, diagnosis.type, {metadata}
            FROM file(
                {clickhouse_path}, 'JSONEachRow',
                'stay_sk String, diagnostics Array(Tuple(code_cim10 String, type String))'
            )
            ARRAY JOIN diagnostics AS diagnosis
        """
    if file.domain == "monitoring":
        return f"""
            INSERT INTO bronze.monitoring
            SELECT
                toFixedString(stay_sk, 64), toDateTime64(ts, 6, 'UTC'),
                toInt32(heart_rate), toInt16(spo2), toFloat64(temp_c), {metadata}
            FROM file({clickhouse_path}, 'Parquet')
        """
    if file.domain == "services":
        return f"""
            INSERT INTO bronze.services
            SELECT service_code, service_label, {metadata}
            FROM file(
                {clickhouse_path}, 'CSVWithNames',
                'service_code String, service_label String'
            )
        """
    if file.domain == "cim10":
        return f"""
            INSERT INTO bronze.cim10
            SELECT code_cim10, libelle, {metadata}
            FROM file(
                {clickhouse_path}, 'CSVWithNames',
                'code_cim10 String, libelle String'
            )
        """
    raise ValueError(f"Domaine non pris en charge: {file.domain}")


def was_loaded(client: ClickHouseClient, batch_id: UUID) -> bool:
    loaded = client.scalar(
        "SELECT count() FROM control.v_ingested_files_current "
        f"WHERE batch_id = toUUID('{batch_id}') AND status = 'SUCCESS' "
        "FORMAT TabSeparated"
    )
    return int(loaded) > 0


def write_status(
    client: ClickHouseClient,
    file: LakeFile,
    batch_id: UUID,
    file_checksum: str,
    status: str,
    rows_loaded: int,
    started_at: str,
    finished_at: str | None,
    error_message: str | None,
) -> None:
    client.insert_json(
        "control.ingested_files",
        {
            "batch_id": str(batch_id),
            "domain": file.domain,
            "bronze_table": file.bronze_table,
            "source_file": file.relative_path,
            "source_day": file.source_day.isoformat(),
            "file_checksum": file_checksum,
            "status": status,
            "rows_loaded": rows_loaded,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "updated_at": utc_now(),
        },
    )


def load_file(client: ClickHouseClient, file: LakeFile) -> str:
    file_checksum = checksum(file.path)
    batch_id = uuid5(BATCH_NAMESPACE, f"{file.relative_path}:{file_checksum}")
    if was_loaded(client, batch_id):
        return f"SKIPPED {file.relative_path}"

    started_at = utc_now()
    write_status(
        client, file, batch_id, file_checksum, "RUNNING", 0, started_at, None, None
    )
    try:
        client.execute(build_insert_query(file, file_checksum, batch_id))
        rows_loaded = int(
            client.scalar(
                f"SELECT count() FROM {file.bronze_table} FINAL "
                f"WHERE batch_id = toUUID('{batch_id}') FORMAT TabSeparated"
            )
        )
        write_status(
            client,
            file,
            batch_id,
            file_checksum,
            "SUCCESS",
            rows_loaded,
            started_at,
            utc_now(),
            None,
        )
        return f"LOADED {file.relative_path} rows={rows_loaded}"
    except Exception as error:
        write_status(
            client,
            file,
            batch_id,
            file_checksum,
            "FAILED",
            0,
            started_at,
            utc_now(),
            str(error)[:4_000],
        )
        raise


def main() -> None:
    lake_root = Path(os.getenv("LAKE_ROOT", "/app/lake"))
    sql_root = Path(os.getenv("SQL_ROOT", "/app/sql"))
    client = ClickHouseClient()
    apply_schema(client, sql_root)

    files = discover_lake_files(lake_root)
    loaded = 0
    skipped = 0
    for file in files:
        result = load_file(client, file)
        print(result, flush=True)
        if result.startswith("LOADED"):
            loaded += 1
        else:
            skipped += 1
    print(f"DONE files={len(files)} loaded={loaded} skipped={skipped}", flush=True)


if __name__ == "__main__":
    main()
