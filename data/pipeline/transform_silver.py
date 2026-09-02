from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid5


RUN_NAMESPACE = UUID("15f93989-c6a1-47bd-a876-30a9fbebdd0c")
TRANSFORMATION_VERSION = "silver-v2"


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
        result = self.execute(query).strip().splitlines()
        if not result:
            raise RuntimeError("ClickHouse n'a retourné aucune valeur")
        return result[0]

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


def execute_sql_file(
    client: ClickHouseClient,
    path: Path,
    replacements: dict[str, str] | None = None,
) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Script SQL introuvable: {path}")
    sql = path.read_text(encoding="utf-8")
    for placeholder, value in (replacements or {}).items():
        sql = sql.replace(placeholder, value)
    statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
    for position, statement in enumerate(statements, start=1):
        print(f"SQL {path.name} {position}/{len(statements)}", flush=True)
        client.execute(statement)


def apply_schema(client: ClickHouseClient, sql_root: Path) -> None:
    for name in ("10_control.sql", "30_silver.sql"):
        execute_sql_file(client, sql_root / "init" / name)


def bronze_fingerprint(client: ClickHouseClient) -> str:
    loaded_files = int(
        client.scalar(
            "SELECT count() FROM control.v_ingested_files_current "
            "WHERE status = 'SUCCESS' FORMAT TabSeparated"
        )
    )
    if loaded_files == 0:
        raise RuntimeError("Aucun fichier Bronze au statut SUCCESS")
    return client.scalar(
        "SELECT lower(hex(SHA256(arrayStringConcat(arraySort(groupArray("
        "concat(toString(batch_id), ':', toString(file_checksum)))), '|')))) "
        "FROM control.v_ingested_files_current WHERE status = 'SUCCESS' "
        "FORMAT TabSeparated"
    )


def was_published(
    client: ClickHouseClient,
    transformation_version: str,
    input_fingerprint: str,
) -> bool:
    count = client.scalar(
        "SELECT count() FROM control.v_silver_runs_current "
        f"WHERE transformation_version = {sql_string(transformation_version)} "
        f"AND input_fingerprint = toFixedString({sql_string(input_fingerprint)}, 64) "
        "AND status = 'SUCCESS' FORMAT TabSeparated"
    )
    return int(count) > 0


def write_status(
    client: ClickHouseClient,
    run_id: UUID,
    input_fingerprint: str,
    status: str,
    rows_written: int,
    quality_events: int,
    started_at: str,
    finished_at: str | None,
    error_message: str | None,
) -> None:
    client.insert_json(
        "control.silver_runs",
        {
            "run_id": str(run_id),
            "transformation_version": TRANSFORMATION_VERSION,
            "input_fingerprint": input_fingerprint,
            "status": status,
            "rows_written": rows_written,
            "quality_events": quality_events,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "updated_at": utc_now(),
        },
    )


def count_rows(client: ClickHouseClient, run_id: UUID) -> int:
    run = sql_string(str(run_id))
    return int(
        client.scalar(
            "SELECT sum(rows) FROM ("
            f"SELECT count() AS rows FROM silver.dim_patients FINAL WHERE run_id = toUUID({run}) "
            "UNION ALL "
            f"SELECT count() AS rows FROM silver.dim_services FINAL WHERE run_id = toUUID({run}) "
            "UNION ALL "
            f"SELECT count() AS rows FROM silver.dim_cim10 FINAL WHERE run_id = toUUID({run}) "
            "UNION ALL "
            f"SELECT count() AS rows FROM silver.fact_stays FINAL WHERE run_id = toUUID({run}) "
            "UNION ALL "
            f"SELECT count() AS rows FROM silver.fact_stay_diagnoses FINAL WHERE run_id = toUUID({run}) "
            "UNION ALL "
            f"SELECT count() AS rows FROM silver.fact_monitoring FINAL WHERE run_id = toUUID({run})"
            ") FORMAT TabSeparated"
        )
    )


def count_quality_events(client: ClickHouseClient, run_id: UUID) -> int:
    return int(
        client.scalar(
            "SELECT count() FROM silver.fact_quality_events FINAL "
            f"WHERE run_id = toUUID({sql_string(str(run_id))}) FORMAT TabSeparated"
        )
    )


def main() -> None:
    sql_root = Path(os.getenv("SQL_ROOT", "/app/sql"))
    client = ClickHouseClient()
    apply_schema(client, sql_root)

    input_fingerprint = bronze_fingerprint(client)
    run_id = uuid5(
        RUN_NAMESPACE,
        f"{TRANSFORMATION_VERSION}:{input_fingerprint}",
    )
    if was_published(client, TRANSFORMATION_VERSION, input_fingerprint):
        print(
            f"SKIPPED version={TRANSFORMATION_VERSION} run_id={run_id}",
            flush=True,
        )
        return

    started_at = utc_now()
    write_status(client, run_id, input_fingerprint, "RUNNING", 0, 0, started_at, None, None)
    try:
        execute_sql_file(
            client,
            sql_root / "transform" / "31_silver_transform.sql",
            {"{{RUN_ID}}": str(run_id)},
        )
        rows_written = count_rows(client, run_id)
        quality_events = count_quality_events(client, run_id)
        write_status(
            client,
            run_id,
            input_fingerprint,
            "SUCCESS",
            rows_written,
            quality_events,
            started_at,
            utc_now(),
            None,
        )
        print(
            f"DONE version={TRANSFORMATION_VERSION} run_id={run_id} "
            f"rows={rows_written} quality_events={quality_events}",
            flush=True,
        )
    except Exception as error:
        write_status(
            client,
            run_id,
            input_fingerprint,
            "FAILED",
            0,
            0,
            started_at,
            utc_now(),
            str(error)[:4_000],
        )
        raise


if __name__ == "__main__":
    main()
