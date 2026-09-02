from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from uuid import UUID, uuid4

from copy_to_lake import main as copy_to_lake
from load_bronze import main as load_bronze
from transform_gold import ClickHouseClient, execute_sql_file, utc_now
from transform_gold import main as transform_gold
from transform_silver import main as transform_silver


PIPELINE_VERSION = "pipeline-v1"


@dataclass(frozen=True)
class PipelineStep:
    order: int
    name: str
    operation: Callable[[], None]


PIPELINE_STEPS = (
    PipelineStep(1, "lake_copy", copy_to_lake),
    PipelineStep(2, "bronze_load", load_bronze),
    PipelineStep(3, "silver_transform", transform_silver),
    PipelineStep(4, "gold_transform", transform_gold),
)


def normalize_trigger_type(value: str) -> str:
    trigger_type = value.strip().upper()
    if trigger_type not in {"MANUAL", "SCHEDULED"}:
        raise ValueError("PIPELINE_TRIGGER_TYPE doit valoir MANUAL ou SCHEDULED")
    return trigger_type


def write_pipeline_status(
    client: ClickHouseClient,
    pipeline_run_id: UUID,
    trigger_type: str,
    status: str,
    started_at: str,
    finished_at: str | None,
    error_message: str | None,
) -> None:
    client.insert_json(
        "control.pipeline_runs",
        {
            "pipeline_run_id": str(pipeline_run_id),
            "pipeline_version": PIPELINE_VERSION,
            "trigger_type": trigger_type,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "updated_at": utc_now(),
        },
    )


def write_step_status(
    client: ClickHouseClient,
    pipeline_run_id: UUID,
    step: PipelineStep,
    status: str,
    started_at: str,
    finished_at: str | None,
    error_message: str | None,
) -> None:
    client.insert_json(
        "control.pipeline_step_runs",
        {
            "pipeline_run_id": str(pipeline_run_id),
            "step_order": step.order,
            "step_name": step.name,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "updated_at": utc_now(),
        },
    )


def main(trigger_type: str | None = None) -> None:
    sql_root = Path(os.getenv("SQL_ROOT", "/app/clickhouse"))
    actual_trigger = normalize_trigger_type(
        trigger_type or os.getenv("PIPELINE_TRIGGER_TYPE", "MANUAL")
    )
    client = ClickHouseClient()
    execute_sql_file(client, sql_root / "init" / "10_control.sql")

    pipeline_run_id = uuid4()
    pipeline_started_at = utc_now()
    write_pipeline_status(
        client,
        pipeline_run_id,
        actual_trigger,
        "RUNNING",
        pipeline_started_at,
        None,
        None,
    )
    print(
        f"PIPELINE RUNNING pipeline_run_id={pipeline_run_id} "
        f"trigger={actual_trigger}",
        flush=True,
    )

    try:
        for step in PIPELINE_STEPS:
            step_started_at = utc_now()
            write_step_status(
                client,
                pipeline_run_id,
                step,
                "RUNNING",
                step_started_at,
                None,
                None,
            )
            print(
                f"STEP RUNNING pipeline_run_id={pipeline_run_id} "
                f"step={step.name}",
                flush=True,
            )
            try:
                step.operation()
            except Exception as error:
                message = str(error)[:4_000]
                write_step_status(
                    client,
                    pipeline_run_id,
                    step,
                    "FAILED",
                    step_started_at,
                    utc_now(),
                    message,
                )
                raise
            write_step_status(
                client,
                pipeline_run_id,
                step,
                "SUCCESS",
                step_started_at,
                utc_now(),
                None,
            )
            print(
                f"STEP SUCCESS pipeline_run_id={pipeline_run_id} "
                f"step={step.name}",
                flush=True,
            )
    except Exception as error:
        message = str(error)[:4_000]
        write_pipeline_status(
            client,
            pipeline_run_id,
            actual_trigger,
            "FAILED",
            pipeline_started_at,
            utc_now(),
            message,
        )
        print(
            f"PIPELINE FAILED pipeline_run_id={pipeline_run_id} error={message}",
            flush=True,
        )
        raise

    write_pipeline_status(
        client,
        pipeline_run_id,
        actual_trigger,
        "SUCCESS",
        pipeline_started_at,
        utc_now(),
        None,
    )
    print(f"PIPELINE SUCCESS pipeline_run_id={pipeline_run_id}", flush=True)


if __name__ == "__main__":
    main()
