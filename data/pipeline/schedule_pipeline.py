from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

def environment_boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError(f"{name} doit être un booléen")


def environment_non_negative_integer(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} doit être un entier") from error
    if value < 0:
        raise ValueError(f"{name} doit être positif ou nul")
    return value


def parse_schedule_time(value: str) -> tuple[int, int]:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError("PIPELINE_SCHEDULE_TIME_UTC doit respecter HH:MM")
    try:
        hour, minute = (int(part) for part in parts)
    except ValueError as error:
        raise ValueError("PIPELINE_SCHEDULE_TIME_UTC doit respecter HH:MM") from error
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("PIPELINE_SCHEDULE_TIME_UTC est hors plage")
    return hour, minute


def next_scheduled_time(now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def execute_scheduled_pipeline() -> None:
    from run_pipeline import main as run_pipeline

    try:
        run_pipeline(trigger_type="SCHEDULED")
    except Exception as error:
        print(f"SCHEDULER run_failed error={str(error)[:4_000]}", flush=True)


def main() -> None:
    hour, minute = parse_schedule_time(
        os.getenv("PIPELINE_SCHEDULE_TIME_UTC", "02:00")
    )
    run_on_startup = environment_boolean("PIPELINE_RUN_ON_STARTUP")
    max_runs = environment_non_negative_integer("PIPELINE_MAX_RUNS", 0)
    completed_runs = 0

    print(
        f"SCHEDULER STARTED schedule_utc={hour:02d}:{minute:02d} "
        f"run_on_startup={str(run_on_startup).lower()}",
        flush=True,
    )

    if run_on_startup:
        execute_scheduled_pipeline()
        completed_runs += 1
        if max_runs and completed_runs >= max_runs:
            print("SCHEDULER STOPPED max_runs_reached", flush=True)
            return

    while True:
        now = datetime.now(timezone.utc)
        scheduled_at = next_scheduled_time(now, hour, minute)
        print(f"SCHEDULER NEXT scheduled_at={scheduled_at.isoformat()}", flush=True)
        while True:
            remaining_seconds = (scheduled_at - datetime.now(timezone.utc)).total_seconds()
            if remaining_seconds <= 0:
                break
            time.sleep(min(remaining_seconds, 60))

        execute_scheduled_pipeline()
        completed_runs += 1
        if max_runs and completed_runs >= max_runs:
            print("SCHEDULER STOPPED max_runs_reached", flush=True)
            return


if __name__ == "__main__":
    main()
