from __future__ import annotations

import csv
import hashlib
import hmac
import json
import os
import shutil
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import ijson


SOURCE_PATTERNS = (
    "patients/*/patients.csv",
    "sejours/*/sejours.csv",
    "diagnostics/*/diagnostics.json",
    "monitoring/*/monitoring.parquet",
    "referentiels/*/services.csv",
    "referentiels/*/cim10.csv",
)

PATIENT_COLUMNS = (
    "patient_id",
    "nir",
    "nom",
    "prenom",
    "birth_date",
    "sex",
    "region_code",
)

STAY_COLUMNS = (
    "stay_id",
    "patient_id",
    "service_code",
    "admission_ts",
    "discharge_ts",
    "admission_mode",
    "discharge_mode",
)


def required_environment(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"La variable {name} est obligatoire")
    return value


def validate_secrets(patient_secret: str, stay_secret: str) -> None:
    if len(patient_secret) < 32 or len(stay_secret) < 32:
        raise ValueError("Les secrets HMAC doivent contenir au moins 32 caractères")
    if patient_secret == stay_secret:
        raise ValueError("Les secrets HMAC patient et séjour doivent être différents")


def pseudonym(secret: str, source_identifier: str) -> str:
    identifier = source_identifier.strip()
    if not identifier:
        raise ValueError("Un identifiant obligatoire est vide")
    return hmac.new(
        secret.encode("utf-8"),
        identifier.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def require_columns(actual: list[str] | None, expected: tuple[str, ...], source: Path) -> None:
    missing = set(expected).difference(actual or [])
    if missing:
        names = ", ".join(sorted(missing))
        raise ValueError(f"Colonnes absentes dans {source}: {names}")


def discover_sources(source_root: Path) -> list[Path]:
    if not source_root.is_dir():
        raise FileNotFoundError(f"Répertoire source introuvable: {source_root}")
    sources = [path for pattern in SOURCE_PATTERNS for path in source_root.glob(pattern)]
    if not sources:
        raise FileNotFoundError(f"Aucun fichier attendu dans {source_root}")
    return sorted(path for path in sources if path.is_file())


def atomic_write(target: Path, writer: Callable[[Path], None]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        writer(temporary)
        os.replace(temporary, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def copy_patients(source: Path, target: Path, patient_secret: str) -> None:
    def write(temporary: Path) -> None:
        with source.open("r", encoding="utf-8-sig", newline="") as source_file:
            reader = csv.DictReader(source_file)
            require_columns(reader.fieldnames, PATIENT_COLUMNS, source)
            with temporary.open("w", encoding="utf-8", newline="") as lake_file:
                writer = csv.DictWriter(
                    lake_file,
                    fieldnames=("patient_sk", "birth_year", "sex", "region_code"),
                    lineterminator="\n",
                )
                writer.writeheader()
                for row in reader:
                    birth_year = date.fromisoformat(row["birth_date"]).year
                    writer.writerow(
                        {
                            "patient_sk": pseudonym(patient_secret, row["patient_id"]),
                            "birth_year": birth_year,
                            "sex": row["sex"],
                            "region_code": row["region_code"],
                        }
                    )

    atomic_write(target, write)


def copy_stays(
    source: Path,
    target: Path,
    patient_secret: str,
    stay_secret: str,
) -> None:
    output_columns = (
        "stay_sk",
        "patient_sk",
        "service_code",
        "admission_ts",
        "discharge_ts",
        "admission_mode",
        "discharge_mode",
    )

    def write(temporary: Path) -> None:
        with source.open("r", encoding="utf-8-sig", newline="") as source_file:
            reader = csv.DictReader(source_file)
            require_columns(reader.fieldnames, STAY_COLUMNS, source)
            with temporary.open("w", encoding="utf-8", newline="") as lake_file:
                writer = csv.DictWriter(
                    lake_file,
                    fieldnames=output_columns,
                    lineterminator="\n",
                )
                writer.writeheader()
                for row in reader:
                    writer.writerow(
                        {
                            "stay_sk": pseudonym(stay_secret, row["stay_id"]),
                            "patient_sk": pseudonym(patient_secret, row["patient_id"]),
                            "service_code": row["service_code"],
                            "admission_ts": row["admission_ts"],
                            "discharge_ts": row["discharge_ts"],
                            "admission_mode": row["admission_mode"],
                            "discharge_mode": row["discharge_mode"],
                        }
                    )

    atomic_write(target, write)


def copy_diagnostics(source: Path, target: Path, stay_secret: str) -> None:
    def write(temporary: Path) -> None:
        with source.open("rb") as source_file, temporary.open(
            "w", encoding="utf-8"
        ) as lake_file:
            lake_file.write("[\n")
            first = True
            for stay in ijson.items(source_file, "item"):
                if "stay_id" not in stay or "diagnostics" not in stay:
                    raise ValueError(f"Structure JSON invalide dans {source}")
                sanitized: dict[str, Any] = {
                    "stay_sk": pseudonym(stay_secret, stay["stay_id"]),
                    "diagnostics": stay["diagnostics"],
                }
                if not first:
                    lake_file.write(",\n")
                json.dump(sanitized, lake_file, ensure_ascii=False, separators=(",", ":"))
                first = False
            lake_file.write("\n]\n")

    atomic_write(target, write)


def copy_reference(source: Path, target: Path) -> None:
    def write(temporary: Path) -> None:
        with source.open("rb") as source_file, temporary.open("wb") as lake_file:
            shutil.copyfileobj(source_file, lake_file, length=1024 * 1024)

    atomic_write(target, write)


def copy_monitoring(source: Path, target: Path, stay_secret: str) -> None:
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    def write(temporary: Path) -> None:
        parquet_file = parquet.ParquetFile(source)
        if "stay_id" not in parquet_file.schema_arrow.names:
            raise ValueError(f"Colonne stay_id absente dans {source}")

        parquet_writer: parquet.ParquetWriter | None = None
        try:
            for batch in parquet_file.iter_batches(batch_size=10_000):
                stay_index = batch.schema.get_field_index("stay_id")
                source_stay_ids = batch.column(stay_index)
                if source_stay_ids.null_count:
                    raise ValueError(f"Identifiant séjour manquant dans {source}")
                stay_keys = arrow.array([
                    pseudonym(stay_secret, value.as_py())
                    for value in source_stay_ids
                ], type=arrow.string())
                sanitized = batch.set_column(
                    stay_index,
                    arrow.field("stay_sk", arrow.string(), nullable=False),
                    stay_keys,
                )
                if parquet_writer is None:
                    parquet_writer = parquet.ParquetWriter(
                        temporary,
                        sanitized.schema,
                        compression="snappy",
                    )
                parquet_writer.write_batch(sanitized)
        finally:
            if parquet_writer is not None:
                parquet_writer.close()

        if parquet_writer is None:
            raise ValueError(f"Fichier Parquet vide: {source}")

    atomic_write(target, write)


def process_source(
    source: Path,
    target: Path,
    patient_secret: str,
    stay_secret: str,
) -> None:
    domain = source.parts[-3]
    if domain == "patients":
        copy_patients(source, target, patient_secret)
    elif domain == "sejours":
        copy_stays(source, target, patient_secret, stay_secret)
    elif domain == "diagnostics":
        copy_diagnostics(source, target, stay_secret)
    elif domain == "monitoring":
        copy_monitoring(source, target, stay_secret)
    elif domain == "referentiels":
        copy_reference(source, target)
    else:
        raise ValueError(f"Domaine non pris en charge: {domain}")


def main() -> None:
    source_root = Path(os.getenv("SOURCE_ROOT", "/app/source-filestorage"))
    lake_root = Path(os.getenv("LAKE_ROOT", "/app/lake"))
    patient_secret = required_environment("HMAC_PATIENT_SECRET")
    stay_secret = required_environment("HMAC_STAY_SECRET")
    validate_secrets(patient_secret, stay_secret)

    sources = discover_sources(source_root)
    for source in sources:
        relative_path = source.relative_to(source_root)
        target = lake_root / relative_path
        process_source(source, target, patient_secret, stay_secret)
        print(f"COPIED {relative_path}", flush=True)
    print(f"DONE files={len(sources)}", flush=True)


if __name__ == "__main__":
    main()
