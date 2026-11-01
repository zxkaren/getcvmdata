
import csv
import json
from pathlib import Path
from typing import Any

def flatten_record(
    record: dict[str, Any],
    parent_key: str = "",
    separator: str = "_",
) -> dict[str, Any]:
    """Achata objetos aninhados, preservando listas como JSON."""
    flattened_record: dict[str, Any] = {}

    for key, value in record.items():
        current_key = (
            f"{parent_key}{separator}{key}"
            if parent_key
            else key
        )

        if isinstance(value, dict):
            flattened_record.update(
                flatten_record(value, current_key, separator)
            )
        elif isinstance(value, list):
            flattened_record[current_key] = json.dumps(
                value,
                ensure_ascii=False,
                default=str,
            )
        else:
            flattened_record[current_key] = value

    return flattened_record


def save_csv_file(
    file_path: Path,
    records: list[dict[str, Any]],
) -> None:
    """Exporta registros para CSV com codificação UTF-8 BOM."""
    flattened_records = [
        flatten_record(record)
        for record in records
    ]

    if not flattened_records:
        with file_path.open(
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["sem_dados"])
        return

    field_names = sorted({
        key
        for record in flattened_records
        for key in record
    })

    with file_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=field_names,
        )
        writer.writeheader()
        writer.writerows(flattened_records)
