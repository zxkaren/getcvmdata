import json
from pathlib import Path
from typing import Any

def save_json_file(
    file_path: Path,
    payload: dict[str, Any],
) -> None:
    """Grava dados estruturados em um arquivo JSON."""
    with file_path.open("w", encoding="utf-8") as json_file:
        json.dump(
            payload,
            json_file,
            ensure_ascii=False,
            indent=4,
        )
