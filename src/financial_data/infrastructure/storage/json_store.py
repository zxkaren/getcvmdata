import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from financial_data.infrastructure.exporters.json_exporter import (
    save_json_file,
)


def read_json_object(file_path: Path) -> dict[str, Any]:
    """Lê um arquivo JSON e valida sua estrutura principal."""
    with file_path.open("r", encoding="utf-8") as json_file:
        content = json.load(json_file)

    if not isinstance(content, dict):
        raise ValueError(f"Objeto JSON inválido: {file_path}")

    return content


def save_json_object_atomically(
    file_path: Path,
    content: dict[str, Any],
) -> None:
    """Grava um JSON sem expor conteúdo parcialmente escrito."""
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with NamedTemporaryFile(
        dir=file_path.parent,
        prefix=f".{file_path.stem}-",
        suffix=".tmp",
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)

    try:
        save_json_file(temporary_path, content)
        temporary_path.replace(file_path)
    finally:
        temporary_path.unlink(missing_ok=True)