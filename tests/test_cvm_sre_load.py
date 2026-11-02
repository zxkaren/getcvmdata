import json
from pathlib import Path

import pytest

from financial_data.application.prepare_cvm_sre_data import (
    prepare_cvm_sre_data,
)


def create_raw_extraction(
    directory: Path,
    timestamp: str,
    status: str,
    amount: str = "2.697.000,0400",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)

    file_path = directory / f"cvm_sre_ofertas_{timestamp}.json"

    content = {
        "metadata": {
            "data_execucao": (
                f"{timestamp[:4]}-{timestamp[4:6]}-"
                f"{timestamp[6:8]}T{timestamp[9:11]}:"
                f"{timestamp[11:13]}:{timestamp[13:15]}"
            ),
            "total_registros_coletados": 1,
        },
        "registros": [
            {
                "idRequerimento": "28562",
                "nomeEmissor": "Empresa Teste S.A.",
                "cnpjEmissor": "62.550.985/0001-86",
                "valorTotalEmReais": amount,
                "data": "30/10/2026",
                "statusDaOferta": status,
            }
        ],
    }

    file_path.write_text(
        json.dumps(content, ensure_ascii=False),
        encoding="utf-8",
    )

    return file_path


def test_load_consolidates_history_and_is_idempotent(
    tmp_path: Path,
) -> None:
    raw_directory = tmp_path / "raw"
    processed_directory = tmp_path / "processed"

    create_raw_extraction(
        raw_directory,
        "20261101_100000",
        "Registro Concedido",
    )
    create_raw_extraction(
        raw_directory,
        "20261102_100000",
        "Oferta Encerrada",
    )

    current_file, history_file = prepare_cvm_sre_data(
        raw_directory,
        processed_directory,
    )

    current_content = current_file.read_text(encoding="utf-8")
    history_content = history_file.read_text(encoding="utf-8")

    current_data = json.loads(current_content)
    history_data = json.loads(history_content)

    assert current_data["metadata"]["total_offers"] == 1
    assert current_data["offers"][0]["status"] == "Oferta Encerrada"
    assert history_data["metadata"]["total_changes"] == 1
    assert history_data["changes"][0]["offer_key"] == "cvm_sre:28562"

    prepare_cvm_sre_data(raw_directory, processed_directory)

    assert current_file.read_text(encoding="utf-8") == current_content
    assert history_file.read_text(encoding="utf-8") == history_content


def test_invalid_extraction_preserves_processed_files(
    tmp_path: Path,
) -> None:
    raw_directory = tmp_path / "raw"
    processed_directory = tmp_path / "processed"

    create_raw_extraction(
        raw_directory,
        "20261101_100000",
        "Registro Concedido",
    )

    current_file, history_file = prepare_cvm_sre_data(
        raw_directory,
        processed_directory,
    )

    original_current = current_file.read_bytes()
    original_history = history_file.read_bytes()

    create_raw_extraction(
        raw_directory,
        "20261102_100000",
        "Oferta Encerrada",
        amount="valor inválido",
    )

    with pytest.raises(ValueError):
        prepare_cvm_sre_data(
            raw_directory,
            processed_directory,
        )

    assert current_file.read_bytes() == original_current
    assert history_file.read_bytes() == original_history