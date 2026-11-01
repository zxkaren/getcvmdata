
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from financial_data.config.cvm_sre import API_URL
from financial_data.infrastructure.clients.cvm_sre_client import (
    collect_records,
)
from financial_data.infrastructure.exporters.csv_exporter import (
    save_csv_file,
)
from financial_data.infrastructure.exporters.json_exporter import (
    save_json_file,
)
from financial_data.infrastructure.storage.output_directory import (
    get_target_directory,
)


DATA_SOURCE_NAME = "cvm_sre"

logger = logging.getLogger(__name__)


def format_sre_query_period(
    start_date: date,
    end_date: date,
) -> tuple[str, str]:
    """Formata o período compartilhado para a API SRE."""
    return (
        start_date.strftime("%d/%m/%Y"),
        end_date.strftime("%d/%m/%Y"),
    )


def build_sre_export_payload(
    records: list[dict[str, Any]],
    start_date: str,
    end_date: str,
    total_records: int,
) -> dict[str, Any]:
    """Organiza os registros e metadados da coleta SRE."""
    return {
        "metadata": {
            "api_url": API_URL,
            "periodo_consulta": {
                "de": start_date,
                "ate": end_date,
            },
            "data_execucao": datetime.now().isoformat(),
            "total_registros_informado_api": total_records,
            "total_registros_coletados": len(records),
        },
        "registros": records,
    }


def build_output_paths(
    target_directory: Path,
) -> tuple[Path, Path]:
    """Define os caminhos dos arquivos JSON e CSV da execução."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_file_path = (
        target_directory / f"cvm_sre_ofertas_{timestamp}.json"
    )
    csv_file_path = (
        target_directory / f"cvm_sre_ofertas_{timestamp}.csv"
    )

    return json_file_path, csv_file_path


def run_cvm_sre_pipeline(
    start_date: date,
    end_date: date,
) -> None:
    """Executa a coleta SRE e exporta os registros em JSON e CSV."""
    sre_start_date, sre_end_date = format_sre_query_period(
        start_date,
        end_date,
    )

    target_directory = get_target_directory(DATA_SOURCE_NAME)

    json_file_path, csv_file_path = build_output_paths(
        target_directory
    )

    logger.info(
        "Iniciando coleta da CVM para o período de %s até %s.",
        start_date,
        end_date,
    )

    with requests.Session() as session:
        records, total_records = collect_records(
            session,
            sre_start_date,
            sre_end_date,
        )

    json_payload = build_sre_export_payload(
        records,
        sre_start_date,
        sre_end_date,
        total_records,
    )

    save_json_file(json_file_path, json_payload)
    save_csv_file(csv_file_path, records)

    logger.info("Arquivo JSON gerado em: %s", json_file_path)
    logger.info("Arquivo CSV gerado em: %s", csv_file_path)
    logger.info(
        "Processo concluído com %s registros coletados.",
        len(records),
    )
