from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dateutil.relativedelta import relativedelta
from decouple import AutoConfig


config = AutoConfig(search_path=str(Path(__file__).resolve().parent.parent))

API_URL = config("API_URL", "")
REQUEST_TIMEOUT = config("REQUEST_TIMEOUT", 30, cast=int)
PAGE_SIZE = config("PAGE_SIZE", 100, cast=int)
TARGET_DIRECTORY_NAME = config("TARGET_DIRECTORY_NAME", "")
LOOKBACK_MONTHS = config("LOOKBACK_MONTHS", 3, cast=int)

logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Resumo:
        Retorna o diretório raiz do projeto.

    Parâmetros:
        Nenhum.

    Retorno:
        Path: caminho absoluto da raiz do projeto.
    """
    return Path(__file__).resolve().parent.parent


def get_target_directory() -> Path:
    """
    Resumo:
        Retorna o diretório target de saída e o cria caso não exista.

    Parâmetros:
        Nenhum.

    Retorno:
        Path: caminho absoluto do diretório target.
    """
    target_directory = get_project_root() / TARGET_DIRECTORY_NAME
    target_directory.mkdir(parents=True, exist_ok=True)
    return target_directory


def get_reference_period() -> tuple[str, str]:
    """
    Resumo:
        Calcula o período de consulta com base na data atual e em LOOKBACK_MONTHS.

    Parâmetros:
        Nenhum.

    Retorno:
        tuple[str, str]: data inicial e data final no formato dd/mm/aaaa.
    """
    end_date = datetime.now().date()
    start_date = end_date - relativedelta(months=LOOKBACK_MONTHS)
    return start_date.strftime("%d/%m/%Y"), end_date.strftime("%d/%m/%Y")


def build_headers() -> dict[str, str]:
    """
    Resumo:
        Monta os headers necessários para a requisição HTTP.

    Parâmetros:
        Nenhum.

    Retorno:
        dict[str, str]: headers da requisição.
    """
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://web.cvm.gov.br",
        "Referer": "https://web.cvm.gov.br/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
    }


def build_payload(page_number: int, start_date: str, end_date: str) -> dict[str, Any]:
    """
    Resumo:
        Monta o payload da requisição POST para a API da CVM.

    Parâmetros:
        page_number (int): número da página consultada.
        start_date (str): data inicial no formato dd/mm/aaaa.
        end_date (str): data final no formato dd/mm/aaaa.

    Retorno:
        dict[str, Any]: payload da requisição.
    """
    return {
        "periodoCriacaoProcesso": {
            "de": start_date,
            "ate": end_date,
        },
        "colunaOrdenacao": "data",
        "direcaoOrdenacao": "DESC",
        "modalidade": "TODAS",
        "opa": False,
        "pagina": page_number,
        "tamanhoPagina": str(PAGE_SIZE),
        "tipoOferta": "OFERTA_REGULAR",
    }


def request_page(
    session: requests.Session,
    page_number: int,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    """
    Resumo:
        Executa uma chamada POST na API e retorna o JSON da página consultada.

    Parâmetros:
        session (requests.Session): sessão HTTP reutilizável.
        page_number (int): número da página consultada.
        start_date (str): data inicial no formato dd/mm/aaaa.
        end_date (str): data final no formato dd/mm/aaaa.

    Retorno:
        dict[str, Any]: resposta JSON da API.
    """
    response = session.post(
        url=API_URL,
        headers=build_headers(),
        json=build_payload(page_number, start_date, end_date),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def collect_records(
    session: requests.Session,
    start_date: str,
    end_date: str,
) -> tuple[list[dict[str, Any]], int]:
    """
    Resumo:
        Coleta todos os registros disponíveis na API com paginação.

    Parâmetros:
        session (requests.Session): sessão HTTP reutilizável.
        start_date (str): data inicial no formato dd/mm/aaaa.
        end_date (str): data final no formato dd/mm/aaaa.

    Retorno:
        tuple[list[dict[str, Any]], int]: registros coletados e total informado pela API.
    """
    records: list[dict[str, Any]] = []
    total_records = 0
    page_number = 1

    while True:
        response_data = request_page(session, page_number, start_date, end_date)
        page_records = response_data.get("registros", [])
        total_records = response_data.get("totalRegistros", 0)

        if not page_records:
            break

        records.extend(page_records)

        logger.info(
            "Página %s processada com %s registros. Total acumulado: %s.",
            page_number,
            len(page_records),
            len(records),
        )

        if len(records) >= total_records:
            break

        page_number += 1

    return records, total_records


def flatten_record(
    record: dict[str, Any],
    parent_key: str = "",
    separator: str = "_",
) -> dict[str, Any]:
    """
    Resumo:
        Converte um registro JSON em estrutura plana para exportação em CSV.

    Parâmetros:
        record (dict[str, Any]): registro original.
        parent_key (str): prefixo acumulado na recursão.
        separator (str): separador entre níveis das chaves.

    Retorno:
        dict[str, Any]: registro achatado.
    """
    flattened_record: dict[str, Any] = {}

    for key, value in record.items():
        current_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            flattened_record.update(flatten_record(value, current_key, separator))
        elif isinstance(value, list):
            flattened_record[current_key] = json.dumps(value, ensure_ascii=False, default=str)
        else:
            flattened_record[current_key] = value

    return flattened_record


def save_json_file(
    file_path: Path,
    records: list[dict[str, Any]],
    start_date: str,
    end_date: str,
    total_records: int,
) -> None:
    """
    Resumo:
        Salva os dados coletados em arquivo JSON com metadados da execução.

    Parâmetros:
        file_path (Path): caminho do arquivo JSON.
        records (list[dict[str, Any]]): registros coletados.
        start_date (str): data inicial consultada.
        end_date (str): data final consultada.
        total_records (int): total informado pela API.

    Retorno:
        None.
    """
    payload = {
        "metadata": {
            "api_url": API_URL,
            "periodo_consulta": {"de": start_date, "ate": end_date},
            "data_execucao": datetime.now().isoformat(),
            "total_registros_informado_api": total_records,
            "total_registros_coletados": len(records),
        },
        "registros": records,
    }

    with file_path.open("w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=False, indent=4)


def save_csv_file(file_path: Path, records: list[dict[str, Any]]) -> None:
    """
    Resumo:
        Salva os dados coletados em arquivo CSV.

    Parâmetros:
        file_path (Path): caminho do arquivo CSV.
        records (list[dict[str, Any]]): registros coletados.

    Retorno:
        None.
    """
    flattened_records = [flatten_record(record) for record in records]

    if not flattened_records:
        with file_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["sem_dados"])
        return

    field_names = sorted({key for record in flattened_records for key in record})

    with file_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(flattened_records)


def build_output_paths(target_directory: Path) -> tuple[Path, Path]:
    """
    Resumo:
        Monta os caminhos dos arquivos de saída JSON e CSV.

    Parâmetros:
        target_directory (Path): diretório target de saída.

    Retorno:
        tuple[Path, Path]: caminho do JSON e caminho do CSV.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file_path = target_directory / f"cvm_sre_ofertas_{timestamp}.json"
    csv_file_path = target_directory / f"cvm_sre_ofertas_{timestamp}.csv"
    return json_file_path, csv_file_path


def task() -> None:
    """
    Resumo:
        Executa a coleta da API da CVM e gera os arquivos JSON e CSV

    Parâmetros:
        Nenhum.

    Retorno:
        None.
    """
    start_date, end_date = get_reference_period()
    target_directory = get_target_directory()
    json_file_path, csv_file_path = build_output_paths(target_directory)

    logger.info(
        "Iniciando coleta da CVM para o período de %s até %s.",
        start_date,
        end_date,
    )

    with requests.Session() as session:
        records, total_records = collect_records(session, start_date, end_date)

    save_json_file(json_file_path, records, start_date, end_date, total_records)
    save_csv_file(csv_file_path, records)

    logger.info("Arquivo JSON gerado em: %s", json_file_path)
    logger.info("Arquivo CSV gerado em: %s", csv_file_path)
    logger.info("Processo concluído com %s registros coletados.", len(records))