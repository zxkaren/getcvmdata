
import logging
from typing import Any

import requests

from financial_data.config.cvm_sre import (
    API_URL,
    PAGE_SIZE,
    REQUEST_TIMEOUT,
)


logger = logging.getLogger(__name__)


def build_headers() -> dict[str, str]:
    """Monta os cabeçalhos HTTP utilizados pela API SRE."""
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


def build_payload(
    page_number: int,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    """Monta os parâmetros de consulta da API CVM SRE."""
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
    """Consulta uma página de ofertas públicas da CVM SRE."""
    response = session.post(
        url=API_URL,
        headers=build_headers(),
        json=build_payload(
            page_number,
            start_date,
            end_date,
        ),
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()
    response_data = response.json()

    if not isinstance(response_data, dict):
        raise ValueError(
            f"Resposta inválida da API na página {page_number}."
        )

    return response_data


def collect_records(
    session: requests.Session,
    start_date: str,
    end_date: str,
) -> tuple[list[dict[str, Any]], int]:
    """Coleta e valida os registros paginados da CVM SRE."""
    records: list[dict[str, Any]] = []
    total_records: int | None = None
    page_number = 1

    while True:
        response_data = request_page(
            session,
            page_number,
            start_date,
            end_date,
        )

        page_records = response_data.get("registros")
        reported_total = response_data.get("totalRegistros")

        if type(reported_total) is not int or reported_total < 0:
            raise ValueError(
                f"Total de registros inválido na página {page_number}."
            )

        if not isinstance(page_records, list):
            raise ValueError(
                f"Lista de registros inválida na página {page_number}."
            )

        if not all(
            isinstance(record, dict)
            for record in page_records
        ):
            raise ValueError(
                f"Registros com formato inválido na página {page_number}."
            )

        if total_records is None:
            total_records = reported_total
        elif reported_total != total_records:
            raise RuntimeError(
                "O total de registros informado pela API mudou "
                f"durante a paginação. Página: {page_number}."
            )

        if not page_records:
            if total_records == 0 and not records:
                logger.info("Nenhum registro encontrado no período.")
                break

            raise RuntimeError(
                "Coleta incompleta: a API retornou uma página vazia "
                f"antes de completar os {total_records} registros."
            )

        records.extend(page_records)

        if len(records) > total_records:
            raise RuntimeError(
                "Coleta inconsistente: a quantidade de registros "
                "recebidos ultrapassou o total informado pela API."
            )

        logger.info(
            "Página %s processada com %s registros. "
            "Total acumulado: %s.",
            page_number,
            len(page_records),
            len(records),
        )

        if len(records) == total_records:
            break

        page_number += 1

    return records, total_records
