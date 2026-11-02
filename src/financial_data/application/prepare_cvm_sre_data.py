import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from financial_data.infrastructure.storage.json_store import (
    read_json_object,
    save_json_object_atomically,
)
from financial_data.processing.cvm_sre.consolidator import (
    consolidate_sre_offers,
)
from financial_data.processing.cvm_sre.normalizer import (
    normalize_sre_offer_record,
)


SOURCE_NAME = "cvm_sre"
RAW_FILE_PATTERN = "cvm_sre_ofertas_*.json"
SCHEMA_VERSION = 1


def prepare_cvm_sre_data(
    raw_directory: Path,
    processed_directory: Path,
) -> tuple[Path, Path]:
    """Consolida extrações SRE e publica o estado e o histórico."""
    raw_files = sorted(raw_directory.glob(RAW_FILE_PATTERN))

    if not raw_files:
        raise FileNotFoundError(
            f"Nenhuma extração CVM SRE encontrada em {raw_directory}"
        )

    consolidated_offers: list[dict[str, Any]] = []
    offer_history: list[dict[str, Any]] = []
    latest_observation: datetime | None = None
    latest_observed_at = ""

    for raw_file in raw_files:
        extraction = read_json_object(raw_file)
        metadata = extraction.get("metadata")
        records = extraction.get("registros")

        if not isinstance(metadata, dict) or not isinstance(
            records, list
        ):
            raise ValueError(f"Extração inválida: {raw_file.name}")

        reported_total = metadata.get("total_registros_coletados")

        if type(reported_total) is not int or reported_total != len(
            records
        ):
            raise ValueError(
                f"Quantidade inconsistente: {raw_file.name}"
            )

        observed_at = metadata.get("data_execucao")

        if not isinstance(observed_at, str):
            raise ValueError(
                f"Data da coleta ausente: {raw_file.name}"
            )

        try:
            observation_date = datetime.fromisoformat(observed_at)
        except ValueError as error:
            raise ValueError(
                f"Data da coleta inválida: {raw_file.name}"
            ) from error

        if (
            latest_observation is not None
            and observation_date < latest_observation
        ):
            raise ValueError(
                f"Extrações fora de ordem: {raw_file.name}"
            )

        if not all(isinstance(record, dict) for record in records):
            raise ValueError(
                f"Registros inválidos: {raw_file.name}"
            )

        normalized_offers = [
            normalize_sre_offer_record(record)
            for record in records
        ]

        consolidated_offers, changes = consolidate_sre_offers(
            existing_offers=consolidated_offers,
            collected_offers=normalized_offers,
            observed_at=observed_at,
        )

        offer_history.extend(changes)
        latest_observation = observation_date
        latest_observed_at = observed_at

        logging.info("Extração processada: %s", raw_file.name)

    metadata = {
        "source": SOURCE_NAME,
        "schema_version": SCHEMA_VERSION,
        "last_source_execution_at": latest_observed_at,
    }

    current_file = processed_directory / "offers_current.json"
    history_file = processed_directory / "offers_history.json"

    save_json_object_atomically(
        current_file,
        {
            "metadata": {
                **metadata,
                "total_offers": len(consolidated_offers),
            },
            "offers": consolidated_offers,
        },
    )

    save_json_object_atomically(
        history_file,
        {
            "metadata": {
                **metadata,
                "total_changes": len(offer_history),
            },
            "changes": offer_history,
        },
    )

    return current_file, history_file


def main() -> None:
    """Prepara os dados SRE utilizando os diretórios do projeto."""
    from financial_data.infrastructure.storage.output_directory import (
        RAW_DATA_DIRECTORY,
        get_project_root,
    )

    logging.basicConfig(level=logging.INFO)

    project_root = get_project_root()

    raw_directory = (
        project_root / RAW_DATA_DIRECTORY / SOURCE_NAME
    )

    processed_directory = (
        project_root / "data" / "processed" / SOURCE_NAME
    )

    current_file, history_file = prepare_cvm_sre_data(
        raw_directory,
        processed_directory,
    )

    logging.info("Estado consolidado: %s", current_file)
    logging.info("Histórico de alterações: %s", history_file)


if __name__ == "__main__":
    main()