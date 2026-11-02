from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from financial_data.infrastructure.storage.json_store import (
    read_json_object,
    save_json_object_atomically,
)
from financial_data.infrastructure.storage.output_directory import (
    get_project_root,
)


SOURCE_NAME = "cvm_sre"
SCHEMA_VERSION = 1


def publish_dashboard_data(
    source_file: Path,
    target_directory: Path,
) -> tuple[Path, Path]:
    """Valida e publica os dados consolidados para o dashboard."""
    source_data = read_json_object(source_file)

    metadata = source_data.get("metadata")
    offers = source_data.get("offers")

    if not isinstance(metadata, dict) or not isinstance(offers, list):
        raise ValueError("Estrutura dos dados consolidados inválida.")

    if (
        metadata.get("source") != SOURCE_NAME
        or metadata.get("schema_version") != SCHEMA_VERSION
    ):
        raise ValueError("Fonte ou versão do contrato incompatível.")

    total_offers = metadata.get("total_offers")

    if type(total_offers) is not int or total_offers != len(offers):
        raise ValueError("Quantidade de ofertas inconsistente.")

    last_source_execution_at = metadata.get(
        "last_source_execution_at"
    )

    if not isinstance(last_source_execution_at, str):
        raise ValueError("Data da última coleta inválida.")

    try:
        datetime.fromisoformat(last_source_execution_at)
    except ValueError as error:
        raise ValueError("Data da última coleta inválida.") from error

    offer_keys: set[str] = set()

    for offer in offers:
        if not isinstance(offer, dict):
            raise ValueError("Oferta com estrutura inválida.")

        offer_key = offer.get("offer_key")

        if (
            not isinstance(offer_key, str)
            or not offer_key.startswith(f"{SOURCE_NAME}:")
            or offer_key in offer_keys
        ):
            raise ValueError(f"Identificador de oferta inválido: {offer_key}")

        offer_keys.add(offer_key)

    offers_file = target_directory / "offers.json"
    manifest_file = target_directory / "manifest.json"

    offers_payload: dict[str, Any] = {
        "metadata": metadata,
        "offers": offers,
    }

    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            SOURCE_NAME: {
                "last_source_execution_at": last_source_execution_at,
                "total_offers": total_offers,
            }
        },
        "datasets": {
            "offers": "offers.json",
        },
    }

    save_json_object_atomically(
        offers_file,
        offers_payload,
    )

    save_json_object_atomically(
        manifest_file,
        manifest_payload,
    )

    return offers_file, manifest_file


def main() -> None:
    """Publica os dados consolidados da CVM SRE para o dashboard."""
    project_root = get_project_root()

    source_file = (
        project_root
        / "data"
        / "processed"
        / SOURCE_NAME
        / "offers_current.json"
    )

    target_directory = (
        project_root
        / "data"
        / "processed"
        / "dashboard"
        / "v1"
    )

    offers_file, manifest_file = publish_dashboard_data(
        source_file,
        target_directory,
    )

    print(f"Ofertas publicadas: {offers_file}")
    print(f"Manifesto publicado: {manifest_file}")


if __name__ == "__main__":
    main()