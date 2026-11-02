from copy import deepcopy
from datetime import datetime
from typing import Any


OBSERVATION_FIELDS = frozenset({
    "first_seen_at",
    "last_seen_at",
})


def get_offer_content(offer: dict[str, Any]) -> dict[str, Any]:
    """Retorna os dados da oferta sem metadados de observação."""
    return {
        field: value
        for field, value in offer.items()
        if field not in OBSERVATION_FIELDS
    }


def consolidate_sre_offers(
    existing_offers: list[dict[str, Any]],
    collected_offers: list[dict[str, Any]],
    observed_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Consolida ofertas e identifica alterações entre coletas."""
    observation_date = datetime.fromisoformat(observed_at)

    offers_by_key: dict[str, dict[str, Any]] = {}
    changes: list[dict[str, Any]] = []
    collected_by_key: dict[str, dict[str, Any]] = {}

    for offer in existing_offers:
        offer_key = offer["offer_key"]

        if offer_key in offers_by_key:
            raise ValueError(
                f"Oferta duplicada no estado atual: {offer_key}"
            )

        offers_by_key[offer_key] = deepcopy(offer)

    for offer in collected_offers:
        offer_key = offer["offer_key"]
        previous_collected = collected_by_key.get(offer_key)

        if previous_collected is not None:
            if previous_collected != offer:
                raise ValueError(
                    f"Registros conflitantes na coleta: {offer_key}"
                )
            continue

        collected_by_key[offer_key] = offer

        existing_offer = offers_by_key.get(offer_key)

        if existing_offer is None:
            new_offer = deepcopy(offer)
            new_offer["first_seen_at"] = observed_at
            new_offer["last_seen_at"] = observed_at
            offers_by_key[offer_key] = new_offer
            continue

        previous_observation = datetime.fromisoformat(
            existing_offer["last_seen_at"]
        )

        if observation_date < previous_observation:
            raise ValueError(
                f"Coleta anterior ao estado consolidado: {offer_key}"
            )

        previous_content = get_offer_content(existing_offer)
        current_content = get_offer_content(offer)

        if (
            observation_date == previous_observation
            and previous_content != current_content
        ):
            raise ValueError(
                f"Dados conflitantes na mesma observação: {offer_key}"
            )

        changed_fields = {
            field: {
                "before": previous_content.get(field),
                "after": current_content.get(field),
            }
            for field in sorted(
                previous_content.keys() | current_content.keys()
            )
            if previous_content.get(field) != current_content.get(field)
        }

        if changed_fields:
            changes.append({
                "offer_key": offer_key,
                "observed_at": observed_at,
                "changed_fields": changed_fields,
            })

        updated_offer = deepcopy(offer)
        updated_offer["first_seen_at"] = existing_offer["first_seen_at"]
        updated_offer["last_seen_at"] = observed_at

        offers_by_key[offer_key] = updated_offer

    consolidated_offers = [
        offers_by_key[offer_key]
        for offer_key in sorted(offers_by_key)
    ]

    return consolidated_offers, changes