from copy import deepcopy

import pytest

from financial_data.processing.cvm_sre.consolidator import (
    consolidate_sre_offers,
)


@pytest.fixture
def sample_offer() -> dict:
    return {
        "source": "cvm_sre",
        "offer_key": "cvm_sre:28562",
        "request_id": "28562",
        "status": "Registro Concedido",
        "amount_brl": "2697000.0400",
    }


def test_consolidation_removes_identical_duplicates(
    sample_offer: dict,
) -> None:
    offers, changes = consolidate_sre_offers(
        existing_offers=[],
        collected_offers=[sample_offer, deepcopy(sample_offer)],
        observed_at="2026-11-01T21:59:09",
    )

    assert len(offers) == 1
    assert changes == []
    assert offers[0]["first_seen_at"] == "2026-11-01T21:59:09"


def test_consolidation_preserves_status_changes(
    sample_offer: dict,
) -> None:
    first_offers, _ = consolidate_sre_offers(
        existing_offers=[],
        collected_offers=[sample_offer],
        observed_at="2026-11-01T21:59:09",
    )

    updated_offer = deepcopy(sample_offer)
    updated_offer["status"] = "Oferta Encerrada"

    offers, changes = consolidate_sre_offers(
        existing_offers=first_offers,
        collected_offers=[updated_offer],
        observed_at="2026-11-02T10:00:00",
    )

    assert len(offers) == 1
    assert offers[0]["status"] == "Oferta Encerrada"
    assert offers[0]["first_seen_at"] == "2026-11-01T21:59:09"
    assert len(changes) == 1

    assert changes[0]["changed_fields"]["status"] == {
        "before": "Registro Concedido",
        "after": "Oferta Encerrada",
    }


def test_consolidation_is_idempotent(
    sample_offer: dict,
) -> None:
    first_offers, _ = consolidate_sre_offers(
        existing_offers=[],
        collected_offers=[sample_offer],
        observed_at="2026-11-01T21:59:09",
    )

    repeated_offers, changes = consolidate_sre_offers(
        existing_offers=first_offers,
        collected_offers=[sample_offer],
        observed_at="2026-11-01T21:59:09",
    )

    assert repeated_offers == first_offers
    assert changes == []


def test_consolidation_rejects_conflicting_duplicates(
    sample_offer: dict,
) -> None:
    conflicting_offer = deepcopy(sample_offer)
    conflicting_offer["amount_brl"] = "5000000.0000"

    with pytest.raises(ValueError):
        consolidate_sre_offers(
            existing_offers=[],
            collected_offers=[sample_offer, conflicting_offer],
            observed_at="2026-11-01T21:59:09",
        )


def test_consolidation_rejects_older_observation(
    sample_offer: dict,
) -> None:
    existing_offers, _ = consolidate_sre_offers(
        existing_offers=[],
        collected_offers=[sample_offer],
        observed_at="2026-11-02T10:00:00",
    )

    with pytest.raises(ValueError):
        consolidate_sre_offers(
            existing_offers=existing_offers,
            collected_offers=[sample_offer],
            observed_at="2026-11-01T21:59:09",
        )