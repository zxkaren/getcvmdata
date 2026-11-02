
import pytest

from financial_data.processing.cvm_sre.overview import (
    build_sre_overview,
)


def create_offer(
    request_id: str,
    amount_brl: str,
    reference_date: str = "2026-10-01",
    security_type: str | None = "Debêntures",
    status: str | None = "Registro Concedido",
    issuer_cnpj: str | None = "11111111000111",
    coordinator_cnpj: str | None = "33333333000133",
) -> dict:
    """Cria uma oferta normalizada para os testes."""
    return {
        "source": "cvm_sre",
        "offer_key": f"cvm_sre:{request_id}",
        "request_id": request_id,
        "issuer": {
            "name": f"Emissor {request_id}",
            "cnpj": issuer_cnpj,
        },
        "lead_coordinator": {
            "name": "Coordenador Teste",
            "cnpj": coordinator_cnpj,
        },
        "security_type": security_type,
        "offer_type": "PRIMARIA",
        "status": status,
        "amount_brl": amount_brl,
        "reference_date": reference_date,
        "cvm_url": (
            "https://web.cvm.gov.br/sre-publico-cvm/"
            f"#/oferta-publica/{request_id}"
        ),
    }


def test_build_sre_overview_aggregates_offers() -> None:
    offers = [
        create_offer("10", "100.1000"),
        create_offer(
            "11",
            "200.0200",
            reference_date="2026-10-02",
            status="Oferta Encerrada",
        ),
        create_offer(
            "12",
            "0.0001",
            reference_date="2026-10-02",
            security_type=None,
            status=None,
            issuer_cnpj=None,
            coordinator_cnpj=None,
        ),
    ]

    overview = build_sre_overview(offers)

    assert overview["total_offers"] == 3
    assert overview["total_amount_brl"] == "300.1201"
    assert overview["distinct_issuers"] == 1
    assert overview["distinct_lead_coordinators"] == 1
    assert overview["offers_without_issuer_cnpj"] == 1
    assert overview["offers_without_coordinator_cnpj"] == 1

    assert overview["reference_date_range"] == {
        "start": "2026-10-01",
        "end": "2026-10-02",
    }

    assert overview["offers_by_security_type"] == [
        {
            "security_type": "Debêntures",
            "offer_count": 2,
            "amount_brl": "300.1200",
        },
        {
            "security_type": None,
            "offer_count": 1,
            "amount_brl": "0.0001",
        },
    ]

    assert overview["offers_by_date"] == [
        {"date": "2026-10-01", "offer_count": 1},
        {"date": "2026-10-02", "offer_count": 2},
    ]

    status_counts = {
        item["status"]: item["offer_count"]
        for item in overview["offers_by_status"]
    }

    assert status_counts == {
        "Registro Concedido": 1,
        "Oferta Encerrada": 1,
        None: 1,
    }

    assert [
        item["request_id"]
        for item in overview["largest_offers"]
    ] == ["11", "10", "12"]


def test_build_sre_overview_rejects_duplicate_offers() -> None:
    offer = create_offer("10", "100.00")

    with pytest.raises(ValueError, match="Oferta duplicada"):
        build_sre_overview([offer, offer.copy()])


@pytest.mark.parametrize(
    "invalid_amount",
    ["NaN", "-10.00", "valor inválido"],
)
def test_build_sre_overview_rejects_invalid_amounts(
    invalid_amount: str,
) -> None:
    offer = create_offer("10", invalid_amount)

    with pytest.raises(ValueError, match="Valor financeiro inválido"):
        build_sre_overview([offer])


def test_build_sre_overview_limits_ranking() -> None:
    offers = [
        create_offer(str(index), str(index))
        for index in range(1, 13)
    ]

    overview = build_sre_overview(offers)

    assert len(overview["largest_offers"]) == 10
    assert overview["largest_offers"][0]["request_id"] == "12"
    assert overview["largest_offers"][-1]["request_id"] == "3"


def test_build_sre_overview_handles_empty_dataset() -> None:
    overview = build_sre_overview([])

    assert overview["total_offers"] == 0
    assert overview["total_amount_brl"] == "0"
    assert overview["distinct_issuers"] == 0
    assert overview["reference_date_range"] == {
        "start": None,
        "end": None,
    }
    assert overview["largest_offers"] == []
