from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any


SOURCE_NAME = "cvm_sre"
MAX_RANKED_OFFERS = 10


def parse_offer_amount(value: Any) -> Decimal:
    """Converte o valor normalizado da oferta para Decimal."""
    if not isinstance(value, str):
        raise ValueError("Valor financeiro com formato inválido.")

    try:
        amount = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("Valor financeiro inválido.") from error

    if not amount.is_finite() or amount < 0:
        raise ValueError("Valor financeiro inválido.")

    return amount


def build_sre_overview(
    offers: list[dict[str, Any]],
) -> dict[str, Any]:
    """Calcula os indicadores executivos das ofertas CVM SRE."""
    offer_keys: set[str] = set()
    issuer_cnpjs: set[str] = set()
    coordinator_cnpjs: set[str] = set()

    instrument_groups: dict[str | None, dict[str, Any]] = {}
    status_counts: dict[str | None, int] = defaultdict(int)
    date_counts: dict[str, int] = defaultdict(int)

    ranked_offers: list[tuple[Decimal, dict[str, Any]]] = []
    total_amount = Decimal("0")

    offers_without_issuer_cnpj = 0
    offers_without_coordinator_cnpj = 0

    for offer in offers:
        offer_key = offer.get("offer_key")

        if (
            offer.get("source") != SOURCE_NAME
            or not isinstance(offer_key, str)
            or not offer_key.startswith(f"{SOURCE_NAME}:")
        ):
            raise ValueError("Identificador ou fonte inválida.")

        if offer_key in offer_keys:
            raise ValueError(f"Oferta duplicada: {offer_key}")

        offer_keys.add(offer_key)

        amount = parse_offer_amount(offer.get("amount_brl"))
        total_amount += amount

        issuer_cnpj = offer["issuer"]["cnpj"]
        coordinator_cnpj = offer["lead_coordinator"]["cnpj"]

        if issuer_cnpj:
            issuer_cnpjs.add(issuer_cnpj)
        else:
            offers_without_issuer_cnpj += 1

        if coordinator_cnpj:
            coordinator_cnpjs.add(coordinator_cnpj)
        else:
            offers_without_coordinator_cnpj += 1

        security_type = offer["security_type"]
        status = offer["status"]
        reference_date = offer["reference_date"]

        if not isinstance(reference_date, str):
            raise ValueError("Data de referência inválida.")

        try:
            date.fromisoformat(reference_date)
        except ValueError as error:
            raise ValueError("Data de referência inválida.") from error

        instrument_group = instrument_groups.setdefault(
            security_type,
            {
                "offer_count": 0,
                "amount_brl": Decimal("0"),
            },
        )

        instrument_group["offer_count"] += 1
        instrument_group["amount_brl"] += amount

        status_counts[status] += 1
        date_counts[reference_date] += 1

        ranked_offers.append((amount, offer))

    offers_by_security_type = [
        {
            "security_type": security_type,
            "offer_count": metrics["offer_count"],
            "amount_brl": str(metrics["amount_brl"]),
        }
        for security_type, metrics in sorted(
            instrument_groups.items(),
            key=lambda item: (
                -item[1]["offer_count"],
                item[0] or "",
            ),
        )
    ]

    offers_by_status = [
        {
            "status": status,
            "offer_count": count,
        }
        for status, count in sorted(
            status_counts.items(),
            key=lambda item: (-item[1], item[0] or ""),
        )
    ]

    offers_by_date = [
        {
            "date": reference_date,
            "offer_count": count,
        }
        for reference_date, count in sorted(date_counts.items())
    ]

    largest_offers = [
        {
            "offer_key": offer["offer_key"],
            "request_id": offer["request_id"],
            "issuer_name": offer["issuer"]["name"],
            "security_type": offer["security_type"],
            "amount_brl": str(amount),
            "cvm_url": offer["cvm_url"],
        }
        for amount, offer in sorted(
            ranked_offers,
            key=lambda item: (-item[0], item[1]["offer_key"]),
        )[:MAX_RANKED_OFFERS]
    ]

    return {
        "total_offers": len(offer_keys),
        "total_amount_brl": str(total_amount),
        "distinct_issuers": len(issuer_cnpjs),
        "distinct_lead_coordinators": len(coordinator_cnpjs),
        "offers_without_issuer_cnpj": offers_without_issuer_cnpj,
        "offers_without_coordinator_cnpj": (
            offers_without_coordinator_cnpj
        ),
        "reference_date_range": {
            "start": min(date_counts, default=None),
            "end": max(date_counts, default=None),
        },
        "offers_by_security_type": offers_by_security_type,
        "offers_by_status": offers_by_status,
        "offers_by_date": offers_by_date,
        "largest_offers": largest_offers,
    }
