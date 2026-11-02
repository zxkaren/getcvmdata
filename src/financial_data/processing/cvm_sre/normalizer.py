from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


SOURCE_NAME = "cvm_sre"
CVM_OFFER_URL = (
    "https://web.cvm.gov.br/sre-publico-cvm/#/oferta-publica/"
)


def normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None

    if not isinstance(value, str):
        raise ValueError("Campo textual com formato inválido.")

    return value.strip() or None


def normalize_cnpj(value: Any) -> str | None:
    cnpj = normalize_optional_text(value)

    if cnpj is None:
        return None

    digits = "".join(
        character for character in cnpj if character.isdigit()
    )

    if len(digits) != 14:
        raise ValueError("CNPJ com formato inválido.")

    return digits


def normalize_amount_brl(value: Any) -> str:
    amount_text = normalize_optional_text(value)

    if amount_text is None:
        raise ValueError("Valor da oferta não informado.")

    try:
        amount = Decimal(
            amount_text.replace(".", "").replace(",", ".")
        )
    except InvalidOperation as error:
        raise ValueError("Valor da oferta inválido.") from error

    if not amount.is_finite() or amount < 0:
        raise ValueError("Valor da oferta inválido.")

    return str(amount)


def normalize_sre_offer_record(
    record: dict[str, Any],
) -> dict[str, Any]:
    """Converte um registro bruto da CVM SRE para o contrato do dashboard."""
    request_id = normalize_optional_text(
        record.get("idRequerimento")
    )

    if request_id is None or not request_id.isdecimal():
        raise ValueError("idRequerimento inválido.")

    reference_date = normalize_optional_text(
        record.get("data")
    )

    if reference_date is None:
        raise ValueError("Data da oferta não informada.")

    try:
        date_iso = datetime.strptime(
            reference_date, "%d/%m/%Y"
        ).date().isoformat()
    except ValueError as error:
        raise ValueError("Data da oferta inválida.") from error

    return {
        "source": SOURCE_NAME,
        "offer_key": f"{SOURCE_NAME}:{request_id}",
        "request_id": request_id,
        "issuer": {
            "name": normalize_optional_text(
                record.get("nomeEmissor")
            ),
            "cnpj": normalize_cnpj(
                record.get("cnpjEmissor")
            ),
        },
        "lead_coordinator": {
            "name": normalize_optional_text(
                record.get("nomeCoordenadorLider")
            ),
            "cnpj": normalize_cnpj(
                record.get("cnpjCoordenadorLider")
            ),
        },
        "security_type": normalize_optional_text(
            record.get("nomeValorMobiliario")
        ),
        "offer_type": normalize_optional_text(
            record.get("tipoDeOferta")
        ),
        "status": normalize_optional_text(
            record.get("statusDaOferta")
        ),
        "amount_brl": normalize_amount_brl(
            record.get("valorTotalEmReais")
        ),
        "reference_date": date_iso,
        "protocol_number": normalize_optional_text(
            record.get("numeroProtocolo")
        ),
        "process_number": normalize_optional_text(
            record.get("numeroProcesso")
        ),
        "registration_number": normalize_optional_text(
            record.get("numeroRegistro")
        ),
        "cvm_url": f"{CVM_OFFER_URL}{request_id}",
    }