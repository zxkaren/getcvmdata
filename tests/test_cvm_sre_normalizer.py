import pytest

from financial_data.processing.cvm_sre.normalizer import (
    normalize_sre_offer_record,
)


@pytest.fixture
def sample_record() -> dict[str, str]:
    return {
        "idRequerimento": "28562",
        "nomeEmissor": "Empresa Teste S.A.",
        "cnpjEmissor": "62.550.985/0001-86",
        "nomeCoordenadorLider": "Banco Teste S.A.",
        "cnpjCoordenadorLider": "16.695.922/0001-09",
        "nomeValorMobiliario": "Debêntures",
        "tipoDeOferta": "PRIMARIA",
        "statusDaOferta": "Registro Concedido",
        "valorTotalEmReais": "2.697.000,0400",
        "data": "30/10/2026",
        "numeroProtocolo": "3561/2026",
        "numeroProcesso": "SRE/3561/2026",
    }


def test_normalize_sre_offer_record(
    sample_record: dict[str, str],
) -> None:
    normalized = normalize_sre_offer_record(sample_record)

    assert normalized["offer_key"] == "cvm_sre:28562"
    assert normalized["request_id"] == "28562"
    assert normalized["issuer"]["cnpj"] == "62550985000186"
    assert normalized["amount_brl"] == "2697000.0400"
    assert normalized["reference_date"] == "2026-10-30"
    assert normalized["registration_number"] is None
    assert normalized["cvm_url"] == (
        "https://web.cvm.gov.br/sre-publico-cvm/"
        "#/oferta-publica/28562"
    )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("idRequerimento", "ABC"),
        ("valorTotalEmReais", "inválido"),
        ("data", "31/02/2026"),
        ("cnpjEmissor", "123"),
    ],
)
def test_normalize_sre_offer_rejects_invalid_data(
    sample_record: dict[str, str],
    field_name: str,
    invalid_value: str,
) -> None:
    sample_record[field_name] = invalid_value

    with pytest.raises(ValueError):
        normalize_sre_offer_record(sample_record)