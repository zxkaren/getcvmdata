
import json
from pathlib import Path

import pytest

import financial_data.application.publish_dashboard_data as publisher
from financial_data.application.publish_dashboard_data import (
    publish_dashboard_data,
)


@pytest.fixture
def consolidated_source(tmp_path: Path) -> Path:
    source_file = tmp_path / "offers_current.json"

    content = {
        "metadata": {
            "source": "cvm_sre",
            "schema_version": 1,
            "last_source_execution_at": "2026-11-01T21:59:09",
            "total_offers": 1,
        },
        "offers": [
            {
                "source": "cvm_sre",
                "offer_key": "cvm_sre:28562",
                "request_id": "28562",
                "issuer": {
                    "name": "Emissor Teste",
                    "cnpj": "62550985000186",
                },
                "lead_coordinator": {
                    "name": "Coordenador Teste",
                    "cnpj": "16695922000109",
                },
                "security_type": "Cotas de FIDC",
                "offer_type": "PRIMARIA",
                "status": "Registro Concedido",
                "amount_brl": "2697000.0400",
                "reference_date": "2026-10-30",
                "cvm_url": (
                    "https://web.cvm.gov.br/sre-publico-cvm/"
                    "#/oferta-publica/28562"
                ),
            }
        ],
    }

    source_file.write_text(
        json.dumps(content),
        encoding="utf-8",
    )

    return source_file


def test_publish_dashboard_data(
    consolidated_source: Path,
    tmp_path: Path,
) -> None:
    target_directory = tmp_path / "dashboard" / "v1"

    offers_file, overview_file, manifest_file = publish_dashboard_data(
        consolidated_source,
        target_directory,
    )

    offers_data = json.loads(
        offers_file.read_text(encoding="utf-8")
    )

    overview_data = json.loads(
        overview_file.read_text(encoding="utf-8")
    )

    manifest_data = json.loads(
        manifest_file.read_text(encoding="utf-8")
    )

    assert offers_data["metadata"]["total_offers"] == 1
    assert offers_data["offers"][0]["request_id"] == "28562"

    assert overview_data["metadata"]["total_offers"] == 1
    assert overview_data["overview"]["total_offers"] == 1
    assert overview_data["overview"]["total_amount_brl"] == "2697000.0400"
    assert overview_data["overview"]["distinct_issuers"] == 1

    assert manifest_data["schema_version"] == 1
    assert manifest_data["datasets"] == {
        "offers": "offers.json",
        "overview": "overview.json",
    }

    generation_id = manifest_data["generation_id"]

    assert isinstance(generation_id, str)
    assert len(generation_id) == 32

    assert (
        offers_data["metadata"]["generation_id"]
        == generation_id
    )

    assert (
        overview_data["metadata"]["generation_id"]
        == generation_id
    )


def test_publish_rejects_duplicate_offers(
    consolidated_source: Path,
    tmp_path: Path,
) -> None:
    content = json.loads(
        consolidated_source.read_text(encoding="utf-8")
    )

    content["offers"].append(content["offers"][0].copy())
    content["metadata"]["total_offers"] = 2

    consolidated_source.write_text(
        json.dumps(content),
        encoding="utf-8",
    )

    target_directory = tmp_path / "dashboard" / "v1"

    with pytest.raises(ValueError, match="Oferta duplicada"):
        publish_dashboard_data(
            consolidated_source,
            target_directory,
        )

    assert not (target_directory / "offers.json").exists()
    assert not (target_directory / "overview.json").exists()
    assert not (target_directory / "manifest.json").exists()


def test_invalid_amount_preserves_previous_publication(
    consolidated_source: Path,
    tmp_path: Path,
) -> None:
    target_directory = tmp_path / "dashboard" / "v1"

    published_files = publish_dashboard_data(
        consolidated_source,
        target_directory,
    )

    original_files = {
        file.name: file.read_bytes()
        for file in published_files
    }

    content = json.loads(
        consolidated_source.read_text(encoding="utf-8")
    )

    content["offers"][0]["amount_brl"] = "valor inválido"

    consolidated_source.write_text(
        json.dumps(content),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Valor financeiro inválido",
    ):
        publish_dashboard_data(
            consolidated_source,
            target_directory,
        )

    for file in published_files:
        assert file.read_bytes() == original_files[file.name]


def test_publish_writes_manifest_last(
    consolidated_source: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_save = publisher.save_json_object_atomically
    written_files: list[str] = []

    def record_save(file_path: Path, content: dict) -> None:
        written_files.append(file_path.name)
        original_save(file_path, content)

    monkeypatch.setattr(
        publisher,
        "save_json_object_atomically",
        record_save,
    )

    publish_dashboard_data(
        consolidated_source,
        tmp_path / "dashboard" / "v1",
    )

    assert written_files == [
        "offers.json",
        "overview.json",
        "manifest.json",
    ]
