import json
from pathlib import Path

import pytest

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
                "status": "Registro Concedido",
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

    offers_file, manifest_file = publish_dashboard_data(
        consolidated_source,
        target_directory,
    )

    offers_data = json.loads(
        offers_file.read_text(encoding="utf-8")
    )

    manifest_data = json.loads(
        manifest_file.read_text(encoding="utf-8")
    )

    assert offers_data["metadata"]["total_offers"] == 1
    assert offers_data["offers"][0]["request_id"] == "28562"

    assert manifest_data["schema_version"] == 1
    assert manifest_data["datasets"]["offers"] == "offers.json"
    assert manifest_data["sources"]["cvm_sre"]["total_offers"] == 1


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

    with pytest.raises(ValueError):
        publish_dashboard_data(
            consolidated_source,
            target_directory,
        )

    assert not (target_directory / "offers.json").exists()
    assert not (target_directory / "manifest.json").exists()