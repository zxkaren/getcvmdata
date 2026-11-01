
import csv
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

from financial_data.pipelines.cvm_sre import pipeline


def test_cvm_sre_pipeline_exports_json_and_csv(
    tmp_path: Path,
) -> None:
    """Valida a coleta simulada e a geração dos arquivos SRE."""
    records = [
        {
            "nomeEmissor": "Empresa Teste",
            "cnpjEmissor": "12345678000190",
        },
        {
            "nomeEmissor": "Outra Empresa",
            "cnpjEmissor": "98765432000190",
        },
    ]

    with (
        patch.object(
            pipeline,
            "get_target_directory",
            return_value=tmp_path,
        ),
        patch.object(pipeline.requests, "Session") as session_factory,
    ):
        session = session_factory.return_value.__enter__.return_value
        response = session.post.return_value

        response.json.return_value = {
            "registros": records,
            "totalRegistros": len(records),
        }

        pipeline.run_cvm_sre_pipeline(
            date(2026, 1, 1),
            date(2026, 6, 30),
        )

        session.post.assert_called_once()
        response.raise_for_status.assert_called_once()

        request_payload = session.post.call_args.kwargs["json"]

        assert request_payload["periodoCriacaoProcesso"] == {
            "de": "01/01/2026",
            "ate": "30/06/2026",
        }

    json_files = list(tmp_path.glob("cvm_sre_ofertas_*.json"))
    csv_files = list(tmp_path.glob("cvm_sre_ofertas_*.csv"))

    assert len(json_files) == 1
    assert len(csv_files) == 1

    json_payload = json.loads(
        json_files[0].read_text(encoding="utf-8")
    )

    assert json_payload["registros"] == records
    assert (
        json_payload["metadata"]["total_registros_coletados"]
        == 2
    )
    assert (
        json_payload["metadata"]["periodo_consulta"]["de"]
        == "01/01/2026"
    )

    with csv_files[0].open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as csv_file:
        csv_records = list(csv.DictReader(csv_file))

    assert len(csv_records) == 2
    assert csv_records[0]["nomeEmissor"] == "Empresa Teste"
    assert csv_records[1]["nomeEmissor"] == "Outra Empresa"
