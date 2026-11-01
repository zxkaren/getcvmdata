
from financial_data.config.query_period import get_query_period
from financial_data.pipelines.cvm_sre.pipeline import (
    run_cvm_sre_pipeline,
)


def run_data_pipelines() -> None:
    """Executa os pipelines com um período global compartilhado."""
    start_date, end_date = get_query_period()

    run_cvm_sre_pipeline(start_date, end_date)
