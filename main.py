import logging

from financial_data.jobs.scheduler import (
    start_data_pipeline_scheduler,
)


def main() -> None:
    """Inicializa a aplicação e seu agendador."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    start_data_pipeline_scheduler()


if __name__ == "__main__":
    main()
