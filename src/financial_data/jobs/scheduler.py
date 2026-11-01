
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from decouple import config

from financial_data.application.orchestrator import (
    run_data_pipelines,
)


logger = logging.getLogger(__name__)


def run_scheduled_data_pipelines() -> None:
    """Executa os pipelines registrados no orquestrador."""
    try:
        logger.info("Iniciando execução dos pipelines.")
        run_data_pipelines()
        logger.info("Execução dos pipelines concluída.")
    except Exception:
        logger.exception("Erro durante a execução dos pipelines.")
        raise


def start_data_pipeline_scheduler() -> None:
    """Inicia o agendamento conforme as configurações do ambiente."""
    cron_expression = config("CRON_EXPRESSION").strip()
    scheduler_timezone = config("SCHEDULER_TIMEZONE").strip()

    if not cron_expression:
        raise ValueError("CRON_EXPRESSION não pode estar vazia.")

    if not scheduler_timezone:
        raise ValueError("SCHEDULER_TIMEZONE não pode estar vazia.")

    timezone = ZoneInfo(scheduler_timezone)

    cron_trigger = CronTrigger.from_crontab(
        cron_expression,
        timezone=timezone,
    )

    scheduler = BlockingScheduler(timezone=timezone)

    scheduler.add_job(
        func=run_scheduled_data_pipelines,
        trigger=cron_trigger,
        id="data_pipelines",
        replace_existing=True,
        max_instances=1,
    )

    logger.info(
        "Agendador configurado: %s | Fuso: %s",
        cron_expression,
        scheduler_timezone,
    )

    try:
        run_scheduled_data_pipelines()
    except Exception:
        logger.warning(
            "A execução inicial falhou. "
            "O agendador continuará ativo."
        )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Agendador finalizado manualmente.")
