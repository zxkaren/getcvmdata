import logging
from decouple import config
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from jobs.getcvmdata import task


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


def run_task() -> None:
    """
    Executa a tarefa principal do projeto.
    """
    try:
        logger.info("Iniciando execução da task.")
        task()
        logger.info("Execução da task finalizada com sucesso.")
    except Exception as exc:
        logger.exception("Erro ao executar a task: %s", exc)


def start_scheduler() -> None:
    """
    Inicia o scheduler e agenda a task com base na expressão CRON.
    Padrão: a cada minuto (* * * * *).
    """
    cron_expression = config("CRON_EXPRESSION", default="* * * * *")
    scheduler = BlockingScheduler()

    scheduler.add_job(
        func=run_task,
        trigger=CronTrigger.from_crontab(cron_expression),
        id="getcvmdata_task",
        replace_existing=True,
        max_instances=1
    )

    logger.info("Scheduler iniciado com CRON_EXPRESSION=%s", cron_expression)

    # Executa uma vez imediatamente ao subir a aplicação
    run_task()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler finalizado manualmente.")