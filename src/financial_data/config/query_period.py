from datetime import date

from dateutil.relativedelta import relativedelta
from decouple import config


LOOKBACK_MONTHS = config("LOOKBACK_MONTHS", cast=int)


def get_query_period() -> tuple[date, date]:
    """Calcula o período global de consulta configurado no ambiente."""
    if LOOKBACK_MONTHS < 0:
        raise ValueError("LOOKBACK_MONTHS não pode ser negativo.")

    end_date = date.today()
    start_date = end_date - relativedelta(months=LOOKBACK_MONTHS)

    return start_date, end_date
