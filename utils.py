import datetime as dt
import logging
from logging.config import fileConfig

import aiohttp

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
TIME_API_URL = "http://worldtimeapi.org/api/timezone/Europe/Moscow"


def update_envar(path, varname: str, value: str) -> bool:
    with open(path) as f:
        contents = f.readlines()

    for idx, line in enumerate(contents):
        if line.startswith(varname):
            contents.pop(idx)
            contents.append(f"{varname} = {value}")

    with open(path, "w") as f:
        wrote = f.write("".join(contents))
    return wrote > 0


async def get_current_date(
    session: aiohttp.ClientSession, url: str
) -> dt.date:
    """
    Try to fetch current date from external API.
    Use system date if fails.
    """
    today = dt.date.today()
    try:
        async with session.get(url) as response:
            resp_data = await response.json()
            logger.info(f"Получен ответ от стороннего API: {url}.")
    except Exception:
        logger.warning(
            "Не удалось получить ответ от стороннего API. "
            "Текущая дата будет задана операционной системой."
        )
        return today

    if datetime := resp_data.get("datetime"):
        # `datetime` is <str> with format `2022-12-15T00:03:42.431581+03:00`
        # we need only first 10 chars to create a date
        try:
            today = dt.date.fromisoformat(datetime[:10])
        except (TypeError, ValueError):
            logger.warning(
                "Не удалось преобразовать ответ стороннего API в дату. "
                "Формат ответа был изменен."
            )
    return today
