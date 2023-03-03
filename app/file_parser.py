import datetime as dt
import logging
import operator
from logging.config import fileConfig
from typing import Sequence

import numpy as np
import pandas as pd
from aiogram import Bot
from aiohttp import ClientSession
from yadisk_async import YaDisk
from yadisk_async.exceptions import UnauthorizedError

from . import settings
from .utils import (
    MsgProvider,
    get_current_date,
    set_inline_button,
    timestamp_to_datetime_string,
)

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


async def download_file_from_yadisk(
    disk: YaDisk,
    source_path: str,
    output_file: str,
) -> bool:
    """
    Asynchronously download file from Yandex.Disk.
    In case of failure send a message to request starter.
    """
    downloaded = False
    try:
        await disk.download(source_path, output_file)
    except Exception as e:
        error_message = f"YaDisk file download FAILURE!: {e}"
        logger.error(error_message)
        raise
    else:
        downloaded = True
        logger.info("YaDisk file download SUCCESS!")
    finally:
        await disk.close()
        return downloaded


def excel_to_pd_dataframe(
    file_path: str, columns: Sequence = None
) -> pd.DataFrame:
    """Translate excel file into pandas dataframe."""
    try:
        df = pd.DataFrame(pd.read_excel(file_path), columns=columns)
    except FileNotFoundError as e:
        logger.error(f"Pandas could not find bday file: {e}")
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error occur while parsing file with pandas: {e}"
        )
        raise
    return df


birthday_schema = {
    "Дата": {"cond": {"type": int, "gt": 0, "lt": 32}},
    "месяц": {"cond": {"type": str, "ne": "?"}},
    "ФИО": {"cond": {"type": str, "ne": "?"}},
}


def validate_df_row(data, schema) -> bool:
    for field in schema:
        value_to_validate = getattr(data, field, None)
        if value_to_validate:
            for attr, expected_value in schema[field]["cond"].items():
                if attr == "type":
                    validated = type(value_to_validate) == expected_value
                elif attr == "call":
                    f, target_res = expected_value
                    validated = f(value_to_validate) == target_res
                else:
                    operation = getattr(operator, attr, None)
                    if operation is None:
                        continue
                    validated = operation(value_to_validate, expected_value)

                if not validated:
                    return False
    return True


def preprocess_pd_dataframe(
    df: pd.DataFrame, validation_schema, columns: Sequence
) -> "zip":
    """
    Validate dataframe rows, drop invalid.
    Returns zip generator with given columns.
    """
    #### refactor this
    # df.replace(np.nan, "?", inplace=True)

    for i in df.index:
        if not validate_df_row(df.loc[i], validation_schema):
            df.drop(i, inplace=True)
    return zip(*(getattr(df, col) for col in columns))


def to_int_month(month: str) -> int:
    """Return an integer mapping to a month."""
    months = {
        "январь": 1,
        "февраль": 2,
        "март": 3,
        "апрель": 4,
        "май": 5,
        "июнь": 6,
        "июль": 7,
        "август": 8,
        "сентябрь": 9,
        "октябрь": 10,
        "ноябрь": 11,
        "декабрь": 12,
    }
    return months.get(month)


def decline_month(month: str) -> str:
    """Return month name in the right declension in Russian."""
    if month.endswith("т"):
        return month + "а"
    return month[:-1] + "я"


def get_formatted_bday_message(
    today: dt.date = None,
    **data: dict,
) -> str:
    """Return a formatted info message."""
    today = today or dt.date.today()
    name = data.get("name", "Неизвестный партнер")
    day = data.get("day")
    month = data.get("month")
    month = decline_month(month)
    return "\n" + f"{name}, {day} {month}"


def collect_bdays(
    path_to_excel: str,
    today: dt.date,
    columns: Sequence = None,
    future_scope: int = None,
    validation_schema=birthday_schema,
) -> list[str]:
    columns = settings.COLUMNS or columns
    future_scope = settings.FUTURE_SCOPE or future_scope
    today_notifications = []
    future_notifications = []
    result = []
    try:
    df = excel_to_pd_dataframe(path_to_excel, columns)
    logger.info("Excel convert to dataframe [SUCCESS]")
    extracted_cols = preprocess_pd_dataframe(df, validation_schema, columns)
    for row in extracted_cols:
        day, month, name = row
        month = month.lower().strip()
        name = name.strip()
        try:
            birth_date = dt.date(today.year, to_int_month(month), day)
        except TypeError as e:
            logger.error(
                f"date conversion failure: {e}" f"; scipped row for {name}"
            )
            continue
        if birth_date == today:
            today_notifications.append(
                get_formatted_bday_message(
                    today, day=day, month=month, name=name
                )
            )
        elif (
            dt.timedelta(days=1)
            <= birth_date - today
            <= dt.timedelta(days=future_scope)
        ):
            future_notifications.append(
                get_formatted_bday_message(
                    today, day=day, month=month, name=name
                )
            )
    if today_notifications:
        result.append(f"#деньрождения сегодня {''.join(today_notifications)}")
        logger.info("TODAY BDAYS message sent successfuly")
    if future_notifications:
        result.append(
            f"#деньрождения ближайшие {settings.FUTURE_SCOPE} дня: {''.join(future_notifications)}"
        )
        logger.info("FUTURE BDAYS message sent successfuly")
    if not (today_notifications or future_notifications):
        result.append(
            f"на сегодня и ближайшие {settings.FUTURE_SCOPE} дня #деньрождения не найдены."
        )
        logger.info("NO BDAYS collected")
    return result


preloaded_data = []


async def preload_mailing_notifications(bot: Bot, session: ClientSession):
    today = await get_current_date(session, settings.TIME_API_URL)
    downloaded_from_yadisk = False
    try:
        downloaded_from_yadisk = await download_file_from_yadisk()
    except Exception as e:
        if isinstance(e, UnauthorizedError):
            kbd = set_inline_button(
                text="Получить код", callback_data="confirm_code"
            )
            await bot.send_message(
                settings.BOT_MANAGER_TELEGRAM_ID,
                "Токен безопасности Яндекс Диска устарел.\n"
                "Для получения кода подвтерждения нажмите на кнопку ниже и "
                "перейдите по ссылке.\nВ открывшейся вкладке браузера войдите в "
                "Яндекс аккаунт, на котором хранится Excel файл с данными о днях "
                "рождениях. После этого вы автоматически перейдете на страницу "
                "получения кода подвтерждения. Скопируйте этот код и отправьте его "
                "боту с командой /code.",
                reply_markup=kbd,
            )
            logger.error(
                "Could not download file from YaDisk - token expired!"
            )
        else:
            logger.error(f"Unexpected YaDisk error: {e}")

    output_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
    if not download_file_from_yadisk:
        if output_file.is_file():
            updated_at = timestamp_to_datetime_string(
                output_file.stat().st_mtime
            )
            warning_message = (
                "Загрузка актуальных данных в настоящее время невозможна."
                f"Данные актуальны на: {updated_at}"
            )
            preloaded_data.append(warning_message)
            logger.warning(
                f"File with bdays was not updated. Used older version: {updated_at}"
            )
        else:
            await bot.send_message(
                settings.BOT_MANAGER_TELEGRAM_ID,
                (
                    "Файл с перечнем дней рождения партнеров не найден в системе."
                    "Отправка уведомлений получателям невозможна."
                    "Обратитесь к разработчику."
                ),
            )
            logger.error("No file with bdays found!")
            return
    notifications = await collect_bdays()
    preloaded_data.append(*notifications)
