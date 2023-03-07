import asyncio
import datetime as dt
import logging
import operator
from logging.config import fileConfig
from typing import Sequence

import numpy as np
import pandas as pd
from aiogram import Bot
from aiohttp import ClientSession
from yadisk_async.exceptions import UnauthorizedError

from app.yandex_disk import disk, download_file_from_yadisk

from . import settings
from .utils import (
    MsgProvider,
    find_bot,
    get_bot,
    get_current_date,
    set_inline_button,
    timestamp_to_datetime_string,
)

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def excel_to_pd_dataframe(
    file_path: str, columns: Sequence = None
) -> pd.DataFrame:
    """Translate excel file into pandas dataframe."""
    try:
        df = pd.DataFrame(
            pd.read_excel(file_path, engine="openpyxl"), columns=columns
        )
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
        # logger.info("TODAY BDAYS message sent successfuly")
    if future_notifications:
        result.append(
            f"#деньрождения ближайшие {settings.FUTURE_SCOPE} дня: {''.join(future_notifications)}"
        )
        # logger.info("FUTURE BDAYS message sent successfuly")  # переделать логи
    if not (today_notifications or future_notifications):
        result.append(
            f"на сегодня и ближайшие {settings.FUTURE_SCOPE} дня #деньрождения не найдены."
        )
        # logger.info("NO BDAYS collected")
    return result


async def preload_mailing_notifications(
    bot: Bot, check_yadisk_token: bool = True  # session: ClientSession
) -> None:
    async with ClientSession() as session:
        today = await get_current_date(session, settings.TIME_API_URL)

    global preloaded_data
    warning_message = None
    # preloaded_data.clear()
    output_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
    # downloaded_from_yadisk = False
    # if not await disk.check_token():
    #     kbd = set_inline_button(
    #         text="Получить код", callback_data="confirm_code"
    #     )
    #     await bot.send_message(
    #         settings.BOT_MANAGER_TELEGRAM_ID,
    #         "Токен безопасности Яндекс Диска устарел.\n"
    #         "Для получения кода подвтерждения нажмите на кнопку ниже и "
    #         "перейдите по ссылке.\nВ открывшейся вкладке браузера войдите в "
    #         "Яндекс аккаунт, на котором хранится Excel файл с данными о днях "
    #         "рождениях. После этого вы автоматически перейдете на страницу "
    #         "получения кода подвтерждения. Скопируйте этот код и отправьте его "
    #         "боту с командой /code.",
    #         reply_markup=kbd,
    #     )
    #     logger.error("Could not download file from YaDisk - token expired!")
    if check_yadisk_token:
        try:
            await download_file_from_yadisk(
                settings.YADISK_FILEPATH, output_file.as_posix()
            )
        except Exception as e:
            logger.error(f"Unexpected YaDisk error: {e}")
    else:
        if output_file.is_file():
            updated_at = timestamp_to_datetime_string(
                output_file.stat().st_mtime
            )
            warning_message = (
                "Загрузка актуальных данных в настоящее время невозможна."
                f"Данные актуальны на: {updated_at}"
            )
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
            logger.critical("No file with bdays found!")
            return
    try:
        notifications = collect_bdays(
            output_file.as_posix(), today, settings.COLUMNS, 3
        )
    except Exception as e:
        await bot.send_message(
            settings.BOT_MANAGER_TELEGRAM_ID,
            "При обработке файла с перечнем дней рождения произошла ошибка. Обратитесь к разработчику.",
        )
        logger.critical(f"File processing failed with error: {e}")
        return

    preloaded_data.clear()  # clear all previous notifications if we have new ones
    if warning_message:
        preloaded_data.append(warning_message)
    preloaded_data += notifications


async def run_preload(bot_path: str) -> None:
    bot = find_bot(bot_path)
    if not (check_yadisk_token := await disk.check_token()):
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
        logger.error("Could not download file from YaDisk - token expired!")
    await preload_mailing_notifications(bot, check_yadisk_token)


async def dispatch_message_to_chat(bot_path: str, chat_id: int) -> None:
    if not preloaded_data:
        await run_preload(bot_path)

    await asyncio.sleep(5)

    bot = find_bot(bot_path)
    for message in preloaded_data:
        await bot.send_message(chat_id, message)


from sqlalchemy.exc import SQLAlchemyError


async def update_db_from_yadisk():
    try:
        await download_file_from_yadisk(
            settings.YADISK_FILEPATH, output_file.as_posix()
        )
    except Exception as e:
        logger.error(f"Unexpected YaDisk error: {e}")
        raise
    df = excel_to_pd_dataframe()
    extracted_cols = preprocess_pd_dataframe(df)
    with Session() as session:
        for row in extracted_cols:
            day, month, name = row
            month = month.lower().strip()
            name = name.strip()
            try:
                birth_date = dt.date(today.year, to_int_month(month), day)
            except TypeError as e:
                logger.error(
                    f"date conversion failure: {e}; " f"scipped row for {name}"
                )
                continue
            bday = Birthday(name=name, date=birth_date)
            session.add(bday)
            try:
                session.commit()
            except SQLAlchemyError as e:
                logger.error(
                    f"Insert birthday instance to DB error: {e}; "
                    f"Insert skipped for {name}"
                )


class NotificationStorage(dict):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


Storage = NotificationStorage()
bot = get_bot()


async def preload_notifications():
    yadisk_token_valid = await disk.check_token()
    bot = find_bot()
    if not yadisk_token_valid:
        await bot.send_message(
            settings.BOT_MANAGER_TELEGRAM_ID, text="callback"
        )
        logger.warning("YaDisk token expired => file download skipped.")
    else:
        try:
            await update_db_from_yadisk()
        except Exception as e:
            await bot.send_message(
                settings.BOT_MANAGER_TELEGRAM_ID, text="Unexpected error"
            )
            logger.critical(f"Database update failure: {e}")
            Storage.setdefault(
                "warning",
                "Не удалось обновить базу данных. данные актуальны на ...",
            )
        else:
            Storage.pop("warning", None)
    with Session() as session:
        today_birthdays = Birthday.today()
        today_message = get_formatted_bday_message(today_birthdays)
        Storage.setdefault("today", today_message)

        future_birthdays = Birthday.future()
        future_message = get_formatted_bday_message(future_birthdays)
        Storage.setdefault("future", future_message)


async def dispatch_message_to_chat2(bot_path: str, chat_id: int) -> None:
    bot = find_bot(bot_path)
    for message in Storage.values():
        await bot.send_message(chat_id, message)
