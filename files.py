import datetime as dt
import logging
import operator
from logging.config import fileConfig
from typing import Sequence

import numpy as np
import pandas as pd
from aiogram import types
from aiohttp import ClientSession
from yadisk_async import YaDisk

import settings
from utils import get_current_date

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


async def get_file_from_yadisk(
    message: types.Message,
    disk: YaDisk,
    source_path: str,
    output_file: str,
) -> bool:
    """
    Asynchronously download file from Yandex.Disk.
    In case of failure send a message to request starter.
    """
    downloaded = True
    try:
        await disk.download(source_path, output_file)
        logger.info("YaDisk file download SUCCESS!")
    except Exception as e:
        error_message = f"YaDisk file download FAILURE!: {e}"
        logger.error(error_message)
        downloaded = False
        await message.answer(
            "При скачивании файла произошла ошибка.\n"
            "Обратитесь к разработчику"
        )
    await disk.close()
    return downloaded


def excel_to_pd_dataframe(
    message: types.Message, file_path: str, columns: Sequence = None
) -> pd.DataFrame:
    """Translate excel file into pandas dataframe."""
    try:
        df = pd.DataFrame(pd.read_excel(file_path), columns=columns)
    except FileNotFoundError as e:
        logger.error(f"Pandas could not find bday file: {e}")
        message.answer(
            "Файл не найден.\nВозможно, произошла проблема со скачиванием файла.\nПожалуйста, обратитесь к разработчику."
        )
    except Exception as e:
        logger.error(
            f"Unexpected error occur while parsing file with pandas: {e}"
        )
        message.answer(
            "В процессе обработки файла произошла ошибка.\nПожалуйста, обратитесь к разработчику."
        )
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
    df.replace(np.nan, "?", inplace=True)

    for i in df.index:
        if not validate_df_row(df.loc[i], validation_schema):
            df.drop(i, inplace=True)
    return zip(*(getattr(df, col) for col in columns))


def to_int_month(month: str) -> int:
    """Return an integer mapping to a month."""
    return {
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
    }.get(month)


async def collect_bdays(
    message: types.Message,
    session: ClientSession,
    path_to_excel: str,
    validation_schema=birthday_schema,
    columns=("Дата", "месяц", "год", "ФИО"),
    future_scope: int = 3,
):
    today_notifications = []
    future_notifications = []
    today = await get_current_date(session, settings.TIME_API_URL)
    df = excel_to_pd_dataframe(message, path_to_excel, columns)
    extracted_cols = preprocess_pd_dataframe(df, validation_schema, columns)
    for row in extracted_cols:
        day, month, year, name = row
        try:
            birth_date = dt.date(today.year, to_int_month(month), day)
        except TypeError as e:
            logger.error(
                f"date conversion failure: {e}" f"scipped row for {name}"
            )
            continue
        if birth_date == today:
            today_notifications.append(birth_date)
        elif (
            dt.timedelta(days=1)
            <= birth_date - today
            <= dt.timedelta(days=future_scope)
        ):
            future_notifications.append(birth_date)
    if today_notifications:
        await message.answer(today_notifications)
    if future_notifications:
        await message.answer(future_notifications)


df = pd.DataFrame()

for i in df.index:
    if not validate_df_row(df.loc[i]):
        df.drop(i, inplace=True)
