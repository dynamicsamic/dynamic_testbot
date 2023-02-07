import logging
import operator
from logging.config import fileConfig
from typing import Sequence

import numpy as np
import pandas as pd
from aiogram import types
from yadisk_async import YaDisk

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


def extract_bdays_from_excel(df: pd.DataFrame) -> "zip":
    """
    Get birthday info from excel file.
    Read excel file. Make some normalization.
    Delete corrupted rows.
    Returns zip generator with given columns.
    """

    df.replace(np.nan, "?", inplace=True)
    for i in df.index:
        if (
            df.loc[i, EXCEL_DAY_KEY] == "?"
            or df.loc[i, EXCEL_MONTH_KEY] == "?"
            or df.loc[i, EXCEL_NAME_KEY] == "?"
        ):
            df.drop(i, inplace=True)
    return zip(df["Дата"], df["месяц"], df["год"], df["ФИО"])


example_schema = [
    {"Дата": {"cond": {"type": int, "gt": 0, "lt": 32}}},
    {"месяц": {"cond": {"type": str, "ne": "?"}}},
    {"ФИО": {"cond": {"type": str, "ne": "?"}}},
]
{}


def validate_line(data, schema):
    validated = []
    for field in schema:
        value = getattr(data, field, None)
        if value:
            for cond, val in schema[field]["cond"].items():
                if cond == "type":
                    res = type(value) == val
                else:
                    operation = getattr(operator, cond, None)
                    res = operation(value, val)
                validated.append(res)


df = pd.DataFrame()
for i in df.index:
    if not validate_line(df.loc[i]):
        df.drop(i, inplace=True)
