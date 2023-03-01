import asyncio
import datetime as dt
from typing import Any

from aiohttp import client_exceptions as aiohttp_errors
from yadisk_async import YaDisk
from yadisk_async import exceptions as yadisk_errors

from app import settings
from app.utils import timestamp_to_datetime_string

fake_token = "faketoken"
file_download_args = ("disk:/b_day/b_days.xlsx", "hello.xlsx")

disk = YaDisk(token=fake_token)


def get_class_name(obj: object) -> str:
    return obj.__class__.__name__


preloaded_data = []


async def download_file_from_yadisk(
    source_path: str, output_file: str
) -> dict[str, Any]:
    response = {"result": False, "error": None}

    try:
        await disk.download(source_path, output_file)
    except Exception as e:
        response["error"] = e
    else:
        response["result"] = True
    finally:
        await disk.close()
        return response


bot = list()  # delete this stupid thing


async def preload_mailing_notifications():
    result, error = await download_file_from_yadisk()
    if not result:
        if issubclass(error.__class__, yadisk_errors.YaDiskError):
            # if error == get_class_name(yadisk_errors.UnauthorizedError()):
            await bot.send_message("to_manager", "generateNewToken?")
        # elif issubclass(error.__class__, aiohttp_errors.ClientConnectionError):
        # elif error == get_class_name(aiohttp_errors.ClientConnectorError()):
        # await bot.send_message("to_manager", "badConnection")
        else:
            await bot.send_message("something went wrong")

        output_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
        if output_file.is_file():
            updated_at = timestamp_to_datetime_string(
                output_file.stat().st_mtime
            )
            warning_message = (
                "Загрузка актуальных данных в настоящее время невозможна."
                f"Данные актуальны на: {updated_at}"
            )
            preloaded_data.append(warning_message)
        else:
            await bot.send_message(
                "to_manager", "no file available. need help"
            )
            return


def preload_birthday_data() -> tuple:
    """Store formatted messages in tuple"""
    download_file_from_yadisk
    notify_if_yadisk_failure
    # process excel file
    # store data in global tuple
    pass


import datetime as dt


def get_excel_file(
    yadisk_result: bool, refresh_time: dict[str, int] = {"minutes": 10}
):
    try:
        delta = dt.timedelta(**refresh_time)
    except TypeError:
        delta = dt.timedelta(minutes=10)
    # if file was created before refresh_time get new file
    if yadisk_result:
        pass

    if not yadisk_result:
        # find_the_file()
        pass


asyncio.run(download_file_from_yadisk())
