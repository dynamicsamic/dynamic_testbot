import logging
from logging.config import fileConfig

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
