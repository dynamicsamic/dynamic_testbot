import logging
from logging.config import fileConfig

from yadisk_async import YaDisk

from app import settings

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

disk = YaDisk(token=settings.YADISK_TOKEN_TEST)


async def download_file_from_yadisk(
    source_path: str, output_file: str, disk: YaDisk = disk
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
