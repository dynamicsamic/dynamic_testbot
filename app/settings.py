from pathlib import Path

from decouple import config
from pytz import timezone

BASE_DIR = Path(__name__).resolve().parent

BOT_TOKEN = config("BOT_TOKEN")

YADISK_TOKEN = config("YADISK_TOKEN")
YADISK_TOKEN_TEST = config("YADISK_TOKEN_TEST")
YANDEX_APP_ID = config("YANDEX_APP_ID")
YANDEX_SECRET_CLIENT = config("YANDEX_SECRET_CLIENT")
YADISK_FILEPATH = "disk:/b_day/b_days.xlsx"

OUTPUT_FILE_NAME = "temp.xlsx"
TIME_API_URL = "http://worldtimeapi.org/api/timezone/Europe/Moscow"

COLUMNS = ("Дата", "месяц", "ФИО")
FUTURE_SCOPE = 3
TIME_ZONE = timezone("Europe/Moscow")

DEBUG = True

DB = {
    "app": {"engine": "sqlite", "driver": "", "name": config("APP_DB_NAME")},
    "jobstore": {
        "engine": "sqlite",
        "driver": "",
        "name": config("JOBSTORE_DB_NAME"),
    },
}
