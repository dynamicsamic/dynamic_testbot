import datetime as dt
import logging
from functools import partial
from logging.config import fileConfig

import aiogram
import pytz
from aiogram import types
from aiohttp import ClientSession

from app import settings

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
TIME_API_URL = "http://worldtimeapi.org/api/timezone/Europe/Moscow"


def set_inline_button(**options):
    """Set an inline keyboard with one button."""
    kbd = types.InlineKeyboardMarkup()
    kbd.add(types.InlineKeyboardButton(**options))
    return kbd


def update_envar(path, varname: str, value: str) -> bool:
    with open(path) as f:
        contents = f.readlines()

    for idx, line in enumerate(contents):
        if line.startswith(varname):
            contents.pop(idx)
            contents.append(f"{varname} = {value}")

    with open(path, "w") as f:
        written = f.write("".join(contents))
    return written > 0


async def get_current_date(session: ClientSession, url: str) -> dt.date:
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


def timestamp_to_datetime_string(ts: float) -> str:
    """Convert timestamp to a datetime string."""
    date = dt.datetime.fromtimestamp(ts, tz=settings.TIME_ZONE)
    return f"{date: %d-%m-%Y %H:%M}"


class MsgProvider:
    """Class for abstracting telegram message delivery process."""

    def __init__(
        self, source: types.Message | aiogram.Bot, chat_id: int = None
    ):
        if isinstance(source, types.Message):
            self.sender = source.answer
        elif isinstance(source, aiogram.Bot) and isinstance(chat_id, int):
            self.sender = partial(source.send_message, chat_id=chat_id)
        elif isinstance(source, str):
            self.build_sender_from_string(source, chat_id)
        else:
            pass
        self.source = source

    async def dispatch_text(
        self, text: str = None, *args, **kwargs
    ) -> types.Message:
        """
        Send text message.
        Same as `send_message` for aiogram.Bot or
        `answer` for aiogram.types.Message.
        """
        try:
            return await self.sender(text=text, *args, **kwargs)
        except Exception as e:
            logger.error(f"msg_provider message dispatch error: {e}")

    def build_sender_from_string(self, path: str, chat_id: int = None) -> None:
        """
        Route must follow this pattern:
        `module.submodule: sender_instance`
        """
        import importlib

        module_name, instance = path.split(":", 1)
        module = importlib.import_module(module_name)
        source = getattr(module, instance)
        return self.__init__(source, chat_id)
