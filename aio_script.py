import logging
import sys
from logging.config import fileConfig
from pathlib import Path

import yadisk_async
from aiogram import Bot, Dispatcher, executor, types
from decouple import config

BASE_DIR = Path(__name__).resolve().parent

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
import json

BOT_TOKEN = config("BOT_TOKEN")
YADISK_TOKEN = config("YADISK_TOKEN")
YADISK_FILEPATH = "disk:/b_day/b_days.xlsx"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)


async def get_file_from_yadisk(bot: Bot, token: str, path: str) -> str:
    disk = yadisk_async.YaDisk(token=token)
    file_name = "temp.xlsx"
    if not await disk.check_token():
        # add later: generate new token
        error_message = "Invalid yadisk token"
        logger.error(error_message)
        await bot.send_message(text=error_message)
        sys.exit(error_message)
    try:
        await disk.download(
            src_path=path, path_or_file=str(BASE_DIR / file_name)
        )
        logger.info("Yadisk file download SUCCESS!")
    except Exception as e:
        error_message = f"Yadisk file download failure: {e}"
        logger.error(error_message)
        await bot.send_message(text=error_message)
        sys.exit(error_message)
    return file_name


async def set_bot_commands(bot: Bot):
    commands = [
        types.BotCommand("help", "помощь"),
        types.BotCommand("menu", "список всех комманд"),
    ]
    await bot.set_my_commands(commands)


async def on_startup(dp: Dispatcher):
    await set_bot_commands(dp.bot)


@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.reply("hefdfdf")


@dp.message_handler(commands=["help"])
async def help(message: types.Message):
    messages = await dp.bot.get_my_commands()
    await message.answer(messages)


@dp.message_handler(commands=["bdays"])
async def get_bdays(message: types.Message):
    results = await get_file_from_yadisk(bot, YADISK_TOKEN, YADISK_FILEPATH)
    await message.answer(results)


@dp.message_handler(commands=["joke"])
async def send_joke():
    pass


async def main():
    send_scheduled_msg = False


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
