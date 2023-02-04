import asyncio
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
YADISK_TOKEN_TEST = config("YADISK_TOKEN_TEST")
YADISK_FILEPATH = "disk:/b_day/b_days.xlsx"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
disk = yadisk_async.YaDisk(token=YADISK_TOKEN_TEST)


@dp.message_handler(commands="random")
async def cmd_random(message: types.Message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(
            text="Нажми меня", callback_data="random_value"
        )
    )
    await message.answer(
        "Нажмите на кнопку, чтобы бот отправил число от 1 до 10",
        reply_markup=keyboard,
    )


@dp.callback_query_handler(text="random_value")
async def send_random_value(call: types.CallbackQuery):
    disk = yadisk_async.YaDisk(token=YADISK_TOKEN)
    file_name = "temp.xlsx"
    if await disk.check_token():
        file_name = "passed"

    await disk.download(YADISK_FILEPATH, str(BASE_DIR / file_name))
    await disk.close()
    await call.message.answer(file_name)


async def get_file_from_yadisk(
    message: types.Message, token: str, path: str
) -> str:
    disk = yadisk_async.YaDisk(token=token)
    file_name = "temp.xlsx"
    if not await disk.check_token():
        error_message = "Invalid yadisk token"
        logger.error(error_message)
        disk.id = config("YANDEX_APP_ID")
        disk.secret = config("YANDEX_SECRET_CLIENT")
        url = disk.get_code_url()
        kbd = types.InlineKeyboardMarkup()
        kbd.add(
            types.InlineKeyboardButton(
                text="Получить код", callback_data="cofrm_code"
            )
        )
        await message.answer(
            f"Токен безопасности Яндекс Диска устарел. Для получения нового токена необходим код подтверждения. Для получения кода подвтерждения пройдите по ссылке {url}.\nВойдите в Яндекс аккаунт, на котором хранится Excel файл с данными о днях рождениях. После этого вы автоматически перейдете на страницу получения кода подвтерждения. Скопируйте этот код и отправьте его боту с командой `token`.",
            reply_markup=kbd,
        )
        # await disk.close()
        # await message.answer(error_message)
        # return
    # try:
    #    await disk.download(path, str(BASE_DIR / file_name))
    #    logger.info("Yadisk file download SUCCESS!")
    # except Exception as e:
    #    error_message = f"Yadisk file download failure: {e}"
    #    logger.error(error_message)
    #    await message.answer(error_message)
    #    # sys.exit(error_message)
    await disk.close()
    return file_name


async def set_bot_commands(bot: Bot):
    commands = [
        types.BotCommand("help", "помощь"),
        types.BotCommand("menu", "список всех комманд"),
        types.BotCommand("bdays", "список ближайших дней рождений"),
        types.BotCommand("joke", "пошутить"),
        types.BotCommand("test", "для тестирования"),
    ]
    await bot.set_my_commands(commands)


async def on_startup(dp: Dispatcher):
    await set_bot_commands(dp.bot)


@dp.callback_query_handler(text="cofrm_code")
async def confrm_code(call: types.CallbackQuery):
    await call.message.answer("called")
    await call.answer()


@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.reply("hefdfdf")


@dp.message_handler(commands=["help"])
async def help(message: types.Message):
    messages = await dp.bot.get_my_commands()
    await message.answer(messages)


@dp.message_handler(commands=["bdays"])
async def get_bdays(message: types.Message):
    # 1. check yadisk token
    # 2. if false -> send message to call /token commanf
    # 3. if true -> proceed further
    if not await disk.check_token():
        await message.answer("false")
    else:
        results = await get_file_from_yadisk(
            message, YADISK_TOKEN_TEST, YADISK_FILEPATH
        )
        await message.answer(results)


@dp.message_handler(commands=["joke"])
async def send_joke():
    pass


async def main():
    send_scheduled_msg = False


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
