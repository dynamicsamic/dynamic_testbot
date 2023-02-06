import logging
from logging.config import fileConfig

from aiogram import Bot, Dispatcher, executor, types

import settings
from handlers import register_bdays_handlers, register_common_handlers

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

bot = Bot(token=settings.BOT_TOKEN)
dp = Dispatcher(bot)


async def set_bot_commands(bot: Bot):
    commands = [
        types.BotCommand("help", "помощь"),
        types.BotCommand("bdays", "список ближайших дней рождений"),
        types.BotCommand("code", "код подтверждения яндекс диска"),
        types.BotCommand("joke", "пошутить"),
        types.BotCommand("test", "для тестирования"),
    ]
    await bot.set_my_commands(commands)


async def on_startup(dp: Dispatcher):
    await set_bot_commands(dp.bot)


if __name__ == "__main__":
    register_common_handlers(dp)
    register_bdays_handlers(dp)
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
