import asyncio
import logging
from logging.config import fileConfig

from aiogram import Bot, Dispatcher, executor, types
from apscheduler.triggers.cron import CronTrigger

import settings
from handlers import register_bdays_handlers, register_common_handlers
from handlers.bdays import get_bdays
from scheduler import Scheduler

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

bot = Bot(token=settings.BOT_TOKEN)
dp = Dispatcher(bot)


async def foo():
    await bot.send_message(chat_id=359722292, text="hello friend")


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
    Scheduler.add_job(
        get_bdays,
        "interval",
        seconds=30,
        # "cron",
        # day_of_week="mon-fri",
        # hour=10,
        # second="*",
        # trigger=CronTrigger(day_of_week="mon-fri"),
        kwargs={"bot": bot},
        replace_existing=True,
    )
    Scheduler.start()
    await set_bot_commands(dp.bot)


async def on_shutdown(_: Dispatcher):
    """Execute function before Bot shut down polling."""
    # remove all jobs and shut down the Scheduler
    Scheduler.remove_all_jobs()
    Scheduler.shutdown()


if __name__ == "__main__":
    register_common_handlers(dp)
    register_bdays_handlers(dp)
    executor.start_polling(
        dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown
    )
