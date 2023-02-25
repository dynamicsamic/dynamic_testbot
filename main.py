import logging
from logging.config import fileConfig

from aiogram import Bot, Dispatcher, executor, types
from sqlalchemy import create_engine

from app import settings
from app.db import Session, events, models
from app.handlers import register_bdays_handlers, register_common_handlers
from app.scheduler import Scheduler, load_jobs

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


from app.bot import dispatcher as dp


async def set_bot_commands(bot: Bot):
    commands = [
        # types.BotCommand("help", "помощь"),
        types.BotCommand("bdays", "получить список ближайших ДР"),
        types.BotCommand("addchat", "добавить чат рассылку"),
        # types.BotCommand("code", "код подтверждения яндекс диска"),
        # types.BotCommand("joke", "пошутить"),
        # types.BotCommand("test", "для тестирования"),
    ]
    await bot.set_my_commands(commands)


async def on_startup(dp: Dispatcher):
    """ """
    engine = create_engine(
        f"sqlite:////{settings.BASE_DIR}/{settings.DB_NAME}",
        echo=settings.DEBUG,
    )
    Session.configure(bind=engine)
    models.Base.metadata.create_all(engine)
    load_jobs(Session)
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
    events.load_events()
    executor.start_polling(
        dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown
    )
