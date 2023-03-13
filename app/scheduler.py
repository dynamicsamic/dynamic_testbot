from typing import Callable

from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.job import Job
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app import settings
from app.db import jobstore_engine, models
from app.file_parser import (
    dispatch_birthday_message_to_chat,
    preload_birthday_messages,
)


class BotScheduler(AsyncIOScheduler):
    """
    Subclass of `AsyncIOScheduler` from `appscheduler` package
    extended with some convinient custom methods.
    """

    __doc__ += AsyncIOScheduler.__doc__

    def setup_daily_message_preload(self) -> Job:
        """
        Schedule daily birthday messages preload.
        Need to be executed on each program startup.
        """
        return self.add_job(
            preload_birthday_messages,
            trigger=CronTrigger(day_of_week="mon-sun", hour=0, minute=10),
            id=1,
            replace_existing=True,
        )

    def add_chat_to_birthday_mailing(self, chat_id: int) -> Job:
        """Schedule daily birthday messages delivery to provided chat."""
        return self.add_job(
            dispatch_birthday_message_to_chat,
            trigger=CronTrigger(day_of_week="mon-sun", hour=9, minute=0),
            id=str(chat_id),
            replace_existing=True,
            kwargs={"chat_id": chat_id},
        )


Scheduler = BotScheduler(
    jobstores={"default": SQLAlchemyJobStore(engine=jobstore_engine)},
    timezone=settings.TIME_ZONE,
    executors={"default": AsyncIOExecutor()},
    job_defaults={"misfire_grace_time": 30, "coalesce": True},
)


def add_job(f: Callable | str, chat_id: int, **func_kwargs) -> Job:
    """Add single bday_job to Scheduler for a single tg_chat."""
    return Scheduler.add_job(
        f,
        # "app.handlers.bdays:get_bdays_job",
        # trigger=CronTrigger(day_of_week="mon-sun", hour=23, minute=9),
        "interval",
        seconds=10,
        # "cron",
        # day_of_week="mon-sun",
        # hour=9,
        kwargs={"bot_path": "app.bot:bot", "chat_id": chat_id, **func_kwargs},
        replace_existing=True,
        id=str(chat_id),
    )


def add_preload_job():
    return Scheduler.add_job(
        "app.file_parser:run_preload",
        "cron",
        day_of_week="mon-sun",
        hour=0,
        minute=7,
        kwargs={"bot_path": "app.bot:bot"},
        replace_existing=True,
        id="1",
    )
