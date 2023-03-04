from typing import Callable

from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.job import Job
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from app import settings
from app.bot import bot
from app.db import jobstore_engine, models

# from app.handlers.bdays import get_bdays_job

jobstores = {"default": SQLAlchemyJobStore(engine=jobstore_engine)}

Scheduler = AsyncIOScheduler(
    jobstores=jobstores,
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
        "date",
        kwargs={"bot_path": "app.bot:bot"},
        replace_existing=True,
        id=1,
    )


def load_jobs(db_session: Session) -> None:
    """Load bday_job to Scheduler for all tg_chats in db."""
    with db_session() as session:
        tg_chats = models.TelegramChat.ids(session)
    for chat_id in tg_chats:
        add_job(chat_id)
