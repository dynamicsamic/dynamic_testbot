from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.job import Job
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import Session

from app import settings
from app.bot import bot
from app.db import db_engine, models
from app.handlers.bdays import get_bdays_job

jobstores = {"default": SQLAlchemyJobStore(engine=db_engine)}

Scheduler = AsyncIOScheduler(
    timezone=settings.TIME_ZONE,
    executors={"default": AsyncIOExecutor()},
    jobstores={"default": SQLAlchemyJobStore(engine=db_engine)},
    job_defaults={"misfire_grace_time": 30, "coalesce": True},
)


def add_job(chat_id: int) -> Job:
    """Add single bday_job to Scheduler for a single tg_chat."""
    Scheduler.add_job(
        get_bdays_job,
        "cron",
        day_of_week="mon-sun",
        hour=9,
        kwargs={"bot": bot, "chat_id": chat_id},
        replace_existing=True,
    )


def load_jobs(db_session: Session) -> None:
    """Load bday_job to Scheduler for all tg_chats in db."""
    with db_session() as session:
        tg_chats = models.TelegramChat.ids(session)
    for chat_id in tg_chats:
        add_job(chat_id)
