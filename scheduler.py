import asyncio

from aiogram import types
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

from handlers.bdays import cmd_bdays

Scheduler = AsyncIOScheduler(
    timezone=timezone("Europe/Moscow"),
    executors={"default": AsyncIOExecutor()},
    job_defaults={"misfire_grace_time": 30, "coalesce": True},
)
