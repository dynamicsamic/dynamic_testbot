from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

Scheduler = AsyncIOScheduler(
    timezone=timezone("Europe/Moscow"),
    executors={"default": AsyncIOExecutor()},
    job_defaults={"misfire_grace_time": 30, "coalesce": True},
)
