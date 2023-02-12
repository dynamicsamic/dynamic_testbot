from aiogram import types
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

from handlers.bdays import cmd_bdays

Scheduler = AsyncIOScheduler(timezone=timezone("Europe/Moscow"))

Scheduler.add_job(
    cmd_bdays,
    trigger=CronTrigger(day_of_week="mon-sun", minute=1),
    kwargs={"message": types.Message},
)
