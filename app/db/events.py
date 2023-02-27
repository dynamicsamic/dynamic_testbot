import logging

from sqlalchemy import event

from app.scheduler import add_job

from . import models

logger = logging.getLogger(__name__)


def after_tg_chat_insert(mapper, connection, target):
    """Add job after tg chat insert in DB."""
    logger.info("issued chat creation signal")
    add_job(target.tg_chat_id)


def load_events():
    event.listen(models.TelegramChat, "after_insert", after_tg_chat_insert)
