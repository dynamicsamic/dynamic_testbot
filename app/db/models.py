import datetime as dt
from typing import Self

from sqlalchemy import Column, Date, DateTime, Integer, String, select
from sqlalchemy.engine.result import ScalarResult
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from app import settings


class Base(DeclarativeBase):
    pass


from sqlalchemy.dialects.sqlite import insert


class Birthday(Base):
    __tablename__ = "birthday"

    name = Column(String(length=128), unique=True, primary_key=True)
    date = Column(Date, primary_key=True)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.date})"

    @classmethod
    def today(cls, session: Session, today: dt.date) -> ScalarResult[Self]:
        """Return birthdays for today."""
        return session.scalars(select(cls).where(cls.date == today))


class TelegramChat(Base):
    __tablename__ = "telegram_chat"

    tg_chat_id = Column(Integer, primary_key=True)
    created_at = Column(
        DateTime(timezone=True),
        default=dt.datetime.now(tz=settings.TIME_ZONE),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=dt.datetime.now(tz=settings.TIME_ZONE),
        onupdate=dt.datetime.now(tz=settings.TIME_ZONE),
    )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.tg_chat_id})"

    @classmethod
    def all(
        cls, session: Session, to_list: bool = False
    ) -> ScalarResult[Self] | list[Self]:
        """
        Return all model instances
        either in ScalarResult or list form.
        """
        qs = session.scalars(select(cls))
        return qs.all() if to_list else qs

    @classmethod
    def ids(
        cls, session: Session, to_list: bool = False
    ) -> ScalarResult[int] | list[int]:
        """
        Return id's of all model instances
        either in ScalarResult or list form.
        """
        qs = session.scalars(select(cls.tg_chat_id))
        return qs.all() if to_list else qs
