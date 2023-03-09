import datetime as dt
from typing import Any, Self

from sqlalchemy import Column, Date, DateTime, Integer, String, func, select
from sqlalchemy.engine.result import ScalarResult
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from app import settings
from tests.common import today


class Base(DeclarativeBase):
    pass


from sqlalchemy.dialects.sqlite import insert


class QueryManagerBase:
    def __init__(self, model: Base) -> None:
        self.model = model

    def get(self, session, name: str) -> Base:
        """Fetch an instance of `self.model` with given `name` from db."""
        return session.scalar(
            select(self.model).where(self.model.name == name)
        )

    def all(self, session) -> list[Base]:
        """Fetch all instances of `self.model` from db."""
        return session.scalars(select(self.model)).all()

    def count(self, session) -> int:
        """Count number of instances of `self.model` recorded in db."""
        return session.scalars(select(func.count(self.model.name))).one()


class BirthdayQueryManager(QueryManagerBase):
    def last(self, session) -> Base:
        """Fetch last added instance of `self.model` from db."""
        return session.scalars(
            select(self.model).order_by(self.model.date.desc())
        ).limit(1)

    def first(self, session) -> Base:
        """Fetch first added instance of `self.model` from db."""
        return session.scalars(
            select(self.model).order_by(self.model.date)
        ).limit(1)

    def today(self, session) -> list[Base]:
        """Fetch all instances of `self.model` from db
        which have `date` attribute equal to today."""
        return session.scalars(
            select(self.model).where(self.model.date == today())
        ).all()

    def future(self, session, delta: int = 3) -> list[Base]:
        """Fetch all instances of `self.model` from db
        which have `date` attribute greater than today."""
        return session.scalars(
            select(self.model).filter(
                self.model.date.between(today(), dt.timedelta(days=delta))
            )
        )


class Mixin:
    @classmethod
    def get(cls, session, name: str) -> Base:
        """Fetch an instance of `self.model` with given `name` from db."""
        return session.scalar(select(cls).where(cls.name == name))


class Birthday(Base, Mixin):
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
