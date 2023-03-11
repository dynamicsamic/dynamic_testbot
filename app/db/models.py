import datetime as dt
from typing import Any, Self, Type

from sqlalchemy import Column, Date, DateTime, Integer, String, func, select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine.result import ScalarResult
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from app import settings
from tests.common import today as today_


class Base(DeclarativeBase):
    pass


class QueryMixinBase:
    @classmethod
    def get(cls, session: Session, name: str) -> Self:
        """Fetch an instance of `model` with given `name` from db."""
        return session.scalar(select(cls).where(cls.name == name))

    @classmethod
    def all(cls, session: Session) -> list[Self]:
        """Fetch all instances of `model` from db."""
        return session.scalars(select(cls)).all()

    @classmethod
    def count(cls, session: Session) -> int:
        """Count number of instances of `model` recorded in db."""
        return session.scalars(select(func.count(cls.name))).one()


class BirthdayQueryMixin(QueryMixinBase):
    @classmethod
    def last(cls, session: Session) -> Self:
        """Fetch last added instance of `model` from db."""
        return session.scalar(select(cls).order_by(cls.date.desc()).limit(1))

    @classmethod
    def first(cls, session: Session) -> Self:
        """Fetch first added instance of `model` from db."""
        return session.scalar(select(cls).order_by(cls.date).limit(1))

    @classmethod
    def between(
        cls, session: Session, start: dt.date | str, end: dt.date | str
    ) -> list[Self]:
        """
        Fetch all instances of `model` which have
        `date` attribute between given date borders.

        Arguments for `start` and `end` may be passed as strings.
        In this case arguments must follow ISO format `yyyy-mm-dd'.
        If not, borders will be replaced with current year period.
        """
        if isinstance(start, str):
            try:
                start = dt.date.fromisoformat(start)
            except ValueError:
                start = dt.date.fromisoformat(f"{today_().year}-01-01")
        if isinstance(end, str):
            try:
                end = dt.date.fromisoformat(end)
            except ValueError:
                end = dt.date.fromisoformat(f"{today_().year}-12-31")

        return session.scalars(
            select(cls).filter(cls.date.between(start, end))
        ).all()

    @classmethod
    def today(cls, session: Session, today: dt.date = None) -> list[Self]:
        """
        Fetch all instances of `model` from db
        which have `date` attribute equal to today.
        """
        today = today or today_()
        return cls.between(session, today, today)

    @classmethod
    def future(
        cls, session: Session, today: dt.date = None, delta: int = 3
    ) -> list[Self]:
        """Fetch all instances of `model` from db
        which have `date` attribute between tomorrow and delta."""
        today = today or today_()
        start = today + dt.timedelta(days=1)
        end = today + dt.timedelta(days=delta)
        return cls.between(session, start, end)

    @classmethod
    def future_max(cls, session: Session, today: dt.date = None) -> list[Self]:
        """Fetch all instances of `model` from db
        which have `date` attribute greater than today."""
        today = today or today_()
        return session.scalars(select(cls).filter(cls.date > today))


class Birthday(Base, BirthdayQueryMixin):
    __tablename__ = "birthday"

    name = Column(String(length=128), unique=True, primary_key=True)
    date = Column(Date, primary_key=True)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.date})"


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
