import datetime as dt
from functools import cache
from typing import Any, Self, Sequence, Type, TypeVar

from sqlalchemy import Column, Date, DateTime, Integer, String, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine.result import ScalarResult
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from app import settings
from tests.common import today as today_


class Base(DeclarativeBase):
    pass


class QueryManagerBase:
    """
    Class for performing data querying operations
    such as get_one, get_all etc.
    """

    def __init__(self, model: Base) -> None:
        self.model = model

    def get(self, session: Session, name: str) -> Type[Base]:
        """Fetch an instance of `model` with given `name`."""
        return session.scalar(
            select(self.model).where(self.model.name == name)
        )

    def all(self, session: Session) -> list[Type[Base]]:
        """Fetch all instances of `model`."""
        return session.scalars(select(self.model)).all()

    def count(self, session: Session) -> int:
        """Count number of instances of `model` recorded in db."""
        return session.scalar(select(func.count(self.model.name)))


class BirthdayQueryManager(QueryManagerBase):
    def last(self, session: Session) -> Type[Base]:
        """Fetch last added instance of `model`."""
        return session.scalar(
            select(self.model).order_by(self.model.date.desc()).limit(1)
        )

    def first(self, session: Session) -> Type[Base]:
        """Fetch first added instance of `model`."""
        return session.scalar(
            select(self.model).order_by(self.model.date).limit(1)
        )

    def between(
        self, session: Session, start: dt.date | str, end: dt.date | str
    ) -> list[Type[Base]]:
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
            select(self.model).filter(self.model.date.between(start, end))
        ).all()

    def today(
        self, session: Session, today: dt.date = None
    ) -> list[Type[Base]]:
        """
        Fetch all instances of `model` which have
        `date` attribute equal to today.
        """
        today = today or today_()
        return self.between(session, today, today)

    def future(
        self, session: Session, today: dt.date = None, delta: int = 3
    ) -> list[Type[Base]]:
        """Fetch all instances of `model` from db
        which have `date` attribute between tomorrow and delta."""
        today = today or today_()
        start = today + dt.timedelta(days=1)
        end = today + dt.timedelta(days=delta)
        return self.between(session, start, end)

    def future_all(
        self, session: Session, today: dt.date = None
    ) -> list[Type[Base]]:
        """Fetch all instances of `model` from db
        which have `date` attribute greater than today."""
        today = today or today_()
        return session.scalars(
            select(self.model).filter(self.model.date > today)
        ).all()


class BirthdayManipulationManager:
    """
    Class for performing data manipulation operations
    such as create, update, delete.
    """

    def __init__(self, model: Type[Base]) -> None:
        self.model = model

    def refresh_table(
        self, session: Session, mappings: Sequence[dict[str, Any]]
    ) -> None:
        """Delete all 'model' rows then populate db with given mappings."""
        session.query(self.model).delete()
        session.bulk_insert_mappings(self.model, mappings)

    def bulk_save_objects(
        self, session: Session, birthdays: Sequence[Type[Base]]
    ) -> None:
        """Saves new 'model' instances to db."""
        session.bulk_save_objects(birthdays)

    def sqlite_upsert(
        self,
        session: Session,
        name: str,
        date: dt.date,
    ) -> None:
        """
        Insert new row into db. If such row already exists, update it.
        Uses `sqlite` specific syntax.
        WARNING: this method is too slow in bulk insert operations!
        """
        insert_stmt = sqlite_insert(self.model.__table__).values(
            name=name, date=date
        )
        on_duplicate_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=("name", "date"),
            set_=dict(name=name, date=date),
        )
        session.execute(on_duplicate_update_stmt)

    def sqlite_insert_ignore_duplicate(
        self, session: Session, name: str, date: dt.date
    ) -> None:
        """
        Insert new row into db. If such row already exists, ignore it.
        Uses `sqlite` specific syntax.
        WARNING: this method is too slow in bulk insert operations!
        Although a bit faster than `upsert` method.
        """
        insert_stmt = sqlite_insert(self.model.__table__).values(
            name=name, date=date
        )
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
            index_elements=("name", "date")
        )
        session.execute(do_nothing_stmt)


class Birthday(Base):
    __tablename__ = "birthday"

    id = Column(Integer, primary_key=True)
    name = Column(String(length=128), unique=True)
    date = Column(Date)

    @classmethod
    @property
    @cache
    def queries(cls) -> BirthdayQueryManager:
        """Setup query manager."""
        return BirthdayQueryManager(cls)

    @classmethod
    @property
    @cache
    def operations(cls) -> BirthdayManipulationManager:
        """Setup data manipulation manager."""
        return BirthdayManipulationManager(cls)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.id}, {self.name}, {self.date})"
        )
