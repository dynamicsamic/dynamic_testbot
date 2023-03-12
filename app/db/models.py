import datetime as dt
from typing import Any, Self, Sequence, Type

from sqlalchemy import Column, Date, DateTime, Integer, String, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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
        return session.scalars(select(cls).filter(cls.date > today)).all()


class QueryManagerBase:
    def __init__(self, model: Base) -> None:
        self.model = model

    def get(self, session: Session, name: str) -> Self:
        """Fetch an instance of `model` with given `name` from db."""
        return session.scalar(
            select(self.model).where(self.model.name == name)
        )

    def all(self, session: Session) -> list[Self]:
        """Fetch all instances of `model` from db."""
        return session.scalars(select(self.model)).all()

    def count(self, session: Session) -> int:
        """Count number of instances of `model` recorded in db."""
        return session.scalars(select(func.count(self.model.name))).one()


class BirthdayQueryManager(QueryManagerBase):
    def last(self, session: Session) -> Self:
        """Fetch last added instance of `model` from db."""
        return session.scalar(
            select(self.model).order_by(self.model.date.desc()).limit(1)
        )

    def first(self, session: Session) -> Self:
        """Fetch first added instance of `model` from db."""
        return session.scalar(
            select(self.model).order_by(self.model.date).limit(1)
        )

    def between(
        self, session: Session, start: dt.date | str, end: dt.date | str
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
            select(self.model).filter(self.model.date.between(start, end))
        ).all()

    def today(self, session: Session, today: dt.date = None) -> list[Self]:
        """
        Fetch all instances of `model` from db
        which have `date` attribute equal to today.
        """
        today = today or today_()
        return self.between(session, today, today)

    def future(
        self, session: Session, today: dt.date = None, delta: int = 3
    ) -> list[Self]:
        """Fetch all instances of `model` from db
        which have `date` attribute between tomorrow and delta."""
        today = today or today_()
        start = today + dt.timedelta(days=1)
        end = today + dt.timedelta(days=delta)
        return self.between(session, start, end)

    def future_all(
        self, session: Session, today: dt.date = None
    ) -> list[Self]:
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

    def __init__(self, model) -> None:
        self.model = model

    def bulk_upsert(
        self, session: Session, birthdays: Sequence["Birthday"]
    ) -> None:
        """
        Saves new instances to db.
        Updates db rows if any changes occured.
        Skips db rows if no changes detected.
        Works 10x faster than sqlite_upsert.
        """
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
    ):
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

    name = Column(String(length=128), unique=True, primary_key=True)
    date = Column(Date, primary_key=True)

    def __init__(self, **kw: Any):
        class_ = self.__class__

        # set QueryManager for SELECT operations
        if not hasattr(class_, "queries"):
            setattr(class_, "queries", BirthdayQueryManager(class_))

        # set DataManipulationManager instance
        # for CREATE, UPDATE and DELETE operations
        if not hasattr(class_, "operations"):
            setattr(class_, "operations", BirthdayManipulationManager(class_))

        super().__init__(**kw)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.date})"

    # @classmethod
    # def sqlite_upsert(
    #     cls, session: Session, name: str, date: dt.date
    # ) -> None:

    #     insert_stmt = sqlite_insert(cls.__table__).values(
    #         name=name, date=date
    #     )
    #     on_duplicate_update_stmt = insert_stmt.on_conflict_do_update(
    #         index_elements=("name", "date"),
    #         set_=dict(name=name, date=date),
    #     )
    #     session.execute(on_duplicate_update_stmt)
    #     session.commit()


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
