import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from app.db import models

from .factories import BirthdayFactory

IN_MEMORY_TEST_DB_URL = "sqlite://"


@pytest.fixture(scope="session")
def engine():
    return create_engine(IN_MEMORY_TEST_DB_URL, echo=True)


@pytest.fixture(scope="session")
def create_tables(engine):
    models.Base.metadata.create_all(bind=engine)
    yield
    models.Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session(engine, create_tables):
    connection = engine.connect()
    transaction = connection.begin()
    session = scoped_session(sessionmaker(bind=connection, autoflush=False))

    yield session

    session.close()
    transaction.rollback()
    connection.close()


def create_today_birthdays(num: int):
    BirthdayFactory.create_batch(num, date=dt.date.today())


def create_tomorrow_birthdays(num: int):
    BirthdayFactory.create_batch(
        num, date=(dt.date.today() + dt.timedelta(days=1))
    )


@pytest.fixture
def create_test_data(db_session):
    """Create 5 test Birthday instances."""
    BirthdayFactory._meta.sqlalchemy_session = db_session
    create_today_birthdays(3)
    create_tomorrow_birthdays(3)
    BirthdayFactory.create_batch(3)
