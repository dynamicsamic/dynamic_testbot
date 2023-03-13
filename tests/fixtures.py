import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from app.db import models

from .common import constants, today
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


@pytest.fixture
def create_test_data(db_session):
    """Create test instances for Birthday model."""
    BirthdayFactory._meta.sqlalchemy_session = db_session
    BirthdayFactory.create_today_birthdays(constants["TODAY_BDAY_NUM"])
    BirthdayFactory.create_future_birthdays(constants["FUTURE_BDAY_NUM"])
    db_session.commit()


@pytest.fixture
def create_birthday_range(db_session):
    today_ = today()
    days = [{"name": f"name{i}", "date": today_} for i in range(1, 401)]
    db_session.bulk_insert_mappings(models.Birthday, days)
    db_session.commit()
