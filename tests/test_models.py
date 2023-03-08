import datetime as dt

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.db import models

from .fixtures import create_tables, create_test_data, db_session, engine

today = dt.date.today()
tomorrow = dt.date(year=2023, month=3, day=11)


# def test_startup(db_session, create_test_data):
#     pass


def test_general(db_session, create_test_data):
    # create_test_data(db_session)
    bday = models.Birthday(name="generic", date=today)
    bday1 = models.Birthday(name="genefric", date=today)
    bday2 = models.Birthday(name="genefffric", date=tomorrow)
    db_session.add_all((bday, bday1, bday2))
    db_session.commit()
    q = models.Birthday.today(db_session, today)
    for i in q:
        print("HERE\n", i)
    # print(db_session.scalar(select(models.Birthday)))
    # bday = models.Birthday(name="genfferic", date=today)
    # db_session.add(bday)
    # try:
    #     db_session.commit()
    # except SQLAlchemyError as e:
    #     db_session.rollback()
    #     print(e)


def test_factory(db_session):
    q = db_session.scalars(select(models.Birthday)).all()
    print(q)
