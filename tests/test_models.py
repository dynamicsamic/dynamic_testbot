import datetime as dt

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.db import models

from .common import constants, today
from .fixtures import create_tables, create_test_data, db_session, engine


def test_birthday_get_method_with_valid_name_returns_instance(
    db_session, create_test_data
):
    valid_name = "name0"
    birthday = models.Birthday.get(db_session, valid_name)
    assert isinstance(birthday, models.Birthday)


def test_birthday_get_method_with_invalid_name_returns_none(
    db_session, create_test_data
):
    invalid_name = "invalid"
    birthday = models.Birthday.get(db_session, invalid_name)
    assert birthday is None


def test_birthday_today_method_returns_expected_num(
    db_session, create_test_data
):
    today_birthdays = models.Birthday.today(db_session)
    assert len(today_birthdays) == constants["TODAY_BDAY_NUM"]


# def test_general(db_session, create_test_data):
#     # create_test_data(db_session)
#     bday = models.Birthday(name="generic", date=today)
#     bday1 = models.Birthday(name="genefric", date=today)
#     bday2 = models.Birthday(name="genefffric", date=tomorrow)
#     db_session.add_all((bday, bday1, bday2))
#     db_session.commit()
#     q = models.Birthday.today(db_session, today)
#     for i in q:
#         print("HERE\n", i)
#     # print(db_session.scalar(select(models.Birthday)))
#     # bday = models.Birthday(name="genfferic", date=today)
#     # db_session.add(bday)
#     # try:
#     #     db_session.commit()
#     # except SQLAlchemyError as e:
#     #     db_session.rollback()
#     #     print(e)
