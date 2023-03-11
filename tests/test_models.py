import datetime as dt

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.db import models

from .common import constants, today
from .fixtures import create_tables, create_test_data, db_session, engine


def test_birthday_get_method_with_valid_name_returns_model_instance(
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


def test_birthday_all_method_returns_list_of_all_model_instances(
    db_session, create_test_data
):
    expected_birthdays = db_session.scalars(select(models.Birthday))
    birthdays = models.Birthday.all(db_session)
    assert isinstance(birthdays, list)
    for expected_bday, bday in zip(expected_birthdays, birthdays):
        assert expected_bday is bday


def test_birthday_count_method_returns_number_of_all_model_instances(
    db_session, create_test_data
):
    expected_birthday_num = (
        constants["TODAY_BDAY_NUM"] + constants["FUTURE_BDAY_NUM"]
    )
    birthday_num = models.Birthday.count(db_session)
    assert isinstance(birthday_num, int)
    assert birthday_num == expected_birthday_num


def test_birthday_last_method_returns_instance_with_latest_birth_date(
    db_session, create_test_data
):
    birthdays = models.Birthday.all(db_session)
    latest_date = today()
    for birthday in birthdays:
        if birthday.date > latest_date:
            latest_date = birthday.date

    latest_birthday = models.Birthday.last(db_session)
    assert isinstance(latest_birthday, models.Birthday)
    assert latest_birthday.date == latest_date


def test_birthday_first_method_returns_instance_with_earliest_birth_date(
    db_session, create_test_data
):
    birthdays = models.Birthday.all(db_session)
    earliest_date = dt.date(year=2030, month=12, day=31)
    for birthday in birthdays:
        if birthday.date < earliest_date:
            earliest_date = birthday.date

    first_birthday = models.Birthday.first(db_session)
    assert isinstance(first_birthday, models.Birthday)
    assert first_birthday.date == earliest_date


def test_birthday_between_method_with_valid_dates_returns_list_of_model_instances(
    db_session, create_test_data
):
    today_ = today()
    birthdays = models.Birthday.between(db_session, today_, today_)
    assert isinstance(birthdays, list)
    assert len(birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_between_method_with_valid_string_dates_returns_list_of_model_instances(
    db_session, create_test_data
):
    today_ = today()
    birthdays = models.Birthday.between(
        db_session, today_.isoformat(), today_.isoformat()
    )
    assert isinstance(birthdays, list)
    assert len(birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_between_method_with_invalid_string_dates_returns_list_of_current_year_model_instances(
    db_session, create_test_data
):
    # prepartion stage: add random last year birthday
    last_year_date = dt.date(year=today().year - 1, month=7, day=1)
    last_year_birthday = models.Birthday(name="valid", date=last_year_date)
    db_session.add(last_year_birthday)
    db_session.commit()

    invalid_start = "202-1-1"
    invalid_end = "1999-15-3"
    birthdays = models.Birthday.between(db_session, invalid_start, invalid_end)
    birthday_num = models.Birthday.count(db_session)
    assert len(birthdays) == birthday_num - 1
    assert all(birthday.date > last_year_date for birthday in birthdays)


def test_birthday_today_method_returns_expected_num(
    db_session, create_test_data
):
    today_birthdays = models.Birthday.today(db_session)
    assert len(today_birthdays) == constants["TODAY_BDAY_NUM"]


def test_foo(db_session, create_test_data):
    print(models.Birthday.future_no_scope(db_session))


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
