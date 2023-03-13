import datetime as dt

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.db import models

from .common import constants, today
from .fixtures import (
    create_birthday_range,
    create_tables,
    create_test_data,
    db_session,
    engine,
)


def test_birthday_get_method_with_valid_name_returns_model_instance(
    db_session, create_test_data
):
    valid_name = "name0"
    birthday = models.Birthday.queries.get(db_session, valid_name)
    assert isinstance(birthday, models.Birthday)


def test_birthday_get_method_with_invalid_name_returns_none(
    db_session, create_test_data
):
    invalid_name = "invalid"
    birthday = models.Birthday.queries.get(db_session, invalid_name)
    assert birthday is None


def test_birthday_all_method_returns_list_of_all_model_instances(
    db_session, create_test_data
):
    expected_birthdays = db_session.scalars(select(models.Birthday))
    birthdays = models.Birthday.queries.all(db_session)
    assert isinstance(birthdays, list)
    for expected_bday, bday in zip(expected_birthdays, birthdays):
        assert expected_bday is bday


def test_birthday_count_method_returns_number_of_all_model_instances(
    db_session, create_test_data
):
    expected_birthday_num = (
        constants["TODAY_BDAY_NUM"] + constants["FUTURE_BDAY_NUM"]
    )
    birthday_num = models.Birthday.queries.count(db_session)
    assert isinstance(birthday_num, int)
    assert birthday_num == expected_birthday_num


def test_birthday_last_method_returns_instance_with_latest_birth_date(
    db_session, create_test_data
):
    birthdays = models.Birthday.queries.all(db_session)
    latest_date = today()
    for birthday in birthdays:
        if birthday.date > latest_date:
            latest_date = birthday.date

    latest_birthday = models.Birthday.queries.last(db_session)
    assert isinstance(latest_birthday, models.Birthday)
    assert latest_birthday.date == latest_date


def test_birthday_first_method_returns_instance_with_earliest_birth_date(
    db_session, create_test_data
):
    birthdays = models.Birthday.queries.all(db_session)
    earliest_date = dt.date(year=2030, month=12, day=31)
    for birthday in birthdays:
        if birthday.date < earliest_date:
            earliest_date = birthday.date

    first_birthday = models.Birthday.queries.first(db_session)
    assert isinstance(first_birthday, models.Birthday)
    assert first_birthday.date == earliest_date


def test_birthday_between_method_with_valid_dates_returns_list_of_model_instances(
    db_session, create_test_data
):
    today_ = today()
    birthdays = models.Birthday.queries.between(db_session, today_, today_)
    assert isinstance(birthdays, list)
    assert len(birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_between_method_with_valid_string_dates_returns_list_of_model_instances(
    db_session, create_test_data
):
    today_ = today()
    birthdays = models.Birthday.queries.between(
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
    birthdays = models.Birthday.queries.between(
        db_session, invalid_start, invalid_end
    )
    birthday_num = models.Birthday.queries.count(db_session)
    assert len(birthdays) == birthday_num - 1
    assert all(birthday.date > last_year_date for birthday in birthdays)


def test_birthday_today_method_returns_list_of_instances_with_current_date(
    db_session, create_test_data
):
    today_birthdays = models.Birthday.queries.today(db_session)
    assert isinstance(today_birthdays, list)
    assert len(today_birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_future_method_returns_list_of_instaces_with_date_between_tomorrow_and_delta(
    db_session, create_test_data
):
    # prepartion stage: add random next year birthday
    next_year_date = dt.date(year=today().year + 1, month=7, day=1)
    next_year_birthday = models.Birthday(name="valid", date=next_year_date)
    db_session.add(next_year_birthday)
    db_session.commit()

    future_birthdays = models.Birthday.queries.future(db_session)
    assert isinstance(future_birthdays, list)
    assert len(future_birthdays) == constants["FUTURE_BDAY_NUM"]


def test_birthday_future_all_method_returns_list_of_instaces_with_date_greater_than_today(
    db_session, create_test_data
):
    # prepartion stage: add random next year birthday
    next_year_date = dt.date(year=today().year + 1, month=7, day=1)
    next_year_birthday = models.Birthday(name="valid", date=next_year_date)
    db_session.add(next_year_birthday)
    db_session.commit()

    future_birthdays = models.Birthday.queries.future_all(db_session)
    assert isinstance(future_birthdays, list)
    assert len(future_birthdays) == constants["FUTURE_BDAY_NUM"] + 1


def test_birthday_refresh_table_method_deletes_all_rows_and_populates_db_again(
    db_session, create_birthday_range
):
    initial_birthday_num = models.Birthday.queries.count(db_session)
    assert initial_birthday_num == 400  # number of birthdays created

    models.Birthday.operations.refresh_table(
        db_session, [{"name": "valid", "date": today()}]
    )
    db_session.commit()
    assert models.Birthday.queries.count(db_session) == 1


def test_birthday_bulk_save_objects_saves_new_instances_to_db(db_session):
    objects_num_to_be_created = 400
    names = [f"name{i}" for i in range(objects_num_to_be_created)]
    date = today()
    birthdays = [models.Birthday(name=name, date=date) for name in names]

    initial_birthday_num = models.Birthday.queries.count(db_session)

    db_session.bulk_save_objects(birthdays)
    db_session.commit()

    current_birthday_num = models.Birthday.queries.count(db_session)
    assert (
        current_birthday_num
        == initial_birthday_num + objects_num_to_be_created
    )


def test_birthday_sqlite_upsert_method_with_valid_data_saves_instance_to_db(
    db_session,
):
    initial_birthday_num = models.Birthday.queries.count(db_session)
    models.Birthday.operations.sqlite_upsert(db_session, "valid_name", today())
    current_birthday_num = models.Birthday.queries.count(db_session)
    assert current_birthday_num == initial_birthday_num + 1
    inserted_obj = models.Birthday.queries.get(db_session, "valid_name")
    assert inserted_obj.date == today()


def test_birthday_sqlite_upsert_method_updates_instance_instead_of_creating(
    db_session,
):
    models.Birthday.operations.sqlite_upsert(db_session, "valid_name", today())
    initial_birthday_num = models.Birthday.queries.count(db_session)

    models.Birthday.operations.sqlite_upsert(db_session, "valid_name", today())
    current_birthday_num = models.Birthday.queries.count(db_session)

    assert current_birthday_num == initial_birthday_num
