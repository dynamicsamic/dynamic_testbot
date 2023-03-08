import datetime as dt

import factory

from app.db import models


def today():
    return dt.date.today()


class BirthdayFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Birthday
        sqlalchemy_session = None

    name = factory.Sequence(lambda x: f"name{x}")
    date = factory.Faker(
        "date_between",
        start_date=today() - dt.timedelta(days=1),
        end_date=today() + dt.timedelta(days=4),
    )
