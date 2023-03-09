import datetime as dt

constants = {
    "TODAY_BDAY_NUM": 3,
    "FUTURE_BDAY_NUM": 5,
}


def today() -> dt.date:
    return dt.date.today()
