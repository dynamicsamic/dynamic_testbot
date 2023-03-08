import contextlib
import datetime as dt

from sqlalchemy import create_engine, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from app.db.models import Base, Birthday

engine = create_engine(
    "sqlite+pysqlite:///:memory:", echo=True, query_cache_size=0
)


@contextlib.contextmanager
def get_session(cleanup=False):
    session = Session(bind=engine)
    Base.metadata.create_all(engine)

    try:
        yield session
    except Exception:
        session.rollback()
    finally:
        session.close()

    if cleanup:
        Base.metadata.drop_all(engine)


@contextlib.contextmanager
def get_conn(cleanup=False):
    conn = engine.connect()
    Base.metadata.create_all(engine)

    yield conn
    conn.close()

    if cleanup:
        Base.metadata.drop_all(engine)


today = dt.date.today()


def test_general():
    with get_session() as session:
        bday = Birthday(name="generic", date=today)
        session.add(bday)
        session.commit()
        result = session.scalars(select(Birthday))
        print(result.all())


test_general()
