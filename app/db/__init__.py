from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app import settings

app_db = settings.DB["app"]
jobstore_db = settings.DB["jobstore"]

db_engine = create_engine(
    f"{app_db['engine']}:////{settings.BASE_DIR}/{app_db['name']}",
    echo=settings.DEBUG,
)

Session = scoped_session(sessionmaker())

jobstore_engine = create_engine(
    f"{jobstore_db['engine']}:////{settings.BASE_DIR}/{jobstore_db['name']}",
    echo=settings.DEBUG,
)


@contextmanager
def get_session():
    Session.configure(bind=db_engine)
    try:
        yield Session
    except Exception:
        Session.rollback()
    finally:
        Session.close()
