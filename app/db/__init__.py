from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from app import settings

db_engine = create_engine(
    f"sqlite:////{settings.BASE_DIR}/{settings.DB_NAME}",
    echo=settings.DEBUG,
)

Session = scoped_session(sessionmaker())
