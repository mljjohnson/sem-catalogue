from contextlib import contextmanager
import os
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import make_url

from app.core.config import settings
from app.models.tables import Base


def _ensure_sqlite_path(url: str) -> None:
    if url.startswith("sqlite:///"):
        # Convert sqlite:///./data/dev.db -> ./data/dev.db
        path = url[len("sqlite:///") :]
        # Only create directory if path points to a file
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)


_ensure_sqlite_path(settings.database_url)

# Removed verbose DB URL logging for production cleanliness

engine = create_engine(settings.database_url, pool_pre_ping=True, pool_recycle=3600)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Developer convenience: for SQLite dev DBs, ensure tables exist
if engine.dialect.name == "sqlite":
    try:
        Base.metadata.create_all(bind=engine)
    except Exception:
        # Non-fatal in case of race or permission issues; migrations can still run
        pass


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()



