from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.db.config import get_database_settings


def _ensure_sqlite_directory(database_url: str) -> None:
    url = make_url(database_url)
    if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:":
        return

    database_path = Path(url.database)
    database_path.parent.mkdir(parents=True, exist_ok=True)


def _build_engine():
    settings = get_database_settings()
    _ensure_sqlite_directory(settings.database_url)

    connect_args: dict[str, object] = {}
    url = make_url(settings.database_url)
    if url.get_backend_name() == "sqlite":
        connect_args["check_same_thread"] = False

    return create_engine(
        settings.database_url,
        echo=settings.echo,
        future=True,
        pool_pre_ping=url.get_backend_name() != "sqlite",
        connect_args=connect_args,
    )


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)


def get_db_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

