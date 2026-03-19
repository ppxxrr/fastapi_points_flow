from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.engine import URL


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env", override=False)


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    database_url: str
    echo: bool = False


def _build_default_sqlite_url() -> str:
    sqlite_path = ROOT_DIR / "data" / "member_module.db"
    return f"sqlite:///{sqlite_path.as_posix()}"


def build_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return database_url

    mysql_host = os.getenv("MYSQL_HOST", "").strip()
    mysql_database = os.getenv("MYSQL_DATABASE", "").strip()
    if mysql_host and mysql_database:
        mysql_port = int(os.getenv("MYSQL_PORT", "3306"))
        mysql_user = os.getenv("MYSQL_USER", "root")
        mysql_password = os.getenv("MYSQL_PASSWORD", "")
        return URL.create(
            "mysql+pymysql",
            username=mysql_user,
            password=mysql_password,
            host=mysql_host,
            port=mysql_port,
            database=mysql_database,
        ).render_as_string(hide_password=False)

    return _build_default_sqlite_url()


@lru_cache(maxsize=1)
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings(
        database_url=build_database_url(),
        echo=os.getenv("DB_ECHO", "false").lower() == "true",
    )

