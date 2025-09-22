from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from app.core.environment import env


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Component DB envs (optional). If present, used to build DATABASE_URL
    db_driver: Optional[str] = Field(default=None, validation_alias="DB_DRIVER")
    db_user: Optional[str] = Field(default=None, validation_alias="DB_USER")
    db_password: Optional[str] = Field(default=None, validation_alias="DB_PASSWORD")
    db_host: Optional[str] = Field(default=None, validation_alias="DB_HOST")
    db_port: Optional[str] = Field(default=None, validation_alias="DB_PORT")
    db_name: Optional[str] = Field(default=None, validation_alias="DB_NAME")

    # Direct URL fallback
    database_url: str = Field(
        default="mysql+pymysql://ace:ace_pw@db:3306/ace_sem",
        validation_alias="DATABASE_URL",
    )
    scrapingbee_api_key: Optional[str] = Field(
        default=None, validation_alias="SCRAPINGBEE_API_KEY"
    )
    cors_allow_origins: list[str] | str = Field(
        default=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ],
        validation_alias="CORS_ALLOW_ORIGINS",
    )
    openai_api_key: Optional[str] = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1")
    
    # Airtable integration
    airtable_pat: Optional[str] = Field(default=None, validation_alias="AIRTABLE_PAT")
    airtable_base_id: Optional[str] = Field(default=None, validation_alias="AIRTABLE_BASE_ID")
    airtable_table_id: Optional[str] = Field(default=None, validation_alias="AIRTABLE_TABLE_ID")


def _build_database_url_from_parts() -> Optional[str]:
    driver = env.get("DB_DRIVER")
    user = env.get("DB_USER")
    password = env.get("DB_PASSWORD")
    host = env.get("DB_HOST")
    port = env.get("DB_PORT")
    name = env.get("DB_NAME")

    if not driver or not host or not name:
        return None

    if driver.startswith("sqlite"):
        # Allow file path style for sqlite (e.g., DB_NAME=./data/dev.db)
        return f"sqlite:///{name}"

    auth = ""
    if user:
        auth = user
        if password:
            auth += f":{password}"
        auth += "@"

    port_part = f":{port}" if port else ""
    return f"{driver}://{auth}{host}{port_part}/{name}"


settings = Settings()

if isinstance(settings.cors_allow_origins, str):
    settings.cors_allow_origins = [
        o.strip() for o in settings.cors_allow_origins.split(",") if o.strip()
    ]

# Prefer DB parts if present; else keep DATABASE_URL
_db_url_from_parts = _build_database_url_from_parts()
if _db_url_from_parts:
    settings.database_url = _db_url_from_parts

# Normalize CORS origins: drop trailing slashes and whitespace
settings.cors_allow_origins = [o.rstrip("/") for o in settings.cors_allow_origins]



