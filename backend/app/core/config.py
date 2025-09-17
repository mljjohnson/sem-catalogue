from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = Field(
        default="mysql+pymysql://ace:ace_pw@db:3306/ace_sem",
        validation_alias="DATABASE_URL",
    )
    scrapingbee_api_key: Optional[str] = Field(
        default=None, validation_alias="SCRAPINGBEE_API_KEY"
    )
    cors_allow_origins: list[str] | str = Field(
        default=["http://localhost:3000", "http://127.0.0.1:3000"],
        validation_alias="CORS_ALLOW_ORIGINS",
    )
    openai_api_key: Optional[str] = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1")


settings = Settings()

if isinstance(settings.cors_allow_origins, str):
    settings.cors_allow_origins = [
        o.strip() for o in settings.cors_allow_origins.split(",") if o.strip()
    ]



