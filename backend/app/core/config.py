from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = Field(
        default="mysql+pymysql://ace:ace_pw@localhost:3307/ace_sem",
        validation_alias="DATABASE_URL",
    )
    scrapingbee_api_key: Optional[str] = Field(
        default=None, validation_alias="SCRAPINGBEE_API_KEY"
    )
    cors_allow_origins: list[str] | str = Field(
        default=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "https://sem-frontend-878144293804.europe-west1.run.app",
        ],
        validation_alias="CORS_ALLOW_ORIGINS",
    )
    openai_api_key: Optional[str] = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1")
    
    # Airtable settings
    airtable_pat: Optional[str] = Field(default=None, validation_alias="AIRTABLE_PAT")
    airtable_base_id: str = Field(default="appfGPddh3kvKXSkx", validation_alias="AIRTABLE_BASE_ID")
    airtable_table_id: str = Field(default="tbl5X32ZJvqrSwWaH", validation_alias="AIRTABLE_TABLE_ID")
    airtable_view_id: str = Field(default="viwvPKhpPQOlX3tCu", validation_alias="AIRTABLE_VIEW_ID")


settings = Settings()

if isinstance(settings.cors_allow_origins, str):
    settings.cors_allow_origins = [
        o.strip() for o in settings.cors_allow_origins.split(",") if o.strip()
    ]

# Normalize CORS origins: drop trailing slashes and whitespace
settings.cors_allow_origins = [o.rstrip("/") for o in settings.cors_allow_origins]



