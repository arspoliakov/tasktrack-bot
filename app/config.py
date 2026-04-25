from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "TaskTrack Bot"
    app_port: int = 8010
    app_base_url: str
    app_secret_key: str
    default_timezone: str = "Europe/Moscow"

    telegram_bot_token: str
    admin_telegram_id: int

    database_url: str

    deepinfra_api_key: str
    deepinfra_parser_model: str = "deepseek-ai/DeepSeek-V3.1"
    deepinfra_stt_model: str = "openai/whisper-large-v3-turbo"

    google_client_id: str
    google_client_secret: str
    google_redirect_uri: str


@lru_cache
def get_settings() -> Settings:
    return Settings()

