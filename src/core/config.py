import os
from logging import config as logging_config

from pydantic import BaseSettings

from src.core.logger import LOGGING

logging_config.dictConfig(LOGGING)


class Settings(BaseSettings):
    PROJECT_NAME: str = "movies"

    REDIS_HOST: str
    REDIS_PORT: int = 6379

    ELASTIC_HOST: str
    ELASTIC_PORT: int = 9200

    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


settings = Settings(_env_file=".env", _env_file_encoding="utf-8")
