import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "root"
    MYSQL_DB: str = "marketmind"
    
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_MODEL: str = "claude-sonnet-4-5"
    
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-5.4"
    
    XAI_API_KEY: str = ""
    XAI_MODEL: str = "grok-beta"
    
    AI_PROVIDER: str = "anthropic" # anthropic, openai, xai

    APP_ENV: str = "development"
    SECRET_KEY: str = "supersecretkey"
    ADMIN_PASSWORD: str = "admin"
    LOG_LEVEL: str = "INFO"

    @property
    def async_database_url(self) -> str:
        return f"mysql+aiomysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DB}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
