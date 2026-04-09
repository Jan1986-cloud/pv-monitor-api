import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://localhost/pvmonitor")
    secret_key: str = os.getenv("SECRET_KEY", "change-me-in-production-use-random-64-chars")
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480
    entsoe_api_key: str = os.getenv("ENTSOE_API_KEY", "")
    # NL bidding zone for ENTSO-E
    bidding_zone: str = "10YNL----------L"
    port: int = int(os.getenv("PORT", "8000"))

    class Config:
        env_file = ".env"


settings = Settings()
