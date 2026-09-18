"""Application settings loaded from environment variables / .env file.

Day 1-2 (Week 1): Secure API key management via pydantic-settings.
Secrets are never hard-coded; they are read from the environment or a
gitignored `.env` file (see `.env.example`).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application settings (validated by Pydantic)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- AWS / S3 raw data lake ----
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "us-east-1"
    aws_s3_bucket: str | None = None

    # ---- Stripe ----
    stripe_api_key: str | None = None
    stripe_api_base: str = "https://api.stripe.com"

    # ---- Salesforce ----
    salesforce_username: str | None = None
    salesforce_password: str | None = None
    salesforce_security_token: str | None = None
    salesforce_login_url: str = "https://login.salesforce.com"

    # ---- Pipeline ----
    log_level: str = "INFO"
    extraction_page_size: int = 100

    @property
    def stripe_configured(self) -> bool:
        return bool(self.stripe_api_key)

    @property
    def salesforce_configured(self) -> bool:
        return all(
            [
                self.salesforce_username,
                self.salesforce_password,
                self.salesforce_security_token,
            ]
        )

    @property
    def s3_configured(self) -> bool:
        return bool(self.aws_s3_bucket)


@lru_cache
def get_settings() -> Settings:
    """Cached settings accessor (importable anywhere in the pipeline)."""
    return Settings()
