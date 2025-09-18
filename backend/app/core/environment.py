import os
from typing import Optional
import logging

from dotenv import load_dotenv


class EnvironmentHelper:
    """Helper class to manage environment configuration and loading."""

    def __init__(self):
        self.environment = self._detect_environment()
        self._load_environment_config()
        self._debug_environment()

    def _detect_environment(self) -> str:
        """Detect the current environment from ECS or default to development."""
        if os.getenv("ECS_CONTAINER_METADATA_URI"):
            return os.getenv("ENVIRONMENT", "staging").lower()
        else:
            return "development"

    def _load_environment_config(self):
        """Load the appropriate .env file based on detected environment."""
        try:
            if self.environment == "production":
                load_dotenv(dotenv_path=".env.production")
            elif self.environment == "staging":
                load_dotenv(dotenv_path=".env.staging")
            else:
                load_dotenv(dotenv_path=".env")
        except Exception:
            # dotenv not installed or file missing, skip
            pass

    def _debug_environment(self):
        """Debug and log current environment configuration."""
        logging.info("=== ENVIRONMENT DEBUG INFO ===")
        logging.info(f"ECS Container: {bool(os.getenv('ECS_CONTAINER_METADATA_URI'))}")
        logging.info(f"Environment: {os.getenv('ENVIRONMENT', 'NOT_SET')}")
        logging.info(f"Available env vars: {list(os.environ.keys())}")

        # Log all environment variables (masked for security)
        for key, value in os.environ.items():
            if any(secret_word in key.lower() for secret_word in ["key", "secret", "password", "token", "auth"]):
                logging.info(f"{key}: {'*' * len(value)} (MASKED)")
            else:
                logging.info(f"{key}: {value}")
        logging.info("=== END DEBUG INFO ===")

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get an environment variable value."""
        return os.getenv(key, default)

    def get_required(self, key: str) -> str:
        """Get a required environment variable, raise error if missing."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"{key} is missing in environment variables!")
        return value

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"

    @property
    def is_staging(self) -> bool:
        """Check if running in staging environment."""
        return self.environment == "staging"

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"

    @property
    def is_local(self) -> bool:
        """Check if running locally (not in ECS)."""
        return not os.getenv("ECS_CONTAINER_METADATA_URI")


# Create a global instance of the helper
env = EnvironmentHelper()


