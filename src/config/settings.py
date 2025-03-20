import os
from typing import Optional
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()


class settings(BaseModel):

    DB_NAME: Optional[str] = os.getenv("SOURCE_DB")
    DB_USER: Optional[str] = os.getenv("SOURCE_DB_USER")
    DB_PASSWORD: Optional[str] = os.getenv("SOURCE_DB_PASSWORD")
    DB_HOST: Optional[str] = os.getenv("SOURCE_DB_HOST")
    DB_PORT: Optional[str] = os.getenv("SOURCE_DB_PORT")

    DEST_DB_NAME: Optional[str] = os.getenv("SOURCE_DB")
    DEST_DB_USER: Optional[str] = os.getenv("SOURCE_DB_USER")
    DEST_DB_PASSWORD: Optional[str] = os.getenv("SOURCE_DB_PASSWORD")
    DEST_DB_HOST: Optional[str] = os.getenv("SOURCE_DB_HOST")
    DEST_DB_PORT: Optional[str] = os.getenv("SOURCE_DB_PORT")

    def get_sourcedb_uri(self) -> str:
        """Return the database URI."""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    def get_dest_db_uri(self) -> str:
        """Return the database URI."""
        return (
            f"postgresql://{self.DEST_DB_USER}:{self.DEST_DB_PASSWORD}"
            f"@{self.DEST_DB_HOST}:{self.DEST_DB_PORT}/{self.DEST_DB_NAME}"
        )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    POLLING_INTERVAL: int = os.getenv("POLLING_INTERVAL")
    OUTPUT_DIRECTORY: str = os.getenv("OUTPUT_DIRECTORY")


settings = settings()
