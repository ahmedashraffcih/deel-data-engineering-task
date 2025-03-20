import time
from typing import Any, Dict, List, Optional, Tuple
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2
from psycopg2.extras import RealDictCursor
from config.settings import settings
from utils.logger import logger


Base = declarative_base()


def get_source_engine():
    """Create and return a SQLAlchemy engine for the source database."""
    return sa.create_engine(settings.get_source_db_uri())


def get_target_engine():
    """Create and return a SQLAlchemy engine for the target database."""
    return sa.create_engine(settings.get_target_db_uri())


def get_source_connection(
    retries: int = 3, delay: int = 5
) -> psycopg2.extensions.connection:
    """Create and return a connection to the source database with retry logic."""
    attempt = 0
    while attempt < retries:
        try:
            conn = psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                dbname=settings.DB_NAME,
                cursor_factory=RealDictCursor,
            )
            logger.info("Connected to source database")
            return conn
        except psycopg2.OperationalError as e:
            attempt += 1
            if attempt < retries:
                logger.warning(
                    f"Connection attempt {attempt} failed. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"Failed to connect to source database after {retries} attempts"
                )
                raise e


def get_target_connection(
    retries: int = 3, delay: int = 5
) -> psycopg2.extensions.connection:
    """Create and return a connection to the target database with retry logic."""
    attempt = 0
    while attempt < retries:
        try:
            conn = psycopg2.connect(
                host=settings.DEST_DB_HOST,
                port=settings.DEST_DB_PORT,
                user=settings.DEST_DB_USER,
                password=settings.DEST_DB_PASSWORD,
                dbname=settings.DEST_DB_NAME,
                cursor_factory=RealDictCursor,
            )
            logger.info("Connected to target database")
            return conn
        except psycopg2.OperationalError as e:
            attempt += 1
            if attempt < retries:
                logger.warning(
                    f"Connection attempt {attempt} failed. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"Failed to connect to target database after {retries} attempts"
                )
                raise e


def execute_query(
    conn: psycopg2.extensions.connection, query: str, params: Optional[Tuple] = None
) -> List[Dict[str, Any]]:
    """Execute a query and return the results as a list of dictionaries."""
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        if cursor.description:
            return cursor.fetchall()
        return []


def execute_batch(
    conn: psycopg2.extensions.connection, query: str, data: List[Dict[str, Any]]
) -> None:
    """Execute a batch operation on the database."""
    with conn.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor, query, data)
    conn.commit()
