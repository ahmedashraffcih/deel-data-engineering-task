from dotenv import load_dotenv
import os

load_dotenv()

# Source DB Configuration
SOURCE_DB = os.getenv("SOURCE_DB")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD")
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT")

# Destination DB Configuration
DEST_DB = os.getenv("DEST_DB")
DEST_DB_USER = os.getenv("DEST_DB_USER")
DEST_DB_PASSWORD = os.getenv("DEST_DB_PASSWORD")
DEST_DB_HOST = os.getenv("DEST_DB_HOST")
DEST_DB_PORT = os.getenv("DEST_DB_PORT")
