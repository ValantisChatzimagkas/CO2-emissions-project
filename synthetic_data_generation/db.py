import os

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL


def get_engine() -> Engine:
    """Build an engine from the DB_* environment variables."""
    url = URL.create(
        "postgresql+psycopg2",
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )
    return create_engine(url)
