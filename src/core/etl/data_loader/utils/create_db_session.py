import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common_utils.config.config_loader import get_config


def create_db_session() -> sessionmaker:
    """Create and return a sql session factory."""
    config = get_config()["db"]
    database_url = f"postgresql://{config['url']}:{config['port']}/{config['db_name']}"
    try:
        engine = create_engine(
            database_url,
            connect_args={
                "connect_timeout": 5,
            },
        )
        logging.info(f"Successfully connected to: {database_url}")
        return sessionmaker(bind=engine)
    except Exception as e:
        logging.error(f"Failed to connect to sql: {str(e)}")
        raise
