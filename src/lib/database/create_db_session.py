import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from utils.config.config_loader import get_config
from utils.error_handling.error_handling import log_and_exit


def create_db_session() -> sessionmaker:
    """Create and return a transformation session factory."""
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
        log_and_exit(f"Failed to connect to transformation: ", e)
