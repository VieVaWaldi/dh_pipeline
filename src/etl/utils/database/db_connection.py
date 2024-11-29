from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common_utils.config.config_loader import get_config


def create_db_session() -> sessionmaker:
    """Create and return a database session factory."""
    config = get_config()["db"]
    database_url = f"postgresql://{config['url']}:{config['port']}/{config['db_name']}"
    try:
        engine = create_engine(
            database_url,
            connect_args={
                "connect_timeout": 5,
            },
        )
        return sessionmaker(bind=engine)
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        raise
