import logging

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


def get_or_create(session: Session, model, unique_key: dict, **kwargs):
    """Get an existing instance or create a new one.

    Args:
        session: SQLAlchemy session
        model: The model class to query
        unique_key: Dict containing the unique identifier(s) to search by
        **kwargs: Additional fields to use when creating a new instance
    """
    try:
        instance = session.scalar(select(model).filter_by(**unique_key))
        if instance:
            session.add(instance)
            logging.info(f"Instance {model}:{unique_key} was found.")
            return instance, False
        else:
            create_args = {**unique_key, **kwargs}
            instance = model(**create_args)
            session.add(instance)
            return instance, True

    except SQLAlchemyError as e:
        logging.error(f"Database error creating {model.__name__}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error creating {model.__name__}: {str(e)}")
        raise
