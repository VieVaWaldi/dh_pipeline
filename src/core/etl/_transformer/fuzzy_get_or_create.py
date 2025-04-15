import logging
from typing import Tuple, Any, Dict, Type

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from datamodels.digicher.entities import (
    Projects,
    Institutions,
    ResearchOutputs,
    FundingProgrammes,
)
from .fuzzy_search import (
    fuzzy_search_projects,
    fuzzy_search_institutions,
    fuzzy_search_research_outputs,
    fuzzy_search_funding_programmes,
)
from core.etl.data_loader.utils.get_or_create import ModelCreationMonitor


class ModelUpdateMonitor:
    _stats = {}
    _conflicts = {}

    @classmethod
    def record(cls, model_name: str, field_name: str, old_value: Any, new_value: Any):
        """Record a conflict during model update.

        Args:
            model_name: Name of the model class
            field_name: Name of the field with conflict
            old_value: Original value in the database
            new_value: New value that was attempted to be set
        """
        if model_name not in cls._stats:
            cls._stats[model_name] = 0
            cls._conflicts[model_name] = {}

        cls._stats[model_name] += 1

        if field_name not in cls._conflicts[model_name]:
            cls._conflicts[model_name][field_name] = []

        cls._conflicts[model_name][field_name].append(
            {"old": old_value, "new": new_value}
        )

        logging.warning(
            f"Update conflict for {model_name}.{field_name}: "
            f"existing='{old_value}', new='{new_value}'"
        )

    @classmethod
    def log_stats(cls):
        """Log the accumulated conflict statistics for all models."""
        logging.info("=== Model Update Conflict Statistics ===")
        for model_name, count in cls._stats.items():
            conflicts_by_field = cls._conflicts[model_name]
            field_counts = {
                field: len(conflicts) for field, conflicts in conflicts_by_field.items()
            }

            logging.info(f"{model_name:<30} Total conflicts: {count:>7}")
            for field, field_count in field_counts.items():
                logging.info(f"  {field:<28} Conflicts: {field_count:>7}")


def _get_model_info(model):
    """Get the appropriate fuzzy search function and name field for a model.

    Args:
        model: The model class

    Returns:
        tuple: (fuzzy_search_func, unique_field_name, name_field)
    """
    if model == Projects:
        return fuzzy_search_projects, "id_original", "title"
    elif model == Institutions:
        return fuzzy_search_institutions, "name", "name"
    elif model == ResearchOutputs:
        return fuzzy_search_research_outputs, "id_original", "title"
    elif model == FundingProgrammes:
        return fuzzy_search_funding_programmes, "code", "title"
    else:
        raise ValueError(f"Unsupported model for fuzzy search: {model.__name__}")


def _update_fields_if_none(instance, updates):
    """Update fields in instance that are currently None with values from updates.

    Args:
        instance: Model instance to update
        updates: Dict of field:value pairs to potentially update

    Returns:
        bool: True if any updates were made
    """
    made_updates = False

    for field, new_value in updates.items():
        if new_value is not None:  # Only try to update if we have a value
            current_value = getattr(instance, field, None)

            if current_value is None:
                # Field is None, so update it
                setattr(instance, field, new_value)
                made_updates = True
            elif current_value != new_value:
                # There's a conflict between existing and new value
                ModelUpdateMonitor.record(
                    instance.__tablename__, field, current_value, new_value
                )

    return made_updates


def fuzzy_get_or_create(
    session: Session,
    model: Type,
    search_term: str,
    unique_key: Dict[str, Any],
    max_distance_percent: float = 0.15,
    **kwargs,
) -> Tuple[Any, bool]:
    """Get an existing instance using fuzzy search or create a new one.

    Args:
        session: SQLAlchemy session
        model: The model class (Projects, Institutions, etc.)
        search_term: The text to search for using fuzzy matching
        unique_key: Dict containing the unique identifier(s) to use if creating new instance
        max_distance_percent: Maximum distance percentage for fuzzy search
        **kwargs: Additional fields to use when creating a new instance

    Returns:
        Tuple[Any, bool]: (instance, is_created)
    """
    try:
        fuzzy_search_func, unique_field, name_field = _get_model_info(model)

        # First, try exact match with unique_key if provided
        if unique_key:
            instance = session.scalar(select(model).filter_by(**unique_key))
            if instance:
                _update_fields_if_none(instance, kwargs)
                session.add(instance)
                ModelCreationMonitor.record(model.__tablename__, is_created=False)
                return instance, False

        # Next, try fuzzy search if we have a search term
        if search_term:
            results = fuzzy_search_func(session, search_term, max_distance_percent)

            if results and len(results) > 0:
                # Get the first (best) match
                best_match_name, distance = results[0]

                # Find the instance with this name
                name_filter = {name_field: best_match_name}
                instance = session.scalar(select(model).filter_by(**name_filter))

                if instance:
                    _update_fields_if_none(instance, {**unique_key, **kwargs})
                    session.add(instance)
                    ModelCreationMonitor.record(model.__tablename__, is_created=False)
                    return instance, False

        # If we didn't find a match, create a new instance
        create_args = {**unique_key, **kwargs}
        instance = model(**create_args)
        session.add(instance)
        ModelCreationMonitor.record(model.__tablename__, is_created=True)
        return instance, True

    except SQLAlchemyError as e:
        logging.error(f"Database error with {model.__tablename__}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error with {model.__tablename__}: {str(e)}")
        raise
