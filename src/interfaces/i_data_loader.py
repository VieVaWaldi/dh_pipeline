from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict

from sqlalchemy.orm import Session


class IDataLoader(ABC):
    """
    Base Class for all Data Loaders.
    Takes a dict and transform it into respective ORM Models.
    Load models into database using the session.
    """

    def __init__(self, path_to_file: Path):
        self.path_to_file = path_to_file

    @abstractmethod
    def load(self, session: Session, document: Dict):
        pass
