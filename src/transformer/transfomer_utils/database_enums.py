from enum import Enum, auto
from typing import Union


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class Tables(AutoName):
    projects = auto()
    funding_programmes = auto()
    publications = auto()
    people = auto()
    institutions = auto()
    topics = auto()
    dois = auto()
    weblinks = auto()


class ProjectCols(AutoName):
    id = auto()


class FundingProgrammeCols(AutoName):
    id = auto()


class PublicationCols(AutoName):
    id_original = auto()
    arxiv_id = auto()
    title = auto()
    publication_date = auto()
    journal = auto()
    abstract = auto()
    summary = auto()
    full_text = auto()


class PeopleCols(AutoName):
    name = auto()


class InstitutionCols(AutoName):
    name = auto()
    type = auto()
    country = auto()


class TopicCols(AutoName):
    original_name = auto()
    custom_name = auto()


class DoiCols(AutoName):
    doi = auto()


class WeblinkCols(AutoName):
    link = auto()


ColumnTypes = Union[
    ProjectCols,
    FundingProgrammeCols,
    PublicationCols,
    PeopleCols,
    InstitutionCols,
    TopicCols,
    DoiCols,
    WeblinkCols,
]


class FieldInfo:
    def __init__(
        self,
        table: Tables,
        column: ColumnTypes,
        field_type: type,
        is_list: bool = False,
    ):
        self.table = table
        self.column = column
        self.type = field_type
        self.is_list = is_list
