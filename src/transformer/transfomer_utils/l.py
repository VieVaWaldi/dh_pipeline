from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

from transformer.transfomer_utils.database_enums import (
    FieldInfo,
    Tables,
    PublicationCols,
    PeopleCols,
    InstitutionCols,
    TopicCols,
    DoiCols,
    WeblinkCols,
)
from transformer.transfomer_utils.transformer_utils import traverse_dictionary_update_table_dict
from utils.file_handling.file_handling import save_json_dict
from utils.file_handling.file_parsing.general_parser import get_all_documents_with_path

source_schema: Dict[str, FieldInfo] = {
    "ns0:entry.ns0:id": FieldInfo(
        Tables.publications, PublicationCols.id_original, str
    ),
    "ns0:entry.ns0:title": FieldInfo(Tables.publications, PublicationCols.title, str),
    "ns0:entry.ns0:published": FieldInfo(
        Tables.publications, PublicationCols.publication_date, datetime
    ),
    "ns0:entry.ns1:journal_ref": FieldInfo(
        Tables.publications, PublicationCols.journal, str
    ),
    "ns0:entry.ns0:summary": FieldInfo(
        Tables.publications, PublicationCols.summary, str
    ),
    "ns0:entry.ns0:author[_].ns0:name": FieldInfo(
        Tables.people, PeopleCols.name, str, is_list=True
    ),
    "ns0:entry.ns0:author.ns1:affiliation": FieldInfo(
        Tables.institutions, InstitutionCols.name, str
    ),
    "ns0:entry.ns0:category.@term": FieldInfo(
        Tables.topics, TopicCols.original_name, str
    ),
    "ns0:entry.ns1:doi": FieldInfo(Tables.dois, DoiCols.doi, str),
    "ns0:entry.ns0:link[_].@href": FieldInfo(
        Tables.weblinks, WeblinkCols.link, str, is_list=True
    ),
}


def create_empty_temp_dict(
    schema: Dict[str, FieldInfo]
) -> Dict[str, Dict[str, Union[None, List[Any]]]]:
    temp_dict = {}
    for original_key, field_info in schema.items():
        table_value = field_info.table.value  # Use the value of the enum
        if table_value not in temp_dict:
            temp_dict[table_value] = {}
        temp_dict[table_value][original_key] = [] if field_info.is_list else None
    return temp_dict


def run_transformer():
    input_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )
    for doc_idx, (document, path) in enumerate(get_all_documents_with_path(input_path)):
        table_dict = create_empty_temp_dict()
        traverse_dictionary_update_table_dict(document, table_dict, source_schema)
        save_json_dict(
            table_dict,
            Path(
                f"/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/rmme/arxiv/arxiv_{doc_idx}.json"
            ),
        )
        if doc_idx > 10:
            break


if __name__ == "__main__":
    run_transformer()
