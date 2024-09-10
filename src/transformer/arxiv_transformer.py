import copy
from datetime import datetime
from pathlib import Path

from transformer.utils.utils import (
    Tables,
    traverse_dictionary_update_table_dict,
)
from utils.file_handling.file_parsing.general_parser import get_all_documents_with_path

source_schema = {
    "ns0:entry.ns0:id": {
        "table": Tables.PUBLICATIONS.value,
        "column": "id_original",
        "type": str,
    },
    "ns0:entry.ns0:title": {
        "table": Tables.PUBLICATIONS.value,
        "column": "title",
        "type": str,
    },
    "ns0:entry.ns0:published": {
        "table": Tables.PUBLICATIONS.value,
        "column": "publication_date",
        "type": datetime,
    },
    "ns0:entry.ns1:journal_ref": {
        "table": Tables.PUBLICATIONS.value,
        "column": "journal",
        "type": str,
    },
    "ns0:entry.ns0:summary": {
        "table": Tables.PUBLICATIONS.value,
        "column": "summary",
        "type": str,
    },
    "ns0:entry.ns0:author[_].ns0:name": {
        "table": Tables.PEOPLE.value,
        "column": "name",
        "type": str,
    },
    "ns0:entry.ns0:author.ns1:affiliation": {
        "table": Tables.INSTITUTIONS.value,
        "column": "name",
        "type": str,
    },
    "ns0:entry.ns0:category.@term": {
        "table": Tables.TOPICS.value,
        "column": "original_name",
        "type": str,
    },
    "ns0:entry.ns1:doi": {"table": Tables.DOIS.value, "column": "doi", "type": str},
    "ns0:entry.ns0:link[_].@href": {
        "table": Tables.WEBLINKS.value,
        "column": "link",
        "type": str,
    },
}


table_schema = {
    Tables.PUBLICATIONS.value: {
        "ns0:entry.ns0:id": None,
        "ns0:entry.ns0:title": None,
        "ns0:entry.ns0:published": None,
        "ns0:entry.ns1:journal_ref": None,
        "ns0:entry.ns0:summary": None,
    },
    Tables.PEOPLE.value: {
        "ns0:entry.ns0:author.ns0:name": None,
    },
    Tables.INSTITUTIONS.value: {
        "ns0:entry.ns0:author.ns1:affiliation": None,
    },
    Tables.TOPICS.value: {
        "ns0:entry.ns1:primary_category.@term": None,
    },
    Tables.DOIS.value: {
        "ns0:entry.ns1:doi": None,
    },
    Tables.WEBLINKS.value: {
        "ns0:entry.ns0:link[_].@href": None,
    },
}


def run_transformer():
    input_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )
    for doc_idx, (document, path) in enumerate(get_all_documents_with_path(input_path)):
        table_dict = copy.deepcopy(table_schema)
        traverse_dictionary_update_table_dict(document, table_dict, source_schema)
        # save_json_dict(
        #     table_dict,
        #     Path(
        #         f"/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/rmme/rmme_{doc_idx}.json"
        #     ),
        # )
        # if doc_idx > 10:
        #     break


if __name__ == "__main__":
    run_transformer()
