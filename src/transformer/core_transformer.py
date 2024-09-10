import copy
from datetime import datetime
from pathlib import Path

from transformer.transformer_utils import Tables, traverse_dictionary_update_table_dict
from utils.file_handling.file_handling import save_json_dict
from utils.file_handling.file_parsing.general_parser import get_all_documents_with_path

# MISSING
# Institution -> GET dataProviders.url -> name, country
# References

source_schema = {
    "id": {
        "table": Tables.PUBLICATIONS.value,
        "column": "id_original",
        "type": str,
    },
    "arxivId": {
        "table": Tables.PUBLICATIONS.value,
        "column": "arxiv_id",
        "type": str,
    },
    "title": {
        "table": Tables.PUBLICATIONS.value,
        "column": "title",
        "type": str,
    },
    "publishedDate": {
        "table": Tables.PUBLICATIONS.value,
        "column": "publication_date",
        "type": datetime,
    },
    "journals[_].title": {
        "table": Tables.PUBLICATIONS.value,
        "column": "journal",
        "type": str,
    },
    "abstract": {
        "table": Tables.PUBLICATIONS.value,
        "column": "abstract",
        "type": str,
    },
    "fullText": {
        "table": Tables.PUBLICATIONS.value,
        "column": "full_text",
        "type": str,
    },
    "authors[_].name": {
        "table": Tables.PEOPLE.value,
        "column": "name",
        "type": str,
    },
    "fieldOfStudy": {
        "table": Tables.TOPICS.value,
        "column": "original_name",
        "type": str,
    },
    "doi": {
        "table": Tables.DOIS.value,
        "column": "doi",
        "type": str,
    },
}

table_schema = {
    Tables.PUBLICATIONS.value: {
        "id": None,
        "arxivId": None,
        "title": None,
        "publishedDate": None,
        "journals[_].title": None,
        "abstract": None,
        "fullText": None,
    },
    Tables.PEOPLE.value: {
        "authors[_].name": None,
    },
    Tables.TOPICS.value: {
        "fieldOfStudy": None,
    },
    Tables.DOIS.value: {
        "doi": None,
    },
}


def run_transformer():
    input_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/core_LBLBcomputingANDculturalRBORLBcomputingANDheritageRBRB"
    )
    for doc_idx, (document, path) in enumerate(get_all_documents_with_path(input_path)):
        table_dict = copy.deepcopy(table_schema)
        traverse_dictionary_update_table_dict(document, table_dict, source_schema)

        # save_json_dict(
        #     table_dict,
        #     Path(
        #         f"/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/rmme/core_rmme_{doc_idx}.json"
        #     ),
        # )
        # if doc_idx > 10:
        #     break


if __name__ == "__main__":
    run_transformer()
