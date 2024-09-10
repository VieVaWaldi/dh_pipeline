import copy
from datetime import datetime
from pathlib import Path

# from transformer.transformer_utils import Tables, traverse_dictionary_update_table_dict
from utils.file_handling.file_handling import save_json_dict
from utils.file_handling.file_parsing.general_parser import get_all_documents_with_path

# MISSING


PRA = "project.relations.associations."

# source_schema = {
#     PRA
#     + "article[_].@source": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].@type": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].archivedDate": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].availableLanguages.#text": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].availableLanguages.@readOnly": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].contentUpdateDate": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].id": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].rcn": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].teaser": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
#     PRA
#     + "article[_].title": {
#         "table": Tables.PUBLICATIONS.value,
#         "column": "i",
#         "type": str,
#     },
# }
#
# table_schema = {
#     Tables.PUBLICATIONS.value: {
#         PRA + "article[_].@source": None,
#         PRA + "article[_].@type": None,
#         PRA + "article[_].archivedDate": None,
#         PRA + "article[_].availableLanguages.#text": None,
#         PRA + "article[_].availableLanguages.@readOnly": None,
#         PRA + "article[_].contentUpdateDate": None,
#         PRA + "article[_].id": None,
#         PRA + "article[_].rcn": None,
#         PRA + "article[_].teaser": None,
#         PRA + "article[_].title": None,
#     }
# }


def run_transformer():
    cordis_only_project_flag = True
    input_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/_checkpoint/cordis_culturalORheritage"
    )
    for doc_idx, (document, path) in enumerate(
        get_all_documents_with_path(input_path, cordis_only_project_flag)
    ):
        # table_dict = copy.deepcopy(table_schema)
        # traverse_dictionary_update_table_dict(document, table_dict, source_schema)
        save_json_dict(
            document,
            Path(
                f"/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/rmme/cordis/THIS_{doc_idx}.json"
            ),
        )
        break
        # if doc_idx > 10:
        #     break


if __name__ == "__main__":
    run_transformer()
