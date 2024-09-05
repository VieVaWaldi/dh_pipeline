import argparse
import csv
import json
from collections import OrderedDict
from pathlib import Path
from typing import List, Dict

from utils.file_handling.file_parsing.json_parser import (
    get_json_as_dict_recursively,
)


def process_json_documents(json_docs: List[Dict], output_file: str):
    headers = []
    rows = []

    def sanitize_value(value):
        if isinstance(value, str):
            return value.replace(";", ",").replace("\n", " ").replace("\r", "")
        return value

    def flatten_dict(d, prefix=""):
        items = OrderedDict()
        for k, v in d.items():
            new_key = f"{prefix}{k}"
            if isinstance(v, dict):
                items.update(flatten_dict(v, f"{new_key}."))
            elif isinstance(v, list):
                items[new_key] = json.dumps([sanitize_value(item) for item in v])
            else:
                items[new_key] = sanitize_value(v)
        return items

    def process_document(json_dict):
        flattened = flatten_dict(json_dict)

        # Update headers
        new_headers = [header for header in flattened.keys() if header not in headers]
        headers.extend(new_headers)

        # Create row with correct positioning
        row = OrderedDict((header, "") for header in headers)
        row.update(flattened)

        return row

    # Process all documents
    for doc in json_docs:
        rows.append(process_document(doc))

    # Write to CSV
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class CoreTransformer:  # (IExtractor, ABC)
    def __init__(self):
        # super().__init__(extractor_name, checkpoint_name)
        pass

    def transformation(self):
        # for each folder in order!
        path = Path(
            "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/_checkpoint/core_LBLBcomputingANDculturalRBORLBcomputingANDheritageRBRB/last_publishedDate_2024-07-26"
        )
        docs = []
        for document in get_json_as_dict_recursively(path):
            # header, values = xml_to_csv_row(document)
            docs.append(document)

        process_json_documents(docs, "core.csv")


def start_transformation():
    transformer = CoreTransformer()
    return transformer.transformation()


def main():
    parser = argparse.ArgumentParser(description="Run Core transformer")
    # parser.add_argument(
    #     "-r",
    #     "--run_id",
    #     type=int,
    #     default=0,
    #     help="Run ID to use from the config (default: 0)",
    # )
    # args = parser.parse_args()
    # load_dotenv()
    # config = get_query_config()["arxiv"]
    start_transformation()


if __name__ == "__main__":
    main()
