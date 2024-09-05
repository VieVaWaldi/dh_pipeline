from pathlib import Path
from typing import Dict, Any
import pandas as pd

from utils.file_handling.file_parsing.xml_parser import get_xml_as_dict_recursively

# WAIT, author list is kinda missing here .............
# are the lists flattened?

source_schema = {
    "ns0:entry.ns0:id": str,  # id
    "ns0:entry.ns0:published": str,  # published
    "ns0:entry.ns0:title": str,
    "ns0:entry.ns1:journal_ref": str,
    "ns0:entry.ns0:summary": str,
    "ns0:entry.ns0:author.ns0:name": str,  # author primary
    # cant really give topics a priority when they are in their own table right?
    "ns0:entry.ns1:primary_category.@term": str,  # primary category
    "ns0:entry.ns0:category.@term": str,  # secondary category ?
    "ns0:entry.category_term": str,  # secondary category ?
    "ns0:entry.ns1:doi": str,
    "ns0:entry.ns0:link.@href": str,
}


table_schema = {
    "publications": {
        "id": "-1",
        "source": "-1",
        "id_original": "ns0:entry.ns0:id",
        "title": "ns0:entry.ns0:title",
        "publication_date": "ns0:entry.ns0:published",
        "journal": "ns0:entry.ns1:journal_ref",
        "abstract": "ns0:entry.ns0:summary",
        "full_text": "-1",
    },
    "people": {
        "id": "-1",
        "source": "-1",
        "name": "ns0:entry.ns0:author.ns0:name",
    },
    # What happens if i enter the same topic with the same prio again?
    # i think we have to search it before we insert it
    "topics": {
        "id": "-1",
        "source": "-1",
        "original_name": "ns0:entry.ns1:primary_category.@term",
        "custom_name": "-1",
        "priority": "-1",
    },
    "dois": {
        "id": "-1",
        "source": "-1",
        "doi": "ns0:entry.ns1:doi",
    },
    "weblinks": {
        "id": "-1",
        "source": "-1",
        "link": "ns0:entry.ns0:link.@href",
    },
}


def extract_value(data: Dict[str, Any], key_path: str):
    value = data
    for key in key_path.split("."):
        if isinstance(value, dict) and key in value:
            value = value[key]
        elif isinstance(value, list) and key.isdigit():
            value = value[int(key)] if int(key) < len(value) else None
        else:
            return None
    return value


def extract_document_values(
    document: Dict[str, Any], schema: Dict[str, type]
) -> Dict[str, Any]:
    """
    Takes a schema dict with "full_key": "datatype" and returns a dict of all full_keys
    with their respective values.
    """
    extracted = {}
    for key, expected_type in schema.items():
        value = extract_value(document, key)
        if value is not None and isinstance(value, expected_type):
            extracted[key] = value
        elif isinstance(value, list):
            extracted[key] = [v for v in value if isinstance(v, expected_type)]
    return extracted


def process_document(
    document: Dict[str, Any],
    source_schema: Dict[str, type],
    table_schema: Dict[str, Dict[str, str]],
) -> Dict[str, pd.DataFrame]:
    extracted_values = extract_document_values(document, source_schema)
    table_data = {table: {} for table in table_schema}

    for table, columns in table_schema.items():
        for col, source_key in columns.items():
            if source_key in extracted_values:
                value = extracted_values[source_key]
                if isinstance(value, list):
                    table_data[table][col] = value
                else:
                    table_data[table][col] = [value]

    # Handle list expansion for details and related items
    max_length = max(
        len(v)
        for values in table_data.values()
        for v in values.values()
        if isinstance(v, list)
    )

    for table, data in table_data.items():
        for col, values in data.items():
            if not isinstance(values, list):
                table_data[table][col] = [values[0]] * max_length
            elif len(values) < max_length:
                table_data[table][col] = values + [None] * (max_length - len(values))

    return {table: pd.DataFrame(data) for table, data in table_data.items()}


def main():

    dataframe_tables = {table: pd.DataFrame() for table in table_schema}

    # for one checkpoint folder run
    input_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/_checkpoint/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    )

    # for each document and pdf
    for document in get_xml_as_dict_recursively(input_path):

        dict = extract_document_values(document, source_schema)
        print("h")

        # document_dataframes = process_document(document, source_schema, table_schema)

        # Append new data to existing DataFrames
        # for table, df in document_dataframes.items():
        #     dataframes[table] = pd.concat([dataframe_tables[table], df], ignore_index=True)

    # Print sample of results
    # for table, df in dataframe_tables.items():
    #     print(f"\n{table}:")
    #     print(df.head())
    #     print(f"Total rows: {len(df)}")

    # Optionally, save DataFrames to CSV
    # output_dir = Path("arxiv.csv")  # Replace with your desired output directory
    # output_dir.mkdir(parents=True, exist_ok=True)
    # for table, df in dataframe_tables.items():
    #     df.to_csv(output_dir / f"{table}.csv", index=False)


if __name__ == "__main__":
    main()
