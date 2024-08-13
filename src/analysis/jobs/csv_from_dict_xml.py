import csv
from collections import defaultdict
from typing import Dict, Any, List

from analysis.jobs.analysis_interface import IAnalysisJob
from utils.file_handling.file_parser.xml_parser import get_full_xml_as_dict_recursively


class CSVFromDictXmlAnalysis(IAnalysisJob):
    """
    A job to process XML files and convert them to CSV format based on specified column headers.
    This job processes all XML files in the given query's data directory and creates a CSV file
    where each row corresponds to an XML file, and columns are based on the provided header list.
    """

    MAX_LIST_ITEMS = 10

    def __init__(self, query_name: str, column_headers: List[str]):
        super().__init__("csv_from_dict_xml_analysis", query_name)
        self.full_column_headers = column_headers
        self.column_headers = [header.split(".")[-1] for header in column_headers]
        self.rows = []
        self.max_occurrences = defaultdict(int)

    def run(self) -> None:
        for idx, xml_dict in enumerate(
            get_full_xml_as_dict_recursively(self.data_path)
        ):
            row = self.process_dict(xml_dict)
            self.rows.append(row)
            if idx % 10_000 == 0:
                print(f"Processed {idx} files")

        # Update column headers based on max occurrences
        self.update_column_headers()

    def process_dict(self, data: Dict[str, Any]) -> Dict[str, List[str]]:
        row = defaultdict(list)
        for full_header, header in zip(self.full_column_headers, self.column_headers):
            values = self.get_nested_values(data, full_header.split("."))
            for i, value in enumerate(values[: self.MAX_LIST_ITEMS]):
                cleaned_value = self.clean_value(value)
                row[f"{header}_{i+1}" if i > 0 else header].append(cleaned_value)
                self.max_occurrences[header] = max(self.max_occurrences[header], i + 1)
        return row

    def get_nested_values(self, data: Dict[str, Any], keys: List[str]) -> List[Any]:
        if not keys:
            return [data] if data is not None else []

        key, *remaining_keys = keys
        if key.startswith("@"):
            # Handle attributes
            if isinstance(data, dict) and key[1:] in data:
                return self.get_nested_values(data[key[1:]], remaining_keys)
        elif isinstance(data, dict):
            if key in data:
                return self.get_nested_values(data[key], remaining_keys)
        elif isinstance(data, list):
            # Handle multiple occurrences, limiting to MAX_LIST_ITEMS
            values = []
            for item in data[: self.MAX_LIST_ITEMS]:
                values.extend(self.get_nested_values(item, keys))
            return values

        return []

    def clean_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, dict):
            if "#text" in value:
                value = value["#text"]
            else:
                return str(value)
        return str(value).replace("\n", " ").replace(",", " ")

    def update_column_headers(self) -> None:
        new_headers = []
        for header in self.column_headers:
            new_headers.append(header)
            for i in range(
                2, min(self.max_occurrences[header], self.MAX_LIST_ITEMS) + 1
            ):
                new_headers.append(f"{header}_{i}")
        self.column_headers = new_headers

    def save_output(self) -> None:
        output_file = self.output_path / f"{self.analysis_name}_results.csv"
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self.column_headers)
            for row in self.rows:
                csv_row = []
                for header in self.column_headers:
                    csv_row.append(row[header][0] if header in row else "")
                writer.writerow(csv_row)
        print(f"Results saved to {output_file}")


if __name__ == "__main__":
    # ID, date, names, doi, classification, extra
    query_name = "cordis_contenttypeISprojectANDSTAR"
    column_headers = [
        "project.id",
        "project.startDate",
        "project.relations.associations.organization.legalName",
        "project.identifiers.grantDoi",
        "project.relations.associations.organization.relations.categories.category.@classification",
        "project.relations.associations.organization.relations.categories.category.displayCode.#text",
        "project.objective",
    ]
    job = CSVFromDictXmlAnalysis(query_name, column_headers)
    job.run()
    job.save_output()

    # ID, published, author, arxiv:doi, author
    query_name = "arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB"
    column_headers = [
        "ns0:entry.ns0:id",
        "ns0:entry.ns0:published",
        "ns0:entry.ns1:doi",
        "ns0:entry.category_term",
        "ns0:entry.ns0:author.ns0:name",
    ]
    job = CSVFromDictXmlAnalysis(query_name, column_headers)
    job.run()
    job.save_output()
