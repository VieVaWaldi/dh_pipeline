import csv
from collections import defaultdict
from typing import Dict, Any, List

from analysis.utils.analysis_interface import IAnalysisJob
from analysis.utils.analysis_utils import clean_value
from common_utils.file_handling.file_parsing.general_parser import yield_all_documents


class CSVFromDict(IAnalysisJob):
    """
    A job to process dictionaries and convert them to CSV format given a list of full keys.
    Use @items_per_list to set the amount of columns and values to extract
    for lists which have multiple items.
    If a list is part of the full_key treat it as a normal variable without [].
    """

    def __init__(
        self,
        query: str,
        column_keys: List[str],
        items_per_list: int,
        max_row_width: int = 50,
        cordis_only_project_flag: bool = False,
    ):
        super().__init__("csv_from_dict", query)
        self.full_keys = column_keys
        # self.full_keys = [header.split(".")[-2] for header in column_keys]
        self.rows = []

        self.items_per_list = items_per_list
        self.occurrences_per_column = defaultdict(int)

        self.max_row_width = max_row_width
        self.cordis_only_project_flag = cordis_only_project_flag

    def run(self) -> None:
        for idx, (document, path) in enumerate(
            yield_all_documents(self.data_path, self.cordis_only_project_flag)
        ):
            self.rows.append(self.extract_values_from_columns(document))

            if idx % 10_000 == 0:
                print(f"Processed {idx} files")

        self.expand_columns()
        self.save_output()

    def extract_values_from_columns(
        self, document: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        row = defaultdict(list)

        for full_key, column in zip(self.full_keys, self.full_keys):
            values = self.get_all_nested_values(document, full_key.split("."))

            for i, value in enumerate(values[: self.items_per_list]):
                self.add_value_to_row(
                    row, column, clean_value(str(value)[: self.max_row_width]), i
                )

        return row

    def get_all_nested_values(
        self, document: Dict[str, Any], keys: List[str]
    ) -> List[Any]:
        if not keys:
            return [document] if document is not None else []

        key, *remaining_keys = keys

        if isinstance(document, dict):
            if key in document:
                return self.get_all_nested_values(document[key], remaining_keys)

        elif isinstance(document, list):
            values = []
            for item in document[: self.items_per_list]:
                values.extend(self.get_all_nested_values(item, keys))
            return values

        return []

    def add_value_to_row(self, row: Dict, column: str, value: str, row_occurence: int):
        row[f"{column}_{row_occurence+1}" if row_occurence > 0 else column].append(
            value
        )
        self.occurrences_per_column[column] = max(
            self.occurrences_per_column[column], row_occurence + 1
        )

    def expand_columns(self) -> None:
        expanded_columns = []
        for key in self.full_keys:
            expanded_columns.append(key)
            for i in range(
                2,
                min(self.occurrences_per_column[key], self.items_per_list) + 1,
            ):
                expanded_columns.append(f"{key}_{i}")
        self.full_keys = expanded_columns

    def save_output(self) -> None:
        output_file = self.output_path / f"{self.analysis_name}_results.csv"
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(self.full_keys)
            for row in self.rows:
                csv_row = []
                for header in self.full_keys:
                    csv_row.append(row[header][0] if header in row else "")
                writer.writerow(csv_row)
        print(f"Results saved to {output_file}")


def run_arxiv():
    columns = [
        # "ns0:entry.ns0:id",
        # "ns0:entry.ns1:doi",
        "ns0:entry.ns0:published",
        # "ns0:entry.ns0:title",
        # "ns0:entry.ns0:author",
        # "ns0:entry.ns0:author.ns0:name",
        # "ns0:entry.ns0:link.@href",
        # "ns0:entry.ns1:primary_category.@term",
        # "ns0:entry.ns0:category.@term",
        # "ns0:entry.category_term",
        # "ns0:entry.ns0:author.ns1:affiliation",
    ]
    job = CSVFromDict(
        "arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB",
        columns,
        items_per_list=3,
    )
    job.run()


def run_cordis():
    columns = [
        # # VISUAL MAP
        # # General
        # "project.title",
        # "project.totalCost",
        # "project.relations.associations.programme[_].frameworkProgramme",
        # "project.relations.associations.programme.frameworkProgramme",
        # "project.objective",
        # # Time
        "project.startDate",
        # # Topic
        # "project.relations.categories.category[_].@classification", # needed for filtering: euroSciVoc
        # "project.relations.categories.category.@classification",  # needed for filtering: euroSciVoc
        # "project.relations.categories.category[_].title",
        # "project.relations.categories.category.title",
        # "project.keywords",
        # # Location
        # "project.relations.associations.organization.address.geolocation",
        # "project.relations.associations.organization.address.country",
        # "project.relations.associations.organization.address.city"
    ]
    job = CSVFromDict(
        # cordis_contenttypeISprojectANDSTAR
        "cordis_culturalORheritage",
        columns,
        items_per_list=3,
        cordis_only_project_flag=True,
    )
    job.run()


def run_core():
    columns = [
        "id",
        "arxivId",
        # "doi",
        # "magId",
        "publishedDate",
        # "authors.name",
        # "dataProviders.name",
        # "publisher",
        # "contributors",
        # "tags",
        # "subjects",
        # "fieldOfStudy",
        # "identifiers.identifier",
        # "identifiers.type",
        # "references.doi",
        # "references.title",
    ]
    job = CSVFromDict(
        "core_LBLBcomputingANDculturalRBORLBcomputingANDheritageRBRB",
        columns,
        items_per_list=3,
    )
    job.run()


if __name__ == "__main__":
    # run_arxiv()
    run_cordis()
    # run_core()
