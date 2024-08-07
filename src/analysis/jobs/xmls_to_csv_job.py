import csv
from typing import Dict, Any, List
from pathlib import Path
from analysis.jobs.analysis_job_interface import IAnalysisJob
from utils.file_handling.file_parser.xml_parser import get_full_xml_as_dict_recursively

class XMLToCSVAnalysisJob(IAnalysisJob):
    """
    A job to process XML files and convert them to CSV format based on specified column headers.
    This job processes all XML files in the given query's data directory and creates a CSV file
    where each row corresponds to an XML file, and columns are based on the provided header list.
    Commas in cell values are replaced with "COM" to prevent CSV formatting issues.
    """

    def __init__(self, query_name: str, column_headers: List[str]):
        super().__init__("xml_to_csv_analysis", query_name)
        self.column_headers = column_headers
        self.rows = []

    def run(self) -> None:
        for xml_dict in get_full_xml_as_dict_recursively(self.data_path):
            row = self.process_dict(xml_dict)
            self.rows.append(row)

    def process_dict(self, data: Dict[str, Any]) -> Dict[str, str]:
        row = {}
        for header in self.column_headers:
            value = self.get_nested_value(data, header.split('.'))
            row[header] = self.format_value(value)
        return row

    def get_nested_value(self, data: Dict[str, Any], keys: List[str]) -> Any:
        for key in keys:
            if isinstance(data, dict):
                if key in data:
                    data = data[key]
                else:
                    return None
            elif isinstance(data, list) and key.isdigit():
                index = int(key)
                if 0 <= index < len(data):
                    data = data[index]
                else:
                    return None
            else:
                return None
        return data

    def format_value(self, value: Any) -> str:
        if isinstance(value, (dict, list)):
            return ""  # Ignore complex nested structures
        elif value is None:
            return ""
        else:
            # Replace commas with "COM"
            return str(value).replace(',', 'COM').replace('\n', ' ')

    def save_output(self) -> None:
        output_file = self.output_path / f"{self.analysis_name}_results.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.column_headers)
            writer.writeheader()
            for row in self.rows:
                writer.writerow(row)
        print(f"Results saved to {output_file}")

if __name__ == "__main__":
    query_name = "cordis_culturalORheritage"
    column_headers = [
        "project.id", "project.rcn", "project.acronym", "project.status",
        "project.title", "project.startDate", "project.endDate",
        "project.totalCost", "project.ecMaxContribution",
        "project.frameworkProgramme", "project.fundingScheme",
        "project.objective"
    ]
    job = XMLToCSVAnalysisJob(query_name, column_headers)
    job.run()
    job.save_output()