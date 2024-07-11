import os
import xml.etree.ElementTree as eT
from typing import List, Dict
from urllib.parse import urlparse

from extractors.non_contextual_transformations import download_and_save_cordis_pdfs
from utils.logger import log_and_raise_exception, setup_logging


def get_all_elements_texts(file_path: str, element_name: str) -> List[str]:
    """
    Searches all files in all subdirectories under the file_path for element_name.
    Returns a list of the text of all found elements.
    """
    return process_xml_files(
        file_path, lambda fp: extract_element_text(fp, element_name)
    )


# Not really used right now! BUT AMAZING FOR analysing stuff later.
# Can use this to dict the whole file right?
def get_all_elements_as_dict(file_path: str, element_name: str) -> List[Dict[str, str]]:
    """
    Searches all files in all subdirectories under the file_path for element_name.
    Returns a list of dictionaries for each element in each file with the given element_name.
    """
    return process_xml_files(
        file_path, lambda fp: extract_element_as_dict(fp, element_name)
    )


def process_xml_files(file_path: str, extraction_func) -> List:
    """
    Walks through all XML files in the given file_path and applies the extraction_func to each.
    """
    all_values = []
    for root, _, files in os.walk(file_path):
        for file in files:
            if file.endswith(".xml"):
                full_file_path = os.path.join(root, file)
                values = extraction_func(full_file_path)
                all_values.extend(values)
    return all_values


def extract_element_text(file_path: str, element_name: str) -> List[str]:
    """
    Extracts the text of the specified element from an XML file.
    """
    try:
        tree = eT.parse(file_path)
        root = tree.getroot()
        return [
            elem.text
            for elem in root.iter()
            if elem.tag == element_name and elem.text is not None
        ]
    except eT.ParseError:
        log_and_raise_exception(f"Error parsing XML file: {file_path}")
        return []


def extract_element_as_dict(file_path: str, element_name: str) -> List[Dict[str, str]]:
    """
    Extracts the specified element from an XML file as a dictionary.
    """
    try:
        tree = eT.parse(file_path)
        root = tree.getroot()
        return [
            element_to_dict(elem) for elem in root.iter() if elem.tag == element_name
        ]
    except eT.ParseError:
        log_and_raise_exception(f"Error parsing XML file: {file_path}")
        return []


def element_to_dict(element: eT.Element) -> Dict[str, str]:
    """
    Converts an XML element to a dictionary.
    """
    result = {
        "tag": element.tag,
        "text": element.text.strip() if element.text else None,
    }
    result.update(element.attrib)
    for child in element:
        child_dict = element_to_dict(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = [result[child.tag], child_dict]
        else:
            result[child.tag] = child_dict
    return result


if __name__ == "__main__":

    """
    # Example to find all elements with the same tag in all files.

    date_elements = get_all_elements_texts(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/OLD_extractors/cordis_TEST/last_startDate_2020-01-01",
        "startDate",
    )
    print(date_elements)
    """

    # Example to find all elements with the same tag in all files and turn them to dicts.
    linkpath = "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/OLD_extractors/cordis_TEST/"
    link_elements = get_all_elements_as_dict(
        linkpath,
        "webLink",
    )
    download_path = "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/rmme"

    for dic in link_elements:
        url = dic["physUrl"]["text"]
        print(url)
        download_and_save_cordis_pdfs(
            url,
            download_path,
        )
