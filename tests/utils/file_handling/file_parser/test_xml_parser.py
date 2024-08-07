import unittest

from utils.file_handling.file_handling import get_root_path
from utils.file_handling.file_parser.xml_parser import (
    get_all_elements_as_dict_recursively,
    extract_element_as_dict,
)
from utils.web_requests.web_requests import get_base_url


class TestXMLParser(unittest.TestCase):

    def setUp(self) -> None:
        self.data_dir = (
            get_root_path()
            / "tests"
            / "utils"
            / "file_handling"
            / "file_parser"
            / "test_xml"
        )

    def test_get_all_checkpoint_elements(self):
        """
        This tests get_new_checkpoint_from_data from cordis.
        """
        date_gen = get_all_elements_as_dict_recursively(self.data_dir, "startDate")
        count = 0
        for date in date_gen:
            count += 1
        assert count == 2

    def test_get_all_weblinks(self):
        """
        This tests download_and_save_attachments from cordis.
        """
        eu_links = []
        dicts_gen = extract_element_as_dict(self.data_dir / "test_1.xml", "webLink")
        for dict in dicts_gen:
            if get_base_url(dict["webLink"]["physUrl"]) == "europa.eu":
                eu_links.append(dict["webLink"]["physUrl"])

        assert len(eu_links) == 39


if __name__ == "__main__":
    unittest.main()
