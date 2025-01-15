import unittest

from core.file_handling import get_root_path
from core.file_handling.file_parsing.xml_parser import (
    extract_element_as_dict,
    extract_xml_as_dict,
    get_all_elements_text_recursively,
)
from core.web_requests import get_base_url


class TestXMLParser(unittest.TestCase):

    def setUp(self) -> None:
        self.data_dir = (
            get_root_path()
            / "tests"
            / "utils"
            / "file_handling"
            / "file_parsing"
            / "test_xml"
        )

    def test_get_all_checkpoint_elements(self):
        """
        This tests get_new_checkpoint_from_data from cordis.
        """
        date_gen = get_all_elements_text_recursively(self.data_dir, "startDate")
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
        for dic in dicts_gen:
            if get_base_url(dic["webLink"]["physUrl"]) == "europa.eu":
                eu_links.append(dic["webLink"]["physUrl"])

        assert len(eu_links) == 39

    def test_get_dict_from_xml_file(self):
        xml_dict = extract_xml_as_dict(self.data_dir / "test_1.xml")
        print(xml_dict)


if __name__ == "__main__":
    unittest.main()
