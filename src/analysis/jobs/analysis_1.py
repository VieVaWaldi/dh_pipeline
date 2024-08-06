from collections import defaultdict
from pathlib import Path
from typing import Dict, Any

from utils.config.config_loader import get_config
from utils.file_handling.file_parser.xml_parser import get_all_elements_as_dict
from utils.web_requests.web_requests import get_base_url


def print_total_number_weblinks(path: str):
    """
    Returns the number of weblinks found in all xml files given the path.
    """
    link_elements = get_all_elements_as_dict(
        path,
        "webLink",
    )
    eu_links = [
        dic
        for dic in link_elements
        if get_base_url(dic["physUrl"]["text"]) == "europa.eu"
    ]
    print(
        f"Total number of all eu links and all weblinks {len(eu_links)} / {len(link_elements)} in {path}"
    )


def print_count_of_each_xml_tag(path: Path):
    """
    Returns all tags and subtags in all xmls given the path and counts them.
    Useful to figure out how much data is missing
    """
    all_elements = get_all_elements_as_dict(path, "project")
    tag_counts = defaultdict(int)
    for idx, element in enumerate(all_elements):
        tag_counts = _count_tags(element, counts=tag_counts)
        if idx % 1000 == 0:
            print(f"Record {idx}/{len(all_elements)}")
    _print_tag_counts(tag_counts)


def _count_tags(
    data: Dict[str, Any], parent: str = None, counts: Dict[str, int] = None
) -> Dict[str, int]:
    if counts is None:
        counts = defaultdict(int)
    for key, value in data.items():
        tag = f"{parent}.{key}" if parent else key
        counts[tag] += 1
        if isinstance(value, dict):
            _count_tags(value, tag, counts)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _count_tags(item, tag, counts)
    return counts


def _print_tag_counts(counts: Dict[str, int]):
    for tag, count in counts.items():
        print(f"{tag}: {count}")


def main():
    config = get_config()

    pile_dir = Path(
        config.get(
            "data_path",
            "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/cordis_contenttype=projectANDSTAR",
        )
    )

    print_count_of_each_xml_tag(pile_dir)


if __name__ == "__main__":
    main()
