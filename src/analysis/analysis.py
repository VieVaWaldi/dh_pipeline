from utils.config_loader import get_config
from utils.web_requests import get_base_url
from utils.xml_parser import get_all_elements_as_dict


def print_total_number_weblinks(path: str):
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


def main():
    config = get_config()
    # pile_dir = f"{config['data_path']}OLD_extractors/cordis_TEST/"
    pile_dir = "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/cordis_culturalORheritage/last_startDate_2010-01-01"

    print_total_number_weblinks(pile_dir)

    # Total number of record datasets


if __name__ == "__main__":
    main()
