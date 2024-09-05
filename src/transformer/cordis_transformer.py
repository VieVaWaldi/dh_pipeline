import argparse
from pathlib import Path

from utils.file_handling.file_parsing.xml_parser import get_xml_as_dict_recursively



class CordisTransformer:  # (IExtractor, ABC)
    def __init__(self):
        # super().__init__(extractor_name, checkpoint_name)
        pass

    def transformation(self):
        # for each folder in order!
        path = Path(
            "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/extractors/_checkpoint/cordis_culturalORheritage"
        )
        docs = []
        for document in get_xml_as_dict_recursively(path):
            # header, values = xml_to_csv_row(document)
            docs.append(document)

        # process_xml_documents(docs, "cordis.csv")
#

def start_transformation():
    transformer = CordisTransformer()
    return transformer.transformation()


def main():
    parser = argparse.ArgumentParser(description="Run Cordis transformer")
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
