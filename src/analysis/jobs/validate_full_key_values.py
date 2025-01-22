import logging

from analysis.utils.analysis_interface import IAnalysisJob
from core.etl.transformer.utils import ensure_list, get_nested
from core.file_handling.file_parsing.general_parser import yield_all_documents


class ValidateFullKeyValues(IAnalysisJob):
    """
    This script is here to help me figure out if we really dont have any topics
    for orgas and results. Or other relations.
    """

    def __init__(self, analysis_name, query_name):
        super().__init__(analysis_name, query_name)

        self.result_topic_set = set()
        self.result_orgas_set = set()
        self.orgas_topic_set = set()
        self.orgas_people_set = set()

    def run(self) -> None:
        for document, path in yield_all_documents(self.data_path):
            self.cordis_result_topic(document)
            self.cordis_result_orga(document)
            self.cordis_orga_topics(document)
            self.cordis_orga_people(document)

        logging.info("Result - Topic: \n", self.result_topic_set)
        logging.info("Result - Orgas: \n", self.result_orgas_set)
        logging.info("Orgas  - Topics: \n", self.orgas_topic_set)
        logging.info("Orgas  - People: \n", self.orgas_people_set)

    def cordis_result_topic(self, document):
        results = ensure_list(
            get_nested(document, "project.relations.associations.result")
        )
        for result in results:
            categories = ensure_list(
                get_nested(result, "relations.categories.category")
            )
            for category in categories:
                self.result_topic_set.add(category["@classification"])

    def cordis_result_orga(self, document):
        # "project.relations.associations.result[_].relations.associations.organization.legalName"

        results = ensure_list(
            get_nested(document, "project.relations.associations.result")
        )
        for result in results:
            orga = get_nested(result, "relations.associations.organization.legalName")
            self.result_orgas_set.add(orga)

    def cordis_orga_topics(self, document):
        # "project.relations.associations.organization[_].relations.categories.category.@classification"

        orgas = ensure_list(
            get_nested(document, "project.relations.associations.organization")
        )
        for orga in orgas:
            topic = get_nested(orga, "relations.categories.category.@classification")
            self.orgas_topic_set.add(topic)

    def cordis_orga_people(self, document):
        # "project.relations.associations.organization[_].relations.associations.person.firstNames"

        orgas = ensure_list(
            get_nested(document, "project.relations.associations.organization")
        )
        for orga in orgas:
            person = get_nested(orga, "relations.associations.person.firstNames")
            self.orgas_people_set.add(person)


if __name__ == "__main__":
    job = ValidateFullKeyValues("validate_full_keys", "cordis_culturalORheritage")
    job.run()
