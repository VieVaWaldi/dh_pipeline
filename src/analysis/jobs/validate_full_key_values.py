import logging
from itertools import islice

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
        self.result_orgas_set = []
        self.orgas_topic_set = set()
        self.orgas_people_set = []

    def run(self) -> None:
        for idx, (document, path) in enumerate(yield_all_documents(self.data_path)):
            self.cordis_result_topic(document)
            self.cordis_result_orga(document)
            self.cordis_orga_topics(document)
            self.cordis_orga_people(document)

            if idx % 1000 == 0:
                print("processed ", idx+1, " elements ...")

        print("Result - Topic: \n", self.result_topic_set)
        print("Result - Orgas: \n", len(self.result_orgas_set))
        print(list(islice(self.result_orgas_set, 30)))

        print("Orgas  - Topics: \n", self.orgas_topic_set)
        print("Orgas  - People: \n", len(self.orgas_people_set))
        print(list(islice(self.orgas_people_set, 30)))

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
            self.result_orgas_set.append(orga)

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
            self.orgas_people_set.append(person)


if __name__ == "__main__":
    job = ValidateFullKeyValues("validate_full_keys", "cordis_culturalORheritage")
    job.run()
