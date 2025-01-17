from typing import List, Tuple

from sqlalchemy.orm import Session

from datamodels.digicher.entities import (
    ResearchOutputs,
    People,
    Institutions,
    Dois,
    Topics,
    Weblinks,
)
from datamodels.digicher.junctions import (
    InstitutionsResearchOutputs,
    InstitutionsPeople,
    ResearchOutputsWeblinks,
    ResearchOutputsTopics,
    ResearchOutputsPeople,
)
from sources.arxiv.arxiv_transform_obj import ArxivEntry
from core.etl.dataloader import get_or_create


class ArxivTransformOrm:
    """Transforms ArxivEntry into ORM models with proper relationships"""

    def __init__(self, session: Session, sanitizer):
        self.session = session
        self.sanitizer = sanitizer

    def map_to_orm(self, entry: ArxivEntry):
        # 1. Create primary entities
        research_output = self._create_research_output(entry)
        doi = self._create_doi(entry)
        topics = self._create_topics(entry)
        people, institutions = self._create_people_and_institutions(entry)
        weblinks = self._create_weblinks(entry)

        self.session.flush()

        # 2. One to One
        if doi:
            research_output.doi = doi

        # 3. Create junction records
        self._create_research_outputs_people(research_output, people)
        self._create_research_outputs_topics(research_output, topics)
        self._create_research_outputs_weblinks(research_output, weblinks)
        self._create_institutions_people(people, institutions, entry.authors)
        self._create_institutions_research_outputs(research_output, institutions)

    def _create_research_output(self, entry: ArxivEntry) -> ResearchOutputs:
        unique_key = {"id_original": entry.id}
        args = {
            "type": "publication",
            "title": entry.title,
            "publication_date": self.sanitizer.clean_date(entry.published_date),
            "journal": entry.journal_ref,
            "summary": entry.summary,
            "comment": entry.comment,
            "arxiv_id": entry.id,
        }
        instance, _ = get_or_create(self.session, ResearchOutputs, unique_key, **args)
        # ToDo:
        # if not _:
        #     # update existing research_output
        return instance

    def _create_doi(self, entry: ArxivEntry) -> Dois:
        if not entry.doi:
            return None
        unique_key = {"doi": entry.doi}
        instance, _ = get_or_create(self.session, Dois, unique_key)
        return instance

    def _create_topics(self, entry: ArxivEntry) -> List[Topics]:
        topics = []
        unique_key = {"name": entry.categories.primary_category}
        primary_topic, _ = get_or_create(self.session, Topics, unique_key)
        topics.append(primary_topic)

        for topic_name in entry.categories.categories:
            unique_key = {"name": topic_name}
            topic, _ = get_or_create(self.session, Topics, unique_key)
            topics.append(topic)
        return topics

    def _create_people_and_institutions(
        self, entry: ArxivEntry
    ) -> Tuple[List[People], List[Institutions]]:
        people = []
        institutions = []

        for author in entry.authors:
            person, _ = get_or_create(self.session, People, {"name": author.name})
            people.append(person)

            for institution_name in author.affiliations:
                institution, _ = get_or_create(
                    self.session, Institutions, {"name": institution_name}
                )
                institutions.append(institution)

        return people, institutions

    def _create_weblinks(self, entry: ArxivEntry) -> List[Weblinks]:
        weblinks = []
        for weblink in entry.weblinks:
            unique_key = {"link": weblink.href}
            args = {"name": weblink.title}
            instance, _ = get_or_create(self.session, Weblinks, unique_key, **args)
            weblinks.append(instance)
        return weblinks

    def _create_research_outputs_people(
        self, research_output: ResearchOutputs, people: List[People]
    ):
        for idx, person in enumerate(people):
            unique_key = {
                "publication_id": research_output.id,
                "person_id": person.id,
            }
            args = {"person_position": idx}
            get_or_create(self.session, ResearchOutputsPeople, unique_key, **args)

    def _create_research_outputs_topics(
        self, research_output: ResearchOutputs, topics: List[Topics]
    ):
        for idx, topic in enumerate(topics):
            unique_key = {
                "publication_id": research_output.id,
                "topic_id": topic.id,
            }
            args = {"is_primary": (idx == 0)}
            get_or_create(self.session, ResearchOutputsTopics, unique_key, **args)

    def _create_research_outputs_weblinks(
        self, research_output: ResearchOutputs, weblinks: List[Weblinks]
    ):
        for weblink in weblinks:
            unique_key = {
                "publication_id": research_output.id,
                "weblink_id": weblink.id,
            }
            get_or_create(self.session, ResearchOutputsWeblinks, unique_key)

    def _create_institutions_people(
        self, people: List[People], institutions: List[Institutions], authors: List
    ):
        for person, author in zip(people, authors):
            for institution in institutions:
                if institution.name in author.affiliations:
                    unique_key = {
                        "institution_id": institution.id,
                        "person_id": person.id,
                    }
                    get_or_create(self.session, InstitutionsPeople, unique_key)

    def _create_institutions_research_outputs(
        self, research_output: ResearchOutputs, institutions: List[Institutions]
    ):
        for institution in institutions:
            unique_key = {
                "institution_id": institution.id,
                "publication_id": research_output.id,
            }
            get_or_create(self.session, InstitutionsResearchOutputs, unique_key)
