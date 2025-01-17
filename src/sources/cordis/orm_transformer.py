from typing import List

from sqlalchemy.orm import Session

from core.etl.transformer.get_or_create import get_or_create
from datamodels.digicher.entities import (
    Projects,
    FundingProgrammes,
    Dois,
    Topics,
    People,
    Institutions,
    Weblinks,
    ResearchOutputs,
)
from datamodels.digicher.junctions import (
    InstitutionsResearchOutputs,
    InstitutionsPeople,
    ResearchOutputsWeblinks,
    ResearchOutputsTopics,
    ResearchOutputsPeople,
    ProjectsTopics,
    ProjectsFundingProgrammes,
    ProjectsInstitutions,
    ProjectsWeblinks,
    ProjectsResearchOutputs,
)
from interfaces.i_orm_transformer import IORMTransformer
from sources.cordis.object_transformer import (
    CordisProject,
    FundingProgramme,
    Topic,
    Person,
    Institution,
    Weblink,
    ResearchOutput,
)


class CordisORMTransformer(IORMTransformer):
    """Transforms CordisProject into ORM models with proper relationships"""

    def __init__(self, session: Session):
        super().__init__(session)

    def map_to_orm(self, cordis_project: CordisProject):
        """Main entry point to transform a CordisProject into ORM models"""

        """ 1. Create primary entities Project """
        project = self._create_project(cordis_project)
        doi = self._create_doi(cordis_project.doi)
        fundingprogrammes = self._create_fundingprogrammes(
            cordis_project.fundingprogrammes
        )
        topics = self._create_topics(cordis_project.topics)
        institutions = self._create_institutions(cordis_project.institutions)
        weblinks = self._create_weblinks(cordis_project.weblinks)

        self.session.flush()  # Get all serial IDs for the instances

        """ 3. Create project relationships """
        if doi is not None:
            project.doi = doi

        self._create_project_fundingprogrammes(project, fundingprogrammes)
        self._create_project_topics(project, topics)
        self._create_project_institutions(
            project, cordis_project.institutions, institutions
        )
        self._create_project_weblinks(project, weblinks)

        """ 4. Create entities ResearchOutput """
        for research_output in cordis_project.research_outputs:
            output_orm = self._create_research_output(research_output)
            self._create_project_research_outputs(project, output_orm)

    def _create_project(self, cordis_project: CordisProject) -> Projects:
        """Create or get Projects instance"""
        unique_key = {"id_original": cordis_project.id_original}
        args = {
            "title": cordis_project.title,
            "acronym": cordis_project.acronym,
            "status": cordis_project.status,
            "start_date": cordis_project.start_date,
            "end_date": cordis_project.end_date,
            "ec_signature_date": cordis_project.ec_signature_date,
            "total_cost": cordis_project.total_cost,
            "ec_max_contribution": cordis_project.ec_max_contribution,
            "objective": cordis_project.objective,
            "call_identifier": cordis_project.call_identifier,
            "call_title": cordis_project.call_title,
            "call_rcn": cordis_project.call_rcn,
        }
        instance, _ = get_or_create(self.session, Projects, unique_key, **args)
        return instance

    def _create_fundingprogrammes(
        self, programmes: List[FundingProgramme]
    ) -> List[FundingProgrammes]:
        """Create or get FundingProgrammes instances"""
        orm_programmes = []
        for prog in programmes:
            unique_key = {"code": prog.code}
            args = {
                "title": prog.title,
                "short_title": prog.short_title,
                "framework_programme": prog.framework_programme,
                "pga": prog.pga,
                "rcn": prog.rcn,
            }
            instance, _ = get_or_create(
                self.session, FundingProgrammes, unique_key, **args
            )
            orm_programmes.append(instance)
        return orm_programmes

    def _create_doi(self, doi: str) -> Dois | None:
        """Create or get Dois instances"""
        if doi is None or doi == "":
            return None

        unique_key = {"doi": doi}
        instance, is_new = get_or_create(self.session, Dois, unique_key)
        return instance

    def _create_topics(self, topics: List[Topic]) -> List[Topics]:
        """Create or get Topics instances"""
        orm_topics = []
        for topic in topics:
            unique_key = {"name": topic.name}
            args = {"level": topic.level}
            instance, _ = get_or_create(self.session, Topics, unique_key, **args)
            orm_topics.append(instance)
        return orm_topics

    def _create_people(self, people: List[Person]) -> List[People]:
        """Create or get People instances"""
        orm_people = []
        for person in people:
            # Use full name if available, otherwise construct from parts
            name = person.name or f"{person.first_name} {person.last_name}".strip()
            if not name:
                continue

            unique_key = {"name": name}
            args = {"title": person.title, "telephone_number": person.telephone_number}
            instance, _ = get_or_create(self.session, People, unique_key, **args)
            orm_people.append(instance)
        return orm_people

    def _create_institutions(
        self, institutions: List[Institution]
    ) -> List[Institutions]:
        """Create or get Institutions instances"""
        orm_institutions = []
        for inst in institutions:
            unique_key = {"name": inst.name}
            args = {
                "sme": inst.sme if inst.sme is not None else False,
                "address_street": inst.address_street,
                "address_postbox": inst.address_postbox,
                "address_postalcode": inst.address_postalcode,
                "address_city": inst.address_city,
                "address_country": inst.address_country,
                "address_geolocation": inst.address_geolocation,
                "url": inst.url,
                "short_name": inst.short_name,
                "vat_number": inst.vat_number,
            }
            instance, _ = get_or_create(self.session, Institutions, unique_key, **args)

            # Handle institution's people
            if inst.people:
                people = self._create_people(inst.people)
                self._create_institutions_people(instance, people)

            orm_institutions.append(instance)
        return orm_institutions

    def _create_weblinks(self, weblinks: List[Weblink]) -> List[Weblinks]:
        """Create or get Weblinks instances"""
        orm_weblinks = []
        for weblink in weblinks:
            unique_key = {"link": weblink.url}
            args = {"name": weblink.title}
            instance, _ = get_or_create(self.session, Weblinks, unique_key, **args)
            orm_weblinks.append(instance)
        return orm_weblinks

    def _create_research_output(self, output: ResearchOutput) -> ResearchOutputs:
        """Create or get ResearchOutputs instance with all its relationships"""
        unique_key = {"id_original": output.id_original}
        args = {
            "type": output.type,
            "title": output.title,
            "publication_date": output.publication_date,
            "journal": output.journal,
            "summary": output.summary,
            "comment": output.comment,
        }
        instance, _ = get_or_create(self.session, ResearchOutputs, unique_key, **args)

        # Create and link related entities
        people = self._create_people(output.people)
        topics = self._create_topics(output.topics)
        weblinks = self._create_weblinks(output.weblinks)
        institutions = self._create_institutions(output.institutions)

        doi = self._create_doi(output.doi)

        self.session.flush()

        if doi is not None:
            # Removing this removes the error
            instance.doi = doi

        self._create_research_outputs_people(instance, people)
        self._create_research_outputs_topics(instance, topics)
        self._create_research_outputs_weblinks(instance, weblinks)
        self._create_institutions_research_outputs(instance, institutions)

        return instance

    def _create_project_fundingprogrammes(
        self, project: Projects, fundingprogrammes: List[FundingProgrammes]
    ):
        for fundingprogramme in fundingprogrammes:
            unique_key = {
                "project_id": project.id,
                "fundingprogramme_id": fundingprogramme.id,
            }
            get_or_create(
                self.session, ProjectsFundingProgrammes, unique_key, **unique_key
            )

    def _create_project_topics(self, project: Projects, topics: List[Topics]):
        """Create ProjectsTopics relationships"""
        for idx, topic in enumerate(topics):
            unique_key = {"project_id": project.id, "topic_id": topic.id}
            get_or_create(self.session, ProjectsTopics, unique_key)

    def _create_project_institutions(
        self,
        project: Projects,
        obj_institutions: List[Institution],
        institutions: List[Institutions],
    ):
        """Create ProjectsInstitutions relationships"""
        for pos, (obj_institution, institution) in enumerate(
            zip(obj_institutions, institutions)
        ):
            if obj_institution.name != institution.name:
                raise Exception(
                    "Trying to match institutions that dont belong together"
                )
            unique_key = {"project_id": project.id, "institution_id": institution.id}
            args = {
                "institution_position": pos,
                "ec_contribution": obj_institution.ec_contribution,
                "net_ec_contribution": obj_institution.net_ec_contribution,
                "total_cost": obj_institution.total_cost,
                "type": obj_institution.type,
                "organization_id": obj_institution.organization_id,
                "rcn": obj_institution.rcn,
            }
            get_or_create(self.session, ProjectsInstitutions, unique_key, **args)

    def _create_project_weblinks(self, project: Projects, weblinks: List[Weblinks]):
        """Create ProjectsWeblinks relationships"""
        for weblink in weblinks:
            unique_key = {"project_id": project.id, "weblink_id": weblink.id}
            get_or_create(self.session, ProjectsWeblinks, unique_key)

    def _create_project_research_outputs(
        self, project: Projects, research_output: ResearchOutputs
    ):
        """Create ProjectsResearchOutputs relationships"""
        unique_key = {"project_id": project.id, "publication_id": research_output.id}
        get_or_create(self.session, ProjectsResearchOutputs, unique_key)

    def _create_research_outputs_people(
        self, research_output: ResearchOutputs, people: List[People]
    ):
        """Create ResearchOutputsPeople relationships"""
        for idx, person in enumerate(people):
            unique_key = {"publication_id": research_output.id, "person_id": person.id}
            args = {"person_position": idx}
            get_or_create(self.session, ResearchOutputsPeople, unique_key, **args)

    def _create_research_outputs_topics(
        self, research_output: ResearchOutputs, topics: List[Topics]
    ):
        """Create ResearchOutputsTopics relationships"""
        for idx, topic in enumerate(topics):
            unique_key = {"publication_id": research_output.id, "topic_id": topic.id}
            get_or_create(self.session, ResearchOutputsTopics, unique_key)

    def _create_research_outputs_weblinks(
        self, research_output: ResearchOutputs, weblinks: List[Weblinks]
    ):
        """Create ResearchOutputsWeblinks relationships"""
        for weblink in weblinks:
            unique_key = {
                "publication_id": research_output.id,
                "weblink_id": weblink.id,
            }
            get_or_create(self.session, ResearchOutputsWeblinks, unique_key)

    def _create_institutions_people(
        self, institution: Institutions, people: List[People]
    ):
        """Create InstitutionsPeople relationships"""
        for person in people:
            unique_key = {"institution_id": institution.id, "person_id": person.id}
            get_or_create(self.session, InstitutionsPeople, unique_key)

    def _create_institutions_research_outputs(
        self, research_output: ResearchOutputs, institutions: List[Institutions]
    ):
        """Create InstitutionsResearchOutputs relationships"""
        for institution in institutions:
            unique_key = {
                "institution_id": institution.id,
                "publication_id": research_output.id,
            }
            get_or_create(self.session, InstitutionsResearchOutputs, unique_key)
