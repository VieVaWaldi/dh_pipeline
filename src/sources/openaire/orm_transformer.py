from typing import List
from datetime import datetime

from sqlalchemy.orm import Session

from core.etl.transformer.get_or_create import get_or_create
from datamodels.digicher.entities import (
    Projects,
    FundingProgrammes,
    Dois,
    Topics,
    Institutions,
    Weblinks,
    ProjectsTopics,
    ProjectsFundingProgrammes,
    ProjectsInstitutions,
    ProjectsWeblinks,
)
from enrichment.search_geolocations import search_geolocation
from interfaces.i_orm_transformer import IORMTransformer
from sources.openaire.data_objects import OpenaireProject, Organization, Subject


class OpenaireORMTransformer(IORMTransformer):
    """Transforms OpenaireProject into ORM models with proper relationships"""

    def __init__(self, session: Session):
        super().__init__(session)

    def map_to_orm(self, openaire_project: OpenaireProject):
        """Main entry point to transform an OpenaireProject into ORM models"""

        """ 1. Create primary entities """
        project = self._create_project(openaire_project)
        doi = self._create_doi(openaire_project.doi)

        # Extract funding programme from call_identifier
        funding_programmes = []
        if openaire_project.call_identifier:
            funding_programme = self._create_funding_programme(
                openaire_project.call_identifier
            )
            if funding_programme:
                funding_programmes.append(funding_programme)

        # Handle subjects as topics
        topics = self._create_topics(openaire_project.subjects)

        # Create institution entities from organizations
        institutions = self._create_institutions(openaire_project.organizations)

        # Create weblink if website_url exists
        weblinks = []
        if openaire_project.website_url:
            weblink = self._create_weblink(
                openaire_project.website_url, "Project Website"
            )
            if weblink:
                weblinks.append(weblink)

        self.session.flush()  # Get all serial IDs for the instances

        """ 2. Create relationships """
        if doi is not None:
            project.doi = doi

        self._create_project_funding_programmes(project, funding_programmes)
        self._create_project_topics(project, topics)
        self._create_project_institutions(
            project, openaire_project.organizations, institutions
        )
        self._create_project_weblinks(project, weblinks)

    def _create_project(self, openaire_project: OpenaireProject) -> Projects:
        """Create or get Projects instance from OpenaireProject"""
        unique_key = {"id_original": openaire_project.id_openaire}

        # Convert datetime fields, handling possible None values
        start_date = openaire_project.start_date
        end_date = openaire_project.end_date

        args = {
            "title": openaire_project.title,
            "acronym": openaire_project.acronym if openaire_project.acronym else "",
            "start_date": start_date,
            "end_date": end_date,
            "total_cost": (
                int(openaire_project.total_cost) if openaire_project.total_cost else 0
            ),
            "ec_max_contribution": (
                int(openaire_project.funded_amount)
                if openaire_project.funded_amount
                else 0
            ),
            "objective": openaire_project.summary if openaire_project.summary else "",
            "call_identifier": (
                openaire_project.call_identifier
                if openaire_project.call_identifier
                else ""
            ),
            # Fields that don't have direct mapping - setting defaults
            "status": "",
            "ec_signature_date": None,
            "call_title": "",
            "call_rcn": "",
        }

        instance, _ = get_or_create(self.session, Projects, unique_key, **args)
        return instance

    def _create_doi(self, doi: str) -> Dois | None:
        """Create or get Dois instance"""
        if not doi:
            return None

        unique_key = {"doi": doi}
        instance, _ = get_or_create(self.session, Dois, unique_key)
        return instance

    def _create_funding_programme(self, call_identifier: str) -> FundingProgrammes:
        """Extract and create funding programme from call identifier"""
        if not call_identifier or "-" not in call_identifier:
            return None

        # Extract framework programme (e.g., H2020) and code (e.g., EINFRA-2017-1)
        parts = call_identifier.split("-", 1)
        framework_programme = parts[0]
        code = call_identifier  # Use full call_identifier as code

        unique_key = {"code": code}
        args = {
            "title": call_identifier,
            "short_title": call_identifier,
            "framework_programme": framework_programme,
            "pga": "",
            "rcn": 0,
        }

        instance, _ = get_or_create(self.session, FundingProgrammes, unique_key, **args)
        return instance

    def _create_topics(self, subjects: List[Subject]) -> List[Topics]:
        """Create topics from subjects"""
        topics = []

        for subject in subjects:
            # For OpenAire, we'll use the subject value as the topic name
            name = subject.value if hasattr(subject, "value") else str(subject)

            unique_key = {"name": name}
            args = {
                "standardised_name": name,
                "level": None,  # Default level as specified
            }

            instance, _ = get_or_create(self.session, Topics, unique_key, **args)
            topics.append(instance)

        return topics

    def _create_institutions(
        self, organizations: List[Organization]
    ) -> List[Institutions]:
        """Create institutions from organizations"""
        institutions = []

        for org in organizations:
            name = org.legal_name

            # Skip if no name
            if not name:
                continue

            unique_key = {"name": name}

            # Handle geolocation, which might be None
            geolocation = None
            if org.geolocation:
                try:
                    geolocation = [float(x) for x in org.geolocation]
                except (TypeError, ValueError):
                    # If conversion fails, keep geolocation as None
                    geolocation = None

            args = {
                "address_geolocation": geolocation,
                "short_name": org.legal_short_name if org.legal_short_name else None,
                "url": org.website_url if org.website_url else "",
                "address_country": (
                    org.country.code if org.country and org.country.code else None
                ),
                # Fields without direct mapping - setting defaults
                "sme": None,
                "address_street": "",
                "address_postbox": "",
                "address_postalcode": "",
                "address_city": "",
                "vat_number": "",
                "updated_at": datetime.now(),
            }

            instance, _ = get_or_create(self.session, Institutions, unique_key, **args)

            if instance.address_geolocation is None:
                # Extract geolocation, which open aire does not provide
                result = search_geolocation(instance)
                geolocation = (
                    [float(result["latitude"]), float(result["longitude"])]
                    if result["latitude"]
                    else None
                )
                instance.address_geolocation = geolocation

            institutions.append(instance)

        return institutions

    def _create_weblink(self, url: str, name: str = "") -> Weblinks:
        """Create weblink from URL"""
        if not url:
            return None

        unique_key = {"link": url}
        args = {"name": name}

        instance, _ = get_or_create(self.session, Weblinks, unique_key, **args)
        return instance

    def _create_project_funding_programmes(
        self, project: Projects, funding_programmes: List[FundingProgrammes]
    ):
        """Create ProjectsFundingProgrammes relationships"""
        for programme in funding_programmes:
            if not programme:
                continue

            unique_key = {
                "project_id": project.id,
                "fundingprogramme_id": programme.id,
            }

            get_or_create(self.session, ProjectsFundingProgrammes, unique_key)

    def _create_project_topics(self, project: Projects, topics: List[Topics]):
        """Create ProjectsTopics relationships"""
        for topic in topics:
            unique_key = {"project_id": project.id, "topic_id": topic.id}
            get_or_create(self.session, ProjectsTopics, unique_key)

    def _create_project_institutions(
        self,
        project: Projects,
        organizations: List[Organization],
        institutions: List[Institutions],
    ):
        """Create ProjectsInstitutions relationships"""
        for idx, (org, institution) in enumerate(zip(organizations, institutions)):
            # Set institution type based on is_first_listed flag
            inst_type = "coordinator" if org.is_first_listed else "participant"

            unique_key = {"project_id": project.id, "institution_id": institution.id}
            args = {
                "institution_position": idx,
                "type": inst_type,
                # Fields without direct mapping - setting defaults
                "ec_contribution": 0,
                "net_ec_contribution": 0,
                "total_cost": 0,
                "organization_id": org.id if org.id else "",
                "rcn": 0,
            }

            get_or_create(self.session, ProjectsInstitutions, unique_key, **args)

    def _create_project_weblinks(self, project: Projects, weblinks: List[Weblinks]):
        """Create ProjectsWeblinks relationships"""
        for weblink in weblinks:
            if not weblink:
                continue

            unique_key = {"project_id": project.id, "weblink_id": weblink.id}
            get_or_create(self.session, ProjectsWeblinks, unique_key)
