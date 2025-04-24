import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from core.data_loader.utils.dict_utils import ensure_list, get_nested
from core.data_loader.utils.get_or_create import get_or_create
from core.sanitizers.sanitizer import (
    parse_string,
    parse_geolocation,
    parse_bool,
    parse_float,
    parse_date,
    parse_titles_and_labels,
    parse_names_and_identifiers,
    parse_web_resources,
    parse_content,
)
from enrichment.ocr.pdf_ocr_reader import pdf_to_text
from interfaces.i_data_loader import IDataLoader
from sources.cordis.data_model import (
    Person,
    Topic,
    Weblink,
    FundingProgramme,
    Institution,
    ResearchOutput,
    Project,
    JunctionInstitutionPerson,
    JunctionResearchOutputPerson,
    JunctionResearchOutputTopic,
    JunctionResearchOutputWeblink,
    JunctionResearchOutputInstitution,
    JunctionProjectTopic,
    JunctionProjectWeblink,
    JunctionProjectFundingProgramme,
    JunctionProjectInstitution,
    JunctionProjectResearchOutput,
)

PROJECT = "project"
RELATIONS = "relations"
ASSOCIATIONS = "associations"
PRE = f"{RELATIONS}.{ASSOCIATIONS}"

RESULT_PATH = f"{PRE}.result"
ORG_PATH = f"{PRE}.organization"
PROGRAMME_PATH = f"{PRE}.programme"
CATEGORIES_PATH = f"{RELATIONS}.categories.category"


class CordisDataLoader(IDataLoader):
    """
    Data loader for Cordis documents.
    Transforms JSON documents into SQLAlchemy ORM models and adds them to the session
    """

    def __init__(self, path_to_file=None):
        super().__init__(path_to_file)
        self.lookup_main_topics = [
            "medical and health sciences",
            "natural sciences",
            "engineering and technology",
            "agricultural sciences",
            "social sciences",
            "humanities",
        ]

    def load(self, session: Session, document: Dict):
        assert (
            isinstance(document, dict) and document
        ), "document must be a non-empty dictionary"

        project_data = document.get(PROJECT, {})
        if not project_data:
            return

        project, created_project = self._create_project(session, project_data)
        # if not created_project:
        #     return

        topics = self._create_topics(session, project_data)
        session.flush()  # Can create the same topics again with research output, need flush for get_or_create

        weblinks = self._create_weblinks(session, project_data)
        funding_programmes = self._create_funding_programmes(session, project_data)
        institutions = self._create_institutions(session, project_data)
        research_outputs = self._create_research_outputs(session, project_data)

        session.flush()

        self._create_project_topics(session, project, topics)
        self._create_project_weblinks(session, project, weblinks)
        self._create_project_funding_programmes(session, project, funding_programmes)
        self._create_project_institutions(session, project, institutions, project_data)
        self._create_project_research_outputs(session, project, research_outputs)

    """ Entities """

    def _create_project(
        self, session: Session, project_data: Dict
    ) -> Tuple[Optional[Project], bool]:
        """Create or retrieve a Project entity from the document."""
        id_original = parse_string(project_data.get("id"))
        if not id_original:
            return None, False

        title = parse_titles_and_labels(project_data.get("title"))
        if not title:
            return None, False

        project, created = get_or_create(
            session,
            Project,
            {"id_original": id_original},
            doi=self._get_doi(project_data, "identifiers.grantDoi"),
            title=title,
            acronym=parse_string(project_data.get("acronym")),
            status=parse_string(project_data.get("status")),
            start_date=parse_date(project_data.get("startDate")),
            end_date=parse_date(project_data.get("endDate")),
            ec_signature_date=parse_date(project_data.get("ecSignatureDate")),
            total_cost=parse_float(project_data.get("totalCost")),
            ec_max_contribution=parse_float(project_data.get("ecMaxContribution")),
            objective=parse_content(project_data.get("objective")),
            call_identifier=self._get_call_info(project_data, "identifier"),
            call_title=self._get_call_info(project_data, "title"),
            call_rcn=self._get_call_info(project_data, "rcn"),
        )

        return project, created

    def _create_topics(
        self, session: Session, project_data: Dict
    ) -> List[Tuple[Topic, int]]:
        """Create or retrieve Topic entities and return with their levels."""
        topics_with_levels = []
        seen_topics = set()

        for cat_data in ensure_list(get_nested(project_data, CATEGORIES_PATH)):
            if cat_data.get("@classification") != "euroSciVoc":
                continue

            if cat_data.get("displayCode") is None or cat_data.get("code") is None:
                continue

            try:
                levels, display_codes = self._sanitize_euroscivoc_topics(
                    cat_data["code"],
                    (
                        cat_data["displayCode"]["#text"]
                        if isinstance(cat_data["displayCode"], dict)
                        else cat_data["displayCode"]
                    ),
                )

                for level, topic_name in zip(levels, display_codes):
                    if not topic_name:
                        continue
                    if topic_name in seen_topics:
                        continue
                    seen_topics.add(topic_name)

                    topic_name = parse_titles_and_labels(topic_name)
                    topic, _ = get_or_create(
                        session, Topic, {"name": topic_name}, level=level
                    )
                    topics_with_levels.append((topic, level))
            except Exception as e:
                continue

        return topics_with_levels

    def _create_weblinks(self, session: Session, project_data: Dict) -> List[Weblink]:
        """Create or retrieve Weblink entities."""
        weblinks = []

        for link_data in ensure_list(get_nested(project_data, f"{PRE}.webLink")):
            url = parse_web_resources(link_data.get("physUrl"))
            if not url:
                continue

            title = parse_titles_and_labels(link_data.get("title"))
            weblink, _ = get_or_create(session, Weblink, {"url": url}, title=title)
            weblinks.append(weblink)

        return weblinks

    def _create_funding_programmes(
        self, session: Session, project_data: Dict
    ) -> List[FundingProgramme]:
        """Create or retrieve FundingProgramme entities."""
        programmes = []

        for prog_data in ensure_list(get_nested(project_data, PROGRAMME_PATH)):
            code = parse_string(prog_data.get("code"))
            if not code:
                continue

            programme, _ = get_or_create(
                session,
                FundingProgramme,
                {"code": code},
                title=parse_titles_and_labels(prog_data.get("title")),
                short_title=parse_titles_and_labels(prog_data.get("shortTitle")),
                framework_programme=parse_string(prog_data.get("frameworkProgramme")),
                pga=parse_string(prog_data.get("pga")),
                rcn=parse_string(prog_data.get("rcn")),
            )
            programmes.append(programme)

        return programmes

    def _create_institutions(
        self, session: Session, project_data: Dict
    ) -> List[Tuple[Institution, Dict]]:
        """Create or retrieve Institution entities with their associated metadata."""
        institutions_with_metadata = []
        seen_institutions = set()

        for org_data in ensure_list(get_nested(project_data, ORG_PATH)):
            legal_name = parse_names_and_identifiers(org_data.get("legalName"))
            if not legal_name or legal_name in seen_institutions:
                continue
            seen_institutions.add(legal_name)

            coordinates = None
            geo_data = get_nested(org_data, "address.geolocation")
            if geo_data:
                coordinates = parse_geolocation(geo_data, True)

            institution, _ = get_or_create(
                session,
                Institution,
                {"legal_name": legal_name},
                sme=parse_bool(org_data.get("@sme")),
                url=parse_web_resources(get_nested(org_data, "address.url")),
                short_name=parse_string(org_data.get("shortName")),
                vat_number=parse_string(org_data.get("vatNumber")),
                street=parse_string(get_nested(org_data, "address.street")),
                postbox=parse_string(get_nested(org_data, "address.postBox")),
                postalcode=parse_string(get_nested(org_data, "address.postalCode")),
                city=parse_string(get_nested(org_data, "address.city")),
                country=parse_string(get_nested(org_data, "address.country")),
                geolocation=coordinates,
            )

            # ToDo: Check if flush needed here?
            # session.flush()

            self._create_institution_people(session, org_data, institution)

            metadata = {
                "ec_contribution": parse_float(org_data.get("@ecContribution")),
                "net_ec_contribution": parse_float(org_data.get("@netEcContribution")),
                "total_cost": parse_float(org_data.get("@totalCost")),
                "type": parse_string(org_data.get("@type")),
                "organization_id": parse_string(org_data.get("id")),
                "rcn": parse_string(org_data.get("rcn")),
                "position": 0,
            }

            institutions_with_metadata.append((institution, metadata))

        return institutions_with_metadata

    def _create_institution_people(
        self, session: Session, org_data: Dict, institution: Institution
    ) -> List[Person]:
        """Create or retrieve People entities associated with an institution."""
        people = []
        person_path = "relations.associations.person"

        for person_data in ensure_list(get_nested(org_data, person_path)):
            first_name = parse_names_and_identifiers(person_data.get("firstNames"))
            last_name = parse_names_and_identifiers(person_data.get("lastName"))

            if not first_name and not last_name:
                continue

            person, _ = get_or_create(
                session,
                Person,
                {"first_name": first_name, "last_name": last_name},
                title=parse_string(person_data.get("title")),
                name=None,
                telephone_number=parse_string(
                    get_nested(person_data, "address.telephoneNumber")
                ),
            )

            people.append(person)

            self._create_person_institution_junction(session, person, institution)

        return people

    def _create_research_outputs(
        self, session: Session, project_data: Dict
    ) -> List[ResearchOutput]:
        """Create or retrieve ResearchOutput entities."""
        research_outputs = []

        for result_data in ensure_list(get_nested(project_data, RESULT_PATH)):
            id_original = parse_string(result_data.get("id"))
            if not id_original:
                continue

            output_type = parse_string(result_data.get("@type"))
            title = parse_titles_and_labels(result_data.get("title"))

            if not title or not output_type:
                continue

            # ToDo: Check here for attachments, assess first

            research_output, created = get_or_create(
                session,
                ResearchOutput,
                {"id_original": id_original},
                from_pdf=False,
                type=output_type,
                doi=self._get_doi(result_data, "identifiers.doi"),
                title=title,
                publication_date=parse_date(result_data.get("contentUpdateDate")),
                journal=parse_string(get_nested(result_data, "details.journalTitle")),
                summary=parse_content(result_data.get("description")),
                comment=parse_content(result_data.get("teaser")),
                fulltext=None,
                funding_number=None,
            )

            if created:
                research_outputs.append(research_output)

                # ToDo: Check if flush here is needed
                # session.flush()

                output_topics = self._create_output_topics(session, result_data)
                output_weblinks = self._create_output_weblinks(session, result_data)
                output_institutions = self._create_output_institutions(
                    session, result_data
                )

                self._create_output_authors(session, result_data, research_output)

                self._create_output_topics_junction(
                    session, research_output, output_topics
                )
                self._create_output_weblinks_junction(
                    session, research_output, output_weblinks
                )
                self._create_output_institutions_junction(
                    session, research_output, output_institutions
                )

        research_output_pdfs = self._create_research_output_attachments(session)
        return research_outputs + research_output_pdfs

    def _create_research_output_attachments(
        self, session: Session
    ) -> List[ResearchOutput]:
        """
        Checks for dir: attachments, finds PDFs, parses them and returns them as ro.
        ToDo: Batch and parallelize this when running into time issues.
        """

        attachments = []
        attachments_dir = Path(self.path_to_file.parent) / "attachments"
        if not attachments_dir.exists() or not attachments_dir.is_dir():
            return []

        for idx, file_path in enumerate(attachments_dir.iterdir()):
            if not file_path.name.endswith(".pdf"):
                continue
            fulltext = parse_content(pdf_to_text(file_path))
            research_output, created = get_or_create(
                session,
                ResearchOutput,
                {"id_original": uuid.uuid4().hex},
                from_pdf=True,
                type="attachment",
                title=f"attachment_{idx}",
                fulltext=fulltext,
            )
            if created:
                attachments.append(research_output)
        return attachments

    def _create_output_topics(self, session: Session, result_data: Dict) -> List[Topic]:
        """Create or retrieve topics for a research output."""
        topics = []
        seen_topics = set()

        for cat_data in ensure_list(get_nested(result_data, CATEGORIES_PATH)):
            if cat_data.get("@classification") != "euroSciVoc":
                continue

            if cat_data.get("displayCode") is None or cat_data.get("code") is None:
                continue

            try:
                levels, display_codes = self._sanitize_euroscivoc_topics(
                    cat_data["code"],
                    (
                        cat_data["displayCode"]["#text"]
                        if isinstance(cat_data["displayCode"], dict)
                        else cat_data["displayCode"]
                    ),
                )

                for level, topic_name in zip(levels, display_codes):
                    if not topic_name:
                        continue
                    if topic_name in seen_topics:
                        continue
                    seen_topics.add(topic_name)

                    topic_name = parse_titles_and_labels(topic_name)
                    topic, _ = get_or_create(
                        session, Topic, {"name": topic_name}, level=level
                    )
                    topics.append(topic)
            except Exception:
                continue

        return topics

    def _create_output_weblinks(
        self, session: Session, result_data: Dict
    ) -> List[Weblink]:
        """Create or retrieve weblinks for a research output."""
        weblinks = []
        for link_data in ensure_list(
            get_nested(result_data, "relations.associations.webLink")
        ):
            url = parse_web_resources(link_data.get("physUrl"))
            if not url:
                continue

            title = parse_titles_and_labels(link_data.get("title"))
            weblink, _ = get_or_create(session, Weblink, {"url": url}, title=title)
            weblinks.append(weblink)

        return weblinks

    def _create_output_institutions(
        self, session: Session, result_data: Dict
    ) -> List[Institution]:
        """Create or retrieve institutions for a research output."""
        institutions = []
        seen_institutions = set()
        org_path = "relations.associations.organization"

        for org_data in ensure_list(get_nested(result_data, org_path)):
            legal_name = parse_names_and_identifiers(org_data.get("legalName"))
            if not legal_name or legal_name in seen_institutions:
                continue
            seen_institutions.add(legal_name)

            coordinates = None
            geo_data = get_nested(org_data, "address.geolocation")
            if geo_data:
                coordinates = parse_geolocation(geo_data, True)

            institution, _ = get_or_create(
                session,
                Institution,
                {"legal_name": legal_name},
                sme=parse_bool(org_data.get("@sme")),
                url=parse_web_resources(get_nested(org_data, "address.url")),
                short_name=parse_string(org_data.get("shortName")),
                vat_number=parse_string(org_data.get("vatNumber")),
                street=parse_string(get_nested(org_data, "address.street")),
                postbox=parse_string(get_nested(org_data, "address.postBox")),
                postalcode=parse_string(get_nested(org_data, "address.postalCode")),
                city=parse_string(get_nested(org_data, "address.city")),
                country=parse_string(get_nested(org_data, "address.country")),
                geolocation=coordinates,
            )

            institutions.append(institution)

        return institutions

    def _create_output_authors(
        self, session: Session, result_data: Dict, research_output: ResearchOutput
    ):
        """Create or retrieve authors for a research output and create junction records."""
        authors_str = get_nested(result_data, "details.authors")
        if not authors_str:
            return

        position = 0
        for author_name in authors_str.split(","):
            author_name = parse_names_and_identifiers(author_name)
            if not author_name:
                continue

            person, _ = get_or_create(
                session,
                Person,
                {"name": author_name},
                title=None,
                first_name=None,
                last_name=None,
                telephone_number=None,
            )

            self._create_person_output_junction(
                session, person, research_output, position
            )
            position += 1

    """ Junctions """

    def _create_person_institution_junction(
        self, session: Session, person: Person, institution: Institution
    ):
        """Create junction table entry between Person and Institution."""
        if person.id and institution.id:
            get_or_create(
                session,
                JunctionInstitutionPerson,
                {"institution_id": institution.id, "person_id": person.id},
            )

    def _create_person_output_junction(
        self,
        session: Session,
        person: Person,
        research_output: ResearchOutput,
        position: int,
    ):
        """Create junction table entry between Person and ResearchOutput with position."""
        if person.id and research_output.id:
            get_or_create(
                session,
                JunctionResearchOutputPerson,
                {"researchoutput_id": research_output.id, "person_id": person.id},
                person_position=position,
            )

    def _create_output_topics_junction(
        self, session: Session, research_output: ResearchOutput, topics: List[Topic]
    ):
        """Create junction table entries between ResearchOutput and Topics."""
        for topic in topics:
            if research_output.id and topic.id:
                get_or_create(
                    session,
                    JunctionResearchOutputTopic,
                    {"researchoutput_id": research_output.id, "topic_id": topic.id},
                )

    def _create_output_weblinks_junction(
        self, session: Session, research_output: ResearchOutput, weblinks: List[Weblink]
    ):
        """Create junction table entries between ResearchOutput and Weblinks."""
        for weblink in weblinks:
            if research_output.id and weblink.id:
                get_or_create(
                    session,
                    JunctionResearchOutputWeblink,
                    {"researchoutput_id": research_output.id, "weblink_id": weblink.id},
                )

    def _create_output_institutions_junction(
        self,
        session: Session,
        research_output: ResearchOutput,
        institutions: List[Institution],
    ):
        """Create junction table entries between ResearchOutput and Institutions."""
        for institution in institutions:
            if research_output.id and institution.id:
                get_or_create(
                    session,
                    JunctionResearchOutputInstitution,
                    {
                        "researchoutput_id": research_output.id,
                        "institution_id": institution.id,
                    },
                )

    def _create_project_topics(
        self,
        session: Session,
        project: Project,
        topics_with_levels: List[Tuple[Topic, int]],
    ):
        """Create junction table entries between Project and Topics."""
        for topic, _ in topics_with_levels:
            if project.id and topic.id:
                get_or_create(
                    session,
                    JunctionProjectTopic,
                    {"project_id": project.id, "topic_id": topic.id},
                )

    def _create_project_weblinks(
        self, session: Session, project: Project, weblinks: List[Weblink]
    ):
        """Create junction table entries between Project and Weblinks."""
        for weblink in weblinks:
            if project.id and weblink.id:
                get_or_create(
                    session,
                    JunctionProjectWeblink,
                    {"project_id": project.id, "weblink_id": weblink.id},
                )

    def _create_project_funding_programmes(
        self,
        session: Session,
        project: Project,
        funding_programmes: List[FundingProgramme],
    ):
        """Create junction table entries between Project and FundingProgrammes."""
        for programme in funding_programmes:
            if project.id and programme.id:
                get_or_create(
                    session,
                    JunctionProjectFundingProgramme,
                    {"project_id": project.id, "fundingprogramme_id": programme.id},
                )

    def _create_project_institutions(
        self,
        session: Session,
        project: Project,
        institutions_with_metadata: List[Tuple[Institution, Dict]],
        project_data: Dict,
    ):
        """Create junction table entries between Project and Institutions with metadata."""

        coordinator_id = None
        for org_data in ensure_list(get_nested(project_data, ORG_PATH)):
            if (
                org_data.get("@coordinator") == "true"
                or org_data.get("@coordinator") is True
            ):
                coordinator_id = parse_string(org_data.get("id"))
                break

        position = 0
        for institution, metadata in institutions_with_metadata:
            if project.id and institution.id:

                if coordinator_id and metadata["organization_id"] == coordinator_id:
                    position_value = 0
                else:
                    position += 1
                    position_value = position

                get_or_create(
                    session,
                    JunctionProjectInstitution,
                    {"project_id": project.id, "institution_id": institution.id},
                    institution_position=position_value,
                    ec_contribution=metadata["ec_contribution"],
                    net_ec_contribution=metadata["net_ec_contribution"],
                    total_cost=metadata["total_cost"],
                    type=metadata["type"],
                    organization_id=metadata["organization_id"],
                    rcn=metadata["rcn"],
                )

    def _create_project_research_outputs(
        self, session: Session, project: Project, research_outputs: List[ResearchOutput]
    ):
        """Create junction table entries between Project and ResearchOutputs."""
        for output in research_outputs:
            if project.id and output.id:
                get_or_create(
                    session,
                    JunctionProjectResearchOutput,
                    {"project_id": project.id, "researchoutput_id": output.id},
                )

    def _get_doi(self, data: dict, path: str) -> Optional[str]:
        """Extracts DOI from the data using the specified path."""
        doi_data = get_nested(data, path)
        return parse_string(doi_data) if doi_data else None

    def _get_call_info(self, data: dict, field: str) -> Optional[str]:
        """Gets call information from the first call entry."""
        calls = get_nested(data, f"{PRE}.call")
        if calls:
            first_call = ensure_list(calls)[0]
            return parse_string(first_call.get(field))
        return None

    def _sanitize_euroscivoc_topics(
        self, codes: str, display_codes: str
    ) -> Tuple[List[int], List[str]]:
        """
        Specifically for EuroSciVoc Topics and assigns them levels depending on the code length.
        - Level 0 is for the main topics
        - Level 1 is for code remaining code length 2
        - Level 2 is for code length 3
        - Level 3 is for all other codes
        """
        if not codes or not display_codes:
            return [], []

        codes = parse_string(codes)
        display_codes = parse_string(display_codes)

        if not codes or not display_codes:
            return [], []

        if codes.startswith("/"):
            codes = codes[1:]
        if display_codes.startswith("/"):
            display_codes = display_codes[1:]

        code_list = codes.split("/")
        display_code_list = display_codes.split("/")

        if len(code_list) != len(display_code_list):
            return [], []

        levels = []
        for code, display in zip(code_list, display_code_list):
            if display.lower() in self.lookup_main_topics:
                levels.append(0)
            elif len(code) == 2:
                levels.append(1)
            elif len(code) == 3:
                levels.append(2)
            else:
                levels.append(3)

        return levels, display_code_list
