from typing import Dict, List, Tuple, Optional

from sqlalchemy.orm import Session

from interfaces.i_loader import ILoader
from lib.database.get_or_create import get_or_create
from lib.file_handling.dict_utils import ensure_list, get_nested
from lib.sanitizers.parse_primitives import (
    parse_bool,
    parse_float,
    parse_date,
)
from lib.sanitizers.parse_text import (
    parse_string,
    parse_titles_and_labels,
    parse_content,
    parse_names_and_identifiers,
    parse_web_resources,
)
from sources.openaire.orm_model import (
    Project,
    Organization,
    Subject,
    Measure,
    Funder,
    FundingStream,
    H2020Programme,
    JunctionProjectOrganization,
    JunctionProjectSubject,
    JunctionProjectMeasure,
    JunctionProjectFunder,
    JunctionProjectFundingStream,
    JunctionProjectH2020Programme,
)


class OpenaireLoader(ILoader):
    """
    Data loading for Openaire documents.
    Transforms JSON documents into SQLAlchemy ORM models and adds them to the session.
    """

    def __init__(self, path_to_file=None):
        super().__init__(path_to_file)

    def load(self, session: Session, document: Dict):
        """
        Load OpenAIRE JSON document into SQLAlchemy ORM models.
        Transforms JSON data into Project, Organization, Subject, etc. models and their relationships.
        """
        if self.path_to_file and self.path_to_file.name == "research_products.json":
            # ToDo: Add research output parsing
            return

        assert (
            isinstance(document, dict) and document
        ), "document must be a non-empty dictionary"

        project_data = get_nested(document, "metadata.oaf:entity.oaf:project")
        if not project_data:
            return

        openaire_id = parse_string(get_nested(document, "header.dri:objIdentifier.$"))

        project, created_project = self._create_project(
            session, project_data, openaire_id
        )
        if not created_project:
            return

        subjects = self._create_subjects(session, project_data)
        measures = self._create_measures(session, project_data)
        organizations = self._create_organizations(session, project_data)
        funder, funding_streams = self._create_funding(session, project_data)
        h2020_programmes = self._create_h2020_programmes(session, project_data)

        session.flush()

        self._create_project_subjects(session, project, subjects)
        self._create_project_measures(session, project, measures)
        self._create_project_organizations(session, project, organizations)

        if funder:
            self._create_project_funder(session, project, funder, project_data)

        self._create_project_funding_streams(session, project, funding_streams)
        self._create_project_h2020_programmes(session, project, h2020_programmes)

    def _create_project(
        self, session: Session, project_data: Dict, openaire_id: Optional[str]
    ) -> Tuple[Optional[Project], bool]:
        """Create or retrieve a Project entity from the document."""
        original_id = parse_string(get_nested(project_data, "originalId.$"))
        if not original_id:
            return None, False

        code = parse_string(get_nested(project_data, "code.$"))
        title = parse_titles_and_labels(get_nested(project_data, "title.$"))

        if not code or not title:
            return None, False

        doi = None
        pids = ensure_list(get_nested(project_data, "pid"))
        for pid in pids:
            if get_nested(pid, "@classid") == "doi":
                doi = parse_string(get_nested(pid, "$"))

        acronym = parse_string(get_nested(project_data, "acronym.$"))
        start_date = parse_date(get_nested(project_data, "startdate.$"))
        end_date = parse_date(get_nested(project_data, "enddate.$"))
        duration = parse_string(get_nested(project_data, "duration.$"))
        summary = parse_content(get_nested(project_data, "summary.$"))
        keywords = parse_string(get_nested(project_data, "keywords.$"))

        ec_article29_3 = parse_bool(get_nested(project_data, "ecarticle29_3.$"))
        open_access_mandate_publications = parse_bool(
            get_nested(project_data, "oamandatepublications.$")
        )
        open_access_mandate_dataset = parse_bool(
            get_nested(project_data, "openAccessMandateForDataset")
        )
        ecsc39 = parse_bool(get_nested(project_data, "ecsc39.$"))

        total_cost = parse_float(get_nested(project_data, "totalcost.$"))
        funded_amount = parse_float(get_nested(project_data, "fundedamount.$"))

        website_url = parse_web_resources(get_nested(project_data, "websiteUrl.$"))
        call_identifier = parse_string(get_nested(project_data, "callidentifier.$"))

        project, created = get_or_create(
            session,
            Project,
            {"id_original": original_id},
            id_openaire=openaire_id,
            code=code,
            title=title,
            doi=doi,
            acronym=acronym,
            start_date=start_date,
            end_date=end_date,
            duration=duration,
            summary=summary,
            keywords=keywords,
            ec_article29_3=ec_article29_3,
            open_access_mandate_publications=open_access_mandate_publications,
            open_access_mandate_dataset=open_access_mandate_dataset,
            ecsc39=ecsc39,
            total_cost=total_cost,
            funded_amount=funded_amount,
            website_url=website_url,
            call_identifier=call_identifier,
        )

        return project, created

    def _create_subjects(
        self, session: Session, project_data: Dict
    ) -> List[Tuple[Subject, bool]]:
        """Create Subject entities from project data."""
        subjects_with_created = []
        seen_subjects = set()

        for subject_data in ensure_list(get_nested(project_data, "subject")):
            if not get_nested(subject_data, "$"):
                continue

            value = parse_titles_and_labels(get_nested(subject_data, "$"))
            if not value or value in seen_subjects:
                continue
            seen_subjects.add(value)

            scheme = parse_string(get_nested(subject_data, "@classname"))
            provenance_type = parse_string(
                get_nested(subject_data, "@provenanceaction")
            )
            trust = parse_float(get_nested(subject_data, "@trust"))

            subject, created = get_or_create(
                session,
                Subject,
                {"value": value},
                scheme=scheme,
                provenance_type=provenance_type,
                trust=trust,
            )

            subjects_with_created.append((subject, created))

        for subject_data in ensure_list(get_nested(project_data, "subjects")):
            if not get_nested(subject_data, "subject.value"):
                continue

            scheme = parse_string(get_nested(subject_data, "subject.scheme"))
            value = parse_titles_and_labels(get_nested(subject_data, "subject.value"))
            provenance_type = parse_string(
                get_nested(subject_data, "provenance.provenance")
            )
            trust = parse_float(get_nested(subject_data, "provenance.trust"))

            if not value:
                continue

            subject, created = get_or_create(
                session,
                Subject,
                {"value": value},
                scheme=scheme,
                provenance_type=provenance_type,
                trust=trust,
            )

            subjects_with_created.append((subject, created))

        return subjects_with_created

    def _create_measures(
        self, session: Session, project_data: Dict
    ) -> List[Tuple[Measure, bool]]:
        """Create Measure entities from project data."""
        measures_with_created = []

        for measure_data in ensure_list(get_nested(project_data, "measure")):
            name = parse_string(get_nested(measure_data, "@id"))
            score = parse_string(get_nested(measure_data, "@score"))

            if not name or not score:
                continue

            measure, created = get_or_create(
                session,
                Measure,
                {"name": name, "score": score},
            )

            measures_with_created.append((measure, created))

        return measures_with_created

    def _create_organizations(
        self, session: Session, project_data: Dict
    ) -> List[Tuple[Organization, dict]]:
        """Create Organization entities from project data."""
        organizations_with_metadata = []
        seen_orgs = set()

        rels_data = get_nested(project_data, "rels.rel")
        if not rels_data:
            return organizations_with_metadata

        for rel_idx, rel_data in enumerate(ensure_list(rels_data)):
            rel_class = get_nested(rel_data, "to.@class")
            rel_type = get_nested(rel_data, "to.@type")

            if rel_class != "hasParticipant" or rel_type != "organization":
                continue

            original_id = parse_string(get_nested(rel_data, "to.$"))
            legal_name = parse_names_and_identifiers(
                get_nested(rel_data, "legalname.$")
            )
            if not legal_name or legal_name in seen_orgs:
                continue
            seen_orgs.add(legal_name)

            legal_short_name = parse_names_and_identifiers(
                get_nested(rel_data, "legalshortname.$")
            )
            website_url = parse_web_resources(get_nested(rel_data, "websiteurl.$"))

            country_code = parse_string(get_nested(rel_data, "country.@classid"))
            country_label = parse_string(get_nested(rel_data, "country.@classname"))

            organization, created = get_or_create(
                session,
                Organization,
                {"legal_name": legal_name},
                original_id=original_id,
                legal_short_name=legal_short_name,
                is_first_listed=(rel_idx == 0),
                website_url=website_url,
                country_code=country_code,
                country_label=country_label,
                # For geolocation and alternative_names, we would need additional data
                # Defaults to empty/null per model
            )

            relation_metadata = {
                "relation_type": rel_class,
                "validated": parse_bool(get_nested(rel_data, "@inferred")),
            }

            organizations_with_metadata.append((organization, relation_metadata))

        return organizations_with_metadata

    def _create_funding(
        self, session: Session, project_data: Dict
    ) -> Tuple[Optional[Funder], List[Tuple[FundingStream, bool]]]:
        """Create Funder and FundingStream entities from project data."""
        funder = None
        funding_streams_with_created = []

        funding_tree = get_nested(project_data, "fundingtree")
        if not funding_tree:
            return funder, funding_streams_with_created

        funder_data = get_nested(funding_tree, "funder")
        if funder_data:
            funder_id = parse_string(get_nested(funder_data, "id.$"))
            funder_name = parse_names_and_identifiers(get_nested(funder_data, "name.$"))
            funder_short_name = parse_string(get_nested(funder_data, "shortname.$"))
            funder_jurisdiction = parse_string(
                get_nested(funder_data, "jurisdiction.$")
            )

            if funder_id and funder_name and funder_short_name and funder_jurisdiction:
                funder, _ = get_or_create(
                    session,
                    Funder,
                    {"original_id": funder_id},
                    name=funder_name,
                    short_name=funder_short_name,
                    jurisdiction=funder_jurisdiction,
                )

        funding_levels = ["funding_level_0", "funding_level_1"]
        parent_id = None

        for level in funding_levels:
            level_data = get_nested(funding_tree, level)
            if not level_data:
                continue

            stream_id = parse_string(get_nested(level_data, "id.$"))
            stream_name = parse_string(get_nested(level_data, "name.$"))
            stream_description = parse_content(get_nested(level_data, "description.$"))

            if not stream_id or not stream_name:
                continue

            funding_stream, created = get_or_create(
                session,
                FundingStream,
                {"original_id": stream_id},
                name=stream_name,
                description=stream_description,
                parent_id=parent_id,
            )

            funding_streams_with_created.append((funding_stream, created))

            parent_id = funding_stream.id

        for funding_data in ensure_list(get_nested(project_data, "fundings")):
            stream_data = get_nested(funding_data, "fundingStream")
            if not stream_data:
                continue

            stream_id = parse_string(get_nested(stream_data, "id"))
            stream_name = parse_string(get_nested(funding_data, "fundingStream"))
            stream_description = parse_content(get_nested(stream_data, "description"))

            if not stream_id or not stream_name:
                continue

            funding_stream, created = get_or_create(
                session,
                FundingStream,
                {"original_id": stream_id},
                name=stream_name,
                description=stream_description,
            )

            funding_streams_with_created.append((funding_stream, created))

        return funder, funding_streams_with_created

    def _create_h2020_programmes(
        self, session: Session, project_data: Dict
    ) -> List[Tuple[H2020Programme, bool]]:
        """Create H2020Programme entities from project data."""
        programmes_with_created = []

        for prog_data in ensure_list(get_nested(project_data, "h2020Programmes")):
            code = parse_string(get_nested(prog_data, "code"))
            description = parse_content(get_nested(prog_data, "description"))

            if not code:
                continue

            programme, created = get_or_create(
                session,
                H2020Programme,
                {"code": code},
                description=description,
            )

            programmes_with_created.append((programme, created))

        return programmes_with_created

    def _create_project_subjects(
        self,
        session: Session,
        project: Project,
        subjects_with_created: List[Tuple[Subject, bool]],
    ):
        """Create junction table entries between Project and Subject entities."""
        for subject, _ in subjects_with_created:
            if project.id and subject.id:
                get_or_create(
                    session,
                    JunctionProjectSubject,
                    {"project_id": project.id, "subject_id": subject.id},
                )

    def _create_project_measures(
        self,
        session: Session,
        project: Project,
        measures_with_created: List[Tuple[Measure, bool]],
    ):
        """Create junction table entries between Project and Measure entities."""
        for measure, _ in measures_with_created:
            if project.id and measure.id:
                get_or_create(
                    session,
                    JunctionProjectMeasure,
                    {"project_id": project.id, "measure_id": measure.id},
                )

    def _create_project_organizations(
        self,
        session: Session,
        project: Project,
        organizations_with_metadata: List[Tuple[Organization, dict]],
    ):
        """Create junction table entries between Project and Organization entities."""
        for organization, metadata in organizations_with_metadata:
            if project.id and organization.id:
                get_or_create(
                    session,
                    JunctionProjectOrganization,
                    {"project_id": project.id, "organization_id": organization.id},
                    relation_type=metadata.get("relation_type"),
                    validated=metadata.get("validated"),
                    validation_date=metadata.get("validation_date"),
                )

    def _create_project_funder(
        self, session: Session, project: Project, funder: Funder, project_data: Dict
    ):
        """Create junction table entry between Project and Funder entities."""
        if project.id and funder.id:
            funded_amount = project.funded_amount
            total_cost = project.total_cost
            currency = parse_string(get_nested(project_data, "currency.$"))

            get_or_create(
                session,
                JunctionProjectFunder,
                {"project_id": project.id, "funder_id": funder.id},
                funded_amount=funded_amount,
                total_cost=total_cost,
                currency=currency,
            )

    def _create_project_funding_streams(
        self,
        session: Session,
        project: Project,
        funding_streams_with_created: List[Tuple[FundingStream, bool]],
    ):
        """Create junction table entries between Project and FundingStream entities."""
        for funding_stream, _ in funding_streams_with_created:
            if project.id and funding_stream.id:
                get_or_create(
                    session,
                    JunctionProjectFundingStream,
                    {"project_id": project.id, "funding_stream_id": funding_stream.id},
                )

    def _create_project_h2020_programmes(
        self,
        session: Session,
        project: Project,
        h2020_programmes_with_created: List[Tuple[H2020Programme, bool]],
    ):
        """Create junction table entries between Project and H2020Programme entities."""
        for programme, _ in h2020_programmes_with_created:
            if project.id and programme.id:
                get_or_create(
                    session,
                    JunctionProjectH2020Programme,
                    {"project_id": project.id, "h2020_programme_id": programme.id},
                )
