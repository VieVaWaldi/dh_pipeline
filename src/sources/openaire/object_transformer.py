from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional

from core.etl.transformer.utils import ensure_list, get_nested
from core.sanitizers.sanitizer import (
    clean_date,
    clean_float,
    clean_bool,
)
from datamodels.digicher.entities import Institutions
from enrichment.search_geolocations import search_geolocation
from interfaces.i_object_transformer import IObjectTransformer
from sources.openaire.data_objects import (
    OpenaireProject,
    Organization,
    FundingTree,
    Funder,
    FundingLevel,
    Subject,
    Measure,
    Funding,
    H2020Programme,
    Grant,
    Country,
)


class OpenaireObjectTransformer(IObjectTransformer):

    def __init__(self, path_to_file: Path):
        super().__init__(path_to_file)

    def transform(self, data: Dict[str, Any]) -> Tuple[OpenaireProject, bool]:
        """
        Transform OpenAIRE JSON data into an OpenAireProject object.
        Returns the project object and a boolean indicating whether the transformation was successful.
        """
        if self.path_to_file and self.path_to_file.name == "research_products.json":
            # ToDo will do research products another time
            # Probably need to query original open aire id to link research product to project
            return None, False

        project_data = get_nested(data, "metadata.oaf:entity.oaf:project")
        if not project_data:
            return None, False

        # Extract OpenAIRE ID from header if available
        openaire_id = get_nested(data, "header.dri:objIdentifier.$")

        # Extract basic project properties
        code = get_nested(project_data, "code.$")
        title = get_nested(project_data, "title.$")

        # Extract dois
        doi = None
        pids = ensure_list(get_nested(project_data, "pid"))
        for pid in pids:
            if get_nested(pid, "@classid") == "doi":
                doi = get_nested(pid, "$")

        if not code or not title:
            return None, False

        # Extract dates
        start_date = clean_date(get_nested(project_data, "startdate.$"))
        end_date = clean_date(get_nested(project_data, "enddate.$"))

        # Extract flags
        ec_article29_3 = clean_bool(get_nested(project_data, "ecarticle29_3.$"))
        open_access_mandate_publications = clean_bool(
            get_nested(project_data, "oamandatepublications.$")
        )
        open_access_mandate_dataset = clean_bool(
            get_nested(project_data, "openAccessMandateForDataset")
        )
        ecsc39 = clean_bool(get_nested(project_data, "ecsc39.$"))

        # Extract financial info
        total_cost = clean_float(get_nested(project_data, "totalcost.$"))
        funded_amount = clean_float(get_nested(project_data, "fundedamount.$"))

        # Extract other fields
        acronym = get_nested(project_data, "acronym.$")
        duration = get_nested(project_data, "duration.$")
        summary = get_nested(project_data, "summary.$")
        website_url = get_nested(project_data, "websiteUrl.$")
        call_identifier = get_nested(project_data, "callidentifier.$")
        keywords = get_nested(project_data, "keywords.$")

        # Extract complex structures
        funding_tree = self.extract_funding_tree(project_data)
        organizations = self.extract_organizations(project_data)
        subjects = self.extract_subjects(project_data)
        measures = self.extract_measures(project_data)
        fundings = self.extract_fundings(project_data)
        h2020_programmes = self.extract_h2020_programmes(project_data)
        granted = self.extract_grant(project_data)

        # Create and return the project object
        openaire_project = OpenaireProject(
            id_original=get_nested(project_data, "originalId.$"),
            id_openaire=openaire_id,
            code=code,
            title=title,
            doi=doi,
            acronym=acronym,
            start_date=start_date,
            end_date=end_date,
            duration=duration,
            summary=summary,
            ec_article29_3=ec_article29_3,
            open_access_mandate_publications=open_access_mandate_publications,
            open_access_mandate_dataset=open_access_mandate_dataset,
            ecsc39=ecsc39,
            total_cost=total_cost,
            funded_amount=funded_amount,
            granted=granted,
            website_url=website_url,
            call_identifier=call_identifier,
            subjects=subjects,
            keywords=keywords,
            funding_tree=funding_tree,
            fundings=fundings,
            h2020_programmes=h2020_programmes,
            organizations=organizations,
            measures=measures,
        )

        return openaire_project, openaire_project.id_original is not None

    def extract_grant(self, project_data: dict) -> Optional[Grant]:
        """Extract grant information from project data."""
        granted_data = get_nested(project_data, "granted")
        if not granted_data:
            return None

        return Grant(
            currency=get_nested(granted_data, "currency"),
            funded_amount=clean_float(get_nested(granted_data, "fundedAmount")),
            total_cost=clean_float(get_nested(granted_data, "totalCost")),
        )

    def extract_measures(self, project_data: dict) -> List[Measure]:
        """Extract performance measures from project data."""
        measures = []
        for measure_data in ensure_list(get_nested(project_data, "measure")):
            if not measure_data.get("@id"):
                continue

            measure = Measure(
                id=measure_data.get("@id"),
                score=measure_data.get("@score"),
            )
            measures.append(measure)
        return measures

    def extract_h2020_programmes(self, project_data: dict) -> List[H2020Programme]:
        """Extract H2020 programmes from project data."""
        programmes = []
        for prog_data in ensure_list(get_nested(project_data, "h2020Programmes")):
            if not prog_data.get("code"):
                continue

            programme = H2020Programme(
                code=prog_data.get("code"),
                description=prog_data.get("description"),
            )
            programmes.append(programme)
        return programmes

    def extract_fundings(self, project_data: dict) -> List[Funding]:
        """Extract funding information from project data."""
        fundings = []
        for funding_data in ensure_list(get_nested(project_data, "fundings")):
            if not funding_data.get("fundingStream"):
                continue

            funding = Funding(
                funding_stream=funding_data.get("fundingStream"),
                jurisdiction=funding_data.get("jurisdiction"),
                name=funding_data.get("name"),
                short_name=funding_data.get("shortName"),
            )
            fundings.append(funding)
        return fundings

    def extract_subjects(self, project_data: dict) -> List[Subject]:
        """Extract subjects/topics from project data."""
        subjects = []

        # Extract from subjects array if present (as in NSF example)
        for subject_data in ensure_list(get_nested(project_data, "subject")):
            if not subject_data.get("$"):
                continue

            subject = Subject(
                scheme=subject_data.get("@classname", "unknown"),
                value=subject_data.get("$"),
                provenance_type=subject_data.get("@provenanceaction"),
                trust=clean_float(subject_data.get("@trust")),
            )
            subjects.append(subject)

        # Extract from subjects object if present (as in OpenAIRE documentation)
        for subject_data in ensure_list(get_nested(project_data, "subjects")):
            if not get_nested(subject_data, "subject.value"):
                continue

            subject = Subject(
                scheme=get_nested(subject_data, "subject.scheme"),
                value=get_nested(subject_data, "subject.value"),
                provenance_type=get_nested(subject_data, "provenance.provenance"),
                trust=clean_float(get_nested(subject_data, "provenance.trust")),
            )
            subjects.append(subject)

        return subjects

    def extract_funding_tree(self, project_data: dict) -> Optional[FundingTree]:
        """Extract funding tree from project data."""
        funding_tree_data = get_nested(project_data, "fundingtree")
        if not funding_tree_data:
            return None

        funder_data = get_nested(funding_tree_data, "funder")
        if not funder_data:
            return None

        funder = Funder(
            id=get_nested(funder_data, "id.$"),
            short_name=get_nested(funder_data, "shortname.$"),
            name=get_nested(funder_data, "name.$"),
            jurisdiction=get_nested(funder_data, "jurisdiction.$"),
        )

        # Extract funding levels
        funding_level_0 = self.extract_funding_level(
            funding_tree_data, "funding_level_0"
        )
        funding_level_1 = self.extract_funding_level(
            funding_tree_data, "funding_level_1"
        )

        return FundingTree(
            funder=funder,
            funding_level_0=funding_level_0,
            funding_level_1=funding_level_1,
        )

    def extract_funding_level(
        self, funding_tree_data: dict, level_key: str
    ) -> Optional[FundingLevel]:
        """Extract funding level from funding tree data."""
        level_data = get_nested(funding_tree_data, level_key)
        if not level_data:
            return None

        parent_data = get_nested(level_data, "parent")

        return FundingLevel(
            id=get_nested(level_data, "id.$"),
            name=get_nested(level_data, "name.$"),
            description=get_nested(level_data, "description.$"),
            parent=parent_data,
            class_type=get_nested(level_data, "class.$"),
        )

    def extract_organizations(self, project_data: dict) -> List[Organization]:
        """Extract organizations from project data."""
        organizations = []

        # Check if we have relations
        rels_data = get_nested(project_data, "rels.rel")
        if not rels_data:
            return organizations

        # Handle both single relation and list of relations
        for rel_idx, rel_data in enumerate(ensure_list(rels_data)):
            # Check if this is a hasParticipant relation with an organization
            rel_class = get_nested(rel_data, "to.@class")
            if (
                rel_class != "hasParticipant"
                or get_nested(rel_data, "to.@type") != "organization"
            ):
                continue

            # Extract country
            country = None
            country_data = get_nested(rel_data, "country")
            if country_data:
                country = Country(
                    code=country_data.get("@classid"),
                    label=country_data.get("@classname"),
                )

            # Create organization with available fields
            organization = Organization(
                id=get_nested(rel_data, "to.$"),
                legal_name=get_nested(rel_data, "legalname.$"),
                legal_short_name=get_nested(rel_data, "legalshortname.$"),
                is_first_listed=True if rel_idx == 0 else False,
                geolocation=None,
                alternative_names=[],  # Doesnt seem to be in the data though docs say it is. I grep'd all 10k files
                website_url=get_nested(rel_data, "websiteurl.$"),
                country=country,
                relation_type=rel_class,
                validated=(
                    clean_bool(rel_data.get("@inferred"))
                    if "@inferred" in rel_data
                    else None
                ),
                # No validation date in the example
            )
            organizations.append(organization)

        return organizations
