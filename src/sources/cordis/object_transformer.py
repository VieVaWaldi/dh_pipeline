from typing import Dict, Any, Tuple
from typing import List, Optional

from core.sanitizers.sanitizer import (
    clean_date,
    clean_float,
    clean_geolocation,
    clean_bool,
    clean_string,
)
from core.etl.transformer.utils import ensure_list, get_nested
from interfaces.i_object_transformer import IObjectTransformer
from sources.cordis.data_objects import (
    CordisProject,
    ResearchOutput,
    Institution,
    Person,
    FundingProgramme,
    Topic,
    Weblink,
)
from utils.error_handling.error_handling import log_and_raise_exception

PROJECT = "project"
RELATIONS = "relations"
ASSOCIATIONS = "associations"
PRE = f"{RELATIONS}.{ASSOCIATIONS}"


RESULT_PATH = f"{PRE}.result"
ORG_PATH = f"{PRE}.organization"
PROGRAMME_PATH = f"{PRE}.programme"
CATEGORIES_PATH = f"{RELATIONS}.categories.category"


class CordisObjectTransformer(IObjectTransformer):

    def __init__(self):
        super().__init__()

        self.lookup_main_topics = [
            "medical and health sciences",
            "natural sciences",
            "engineering and technology",
            "agricultural sciences",
            "social sciences",
            "humanities",
        ]

    def transform(self, data: Dict[str, Any]) -> Tuple[CordisProject, bool]:
        project_data = data.get(PROJECT, {})

        cordis_project = CordisProject(
            id_original=project_data.get("id"),
            doi=self.get_doi(project_data, "identifiers.grantDoi"),
            title=project_data.get("title"),
            acronym=project_data.get("acronym"),
            status=project_data.get("status"),
            start_date=clean_date(project_data.get("startDate")),
            end_date=clean_date(project_data.get("endDate")),
            ec_signature_date=clean_date(project_data.get("ecSignatureDate")),
            total_cost=clean_float(project_data.get("totalCost")),
            ec_max_contribution=clean_float(project_data.get("ecMaxContribution")),
            objective=project_data.get("objective"),
            call_identifier=self.get_call_info(project_data, "identifier"),
            call_title=self.get_call_info(project_data, "title"),
            call_rcn=self.get_call_info(project_data, "rcn"),
            fundingprogrammes=self.get_fundingprogrammes(project_data),
            research_outputs=self.get_research_outputs(project_data),
            institutions=self.get_institutions(project_data),
            topics=self.get_topics(project_data),
            weblinks=self.get_weblinks(project_data),
        )
        return cordis_project, cordis_project.id_original is not None

    def get_research_outputs(self, data: dict) -> List[ResearchOutput]:
        """Extracts research outputs from the project data."""
        results = []
        for result_data in ensure_list(get_nested(data, RESULT_PATH)):
            if result_data.get("id") is None:
                continue
            # fixes doi null violation. ToDo we need sanitizers

            title = result_data.get("title").replace("–", "-").replace("—", "-")
            result = ResearchOutput(
                id_original=result_data.get("id"),
                doi=self.get_doi(result_data, "identifiers.doi"),
                type=result_data.get("@type"),
                title=title,
                publication_date=clean_date(result_data.get("contentUpdateDate")),
                journal=get_nested(result_data, "details.journalTitle"),
                summary=result_data.get("description"),
                comment=result_data.get("teaser"),
                institutions=self.get_result_institutions(result_data),
                topics=self.get_topics(result_data),
                people=self.get_result_people(result_data),
                weblinks=self.get_result_weblinks(result_data),
            )
            results.append(result)
        return results

    def get_institutions(self, data: dict) -> List[Institution]:
        """Extracts institutions from the project data."""
        institutions = []
        for org_data in ensure_list(get_nested(data, ORG_PATH)):
            if org_data.get("legalName") is None:
                continue
            coordinates = clean_geolocation(get_nested(org_data, "address.geolocation"))
            institution = Institution(
                name=org_data.get("legalName"),
                sme=clean_bool(org_data.get("@sme")),
                address_street=get_nested(org_data, "address.street"),
                address_postbox=get_nested(org_data, "address.postBox"),
                address_postalcode=get_nested(org_data, "address.postalCode"),
                address_city=get_nested(org_data, "address.city"),
                address_country=get_nested(org_data, "address.country"),
                address_geolocation=coordinates,
                url=get_nested(org_data, "address.url"),
                short_name=org_data.get("shortName"),
                vat_number=org_data.get("vatNumber"),
                people=self.get_organization_people(org_data),
                #
                ec_contribution=clean_float(org_data.get("@ecContribution")),
                net_ec_contribution=clean_float(org_data.get("@netEcContribution")),
                total_cost=clean_float(org_data.get("@totalCost")),
                type=org_data.get("@type"),
                organization_id=org_data.get("id"),
                rcn=org_data.get("rcn"),
            )
            institutions.append(institution)
        return institutions

    def get_organization_people(self, org_data: dict) -> List[Person]:
        """Extracts people associated with an organization."""
        people = []
        person_path = "relations.associations.person"
        for person_data in ensure_list(get_nested(org_data, person_path)):
            if person_data.get("first_name") is None:
                continue
            person = Person(
                title=person_data.get("title"),
                first_name=person_data.get("firstNames"),
                last_name=person_data.get("lastName"),
                telephone_number=get_nested(person_data, "address.telephoneNumber"),
                name=None,  # Organizations don't use the single name field
            )
            people.append(person)
        return people

    def get_result_people(self, result_data: dict) -> List[Person]:
        """Extracts people (authors) from a research output."""
        authors = []
        authors_str = get_nested(result_data, "details.authors")
        if authors_str:
            # Authors are comma-separated in the source
            for author_name in authors_str.split(","):
                if author_name is None:
                    continue
                authors.append(
                    Person(
                        name=author_name.strip(),
                        title=None,
                        first_name=None,
                        last_name=None,
                        telephone_number=None,
                    )
                )
        return authors

    def get_fundingprogrammes(self, data: dict) -> List[FundingProgramme]:
        """Extracts funding programmes from the project data."""
        programmes = []
        for prog_data in ensure_list(get_nested(data, PROGRAMME_PATH)):
            if prog_data.get("code") is None:
                continue
            programme = FundingProgramme(
                code=prog_data.get("code"),
                title=prog_data.get("title"),
                short_title=prog_data.get("shortTitle"),
                framework_programme=prog_data.get("frameworkProgramme"),
                pga=prog_data.get("pga"),
                rcn=prog_data.get("rcn"),
            )
            programmes.append(programme)
        return programmes

    def get_topics(self, data: dict) -> List[Topic]:
        """Extracts topics (categories) from the project data."""
        topics = []
        for cat_data in ensure_list(get_nested(data, CATEGORIES_PATH)):
            if cat_data.get("@classification") != "euroSciVoc":
                continue
            # if not @type == inFieldOfScience
            if cat_data.get("displayCode") is None or cat_data.get("code") is None:
                continue

            levels, display_codes = self.sanitize_euroscivoc_topics(
                cat_data["code"], cat_data["displayCode"]["#text"]
            )
            for level, name in zip(levels, display_codes):
                topic = Topic(name=name, level=level)
                topics.append(topic)
        return topics

    def get_doi(self, data: dict, path: str) -> str:
        """Extracts DOIs from the project data."""
        doi_data = get_nested(data, path)
        return doi_data if doi_data else None

    def get_weblinks(self, data: dict) -> List[Weblink]:
        """Extracts weblinks from the project data."""
        weblinks = []
        for link_data in ensure_list(get_nested(data, f"{PRE}.webLink")):
            if link_data.get("physUrl") is None:
                continue
            weblink = Weblink(
                url=link_data.get("physUrl"), title=link_data.get("title")
            )
            weblinks.append(weblink)
        return weblinks

    def get_result_weblinks(self, result_data: dict) -> List[Weblink]:
        """Extracts weblinks from a research output."""
        weblinks = []
        for link_data in ensure_list(
            get_nested(result_data, "relations.associations.webLink")
        ):
            if link_data.get("physUrl") is None:
                continue
            weblink = Weblink(
                url=link_data.get("physUrl"), title=link_data.get("title")
            )
            weblinks.append(weblink)
        return weblinks

    def get_result_institutions(self, result_data: dict) -> List[Institution]:
        """Extracts institutions from a research output."""
        institutions = []
        org_path = "relations.associations.organization"
        for org_data in ensure_list(get_nested(result_data, org_path)):
            if org_data.get("legalName") is None:
                continue
            coordinates = clean_geolocation(get_nested(org_data, "address.geolocation"))
            institution = Institution(
                name=org_data.get("legalName"),
                sme=clean_bool(org_data.get("@sme")),
                address_street=get_nested(org_data, "address.street"),
                address_postbox=get_nested(org_data, "address.postBox"),
                address_postalcode=get_nested(org_data, "address.postalCode"),
                address_city=get_nested(org_data, "address.city"),
                address_country=get_nested(org_data, "address.country"),
                address_geolocation=coordinates,
                url=get_nested(org_data, "address.url"),
                short_name=org_data.get("shortName"),
                vat_number=org_data.get("vatNumber"),
                ec_contribution=None,
                net_ec_contribution=None,
                total_cost=None,
                type=None,
                organization_id=None,
                rcn=None,
                people=[],  # Result institutions don't have associated people
            )
            institutions.append(institution)
        return institutions

    def get_call_info(self, data: dict, field: str) -> Optional[str]:
        """Gets call information from the first call entry."""
        calls = get_nested(data, f"{PRE}.call")
        if calls:
            first_call = ensure_list(calls)[0]
            return first_call.get(field)
        return None

    """ Helper Methods """

    def sanitize_euroscivoc_topics(
        self, codes: str, display_codes: str
    ) -> Tuple[List[int], List[str]]:
        """
        Specifically for EuroSciVoc Topics and assigns them levels depending on the code length.
        - Level 0 is for the main topics
        - Level 1 is for code remaining code length 2
        - Level 2 is fore code length 3
        - Level 3 is for all other codes
        """

        codes = clean_string(codes[1:]).split("/")
        display_codes = clean_string(display_codes[1:]).split("/")

        if len(codes) != len(display_codes):
            log_and_raise_exception(
                "Codes and display codes for euroSciVoc have a different length."
            )

        levels = []
        for code, display in zip(codes, display_codes):
            if display.lower() in self.lookup_main_topics:
                levels.append(0)
            elif len(code) == 2:
                levels.append(1)
            elif len(code) == 3:
                levels.append(2)
            else:
                levels.append(3)
        return levels, display_codes
