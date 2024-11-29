from dataclasses import dataclass
from datetime import date
from datetime import datetime
from typing import Dict, Any
from typing import List, Optional


@dataclass
class Person:
    title: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    telephone_number: Optional[str]
    name: Optional[str]  # For result authors which just have a name


@dataclass
class Topic:
    name: str  # title in source
    code: Optional[str]
    description: Optional[str]
    cordis_classification: Optional[str]  # @classification in source


@dataclass
class Weblink:
    url: str
    title: Optional[str]


@dataclass
class FundingProgramme:
    code: str
    title: Optional[str]
    short_title: Optional[str]
    framework_programme: Optional[str]
    pga: Optional[str]
    rcn: Optional[str]


@dataclass
class Institution:
    name: str  # legalName in source
    sme: Optional[bool]  # @sme in source
    address_street: Optional[str]
    address_postbox: Optional[str]
    address_postalcode: Optional[str]
    address_city: Optional[str]
    address_country: Optional[str]
    address_geolocation: Optional[str]
    url: Optional[str]
    short_name: Optional[str]
    vat_number: Optional[str]

    people: List[Person]


@dataclass
class ResearchOutput:
    id_original: str  # id in source
    type: str  # @type in source
    title: str
    publication_date: Optional[date]  # contentUpdateDate in source
    journal: Optional[str]  # journalTitle in source
    summary: Optional[str]  # description in source
    comment: Optional[str]  # teaser in source

    institutions: List[Institution]
    topics: List[Topic]
    people: List[Person]
    weblinks: List[Weblink]


@dataclass
class CordisProject:
    id_original: str  # id in source
    title: str
    acronym: Optional[str]
    status: Optional[str]
    start_date: Optional[date]
    end_date: Optional[date]
    ec_signature_date: Optional[date]
    total_cost: Optional[float]
    ec_max_contribution: Optional[float]
    objective: Optional[str]

    # There is a list of calls but we only will use the first item in there
    call_identifier: Optional[str]
    call_title: Optional[str]
    call_rcn: Optional[str]

    dois: List[str]
    fundingprogrammes: List[FundingProgramme]
    research_outputs: List[ResearchOutput]
    institutions: List[Institution]
    topics: List[Topic]
    weblinks: List[Weblink]



PROJECT = "project"
RELATIONS = "relations"
ASSOCIATIONS = "associations"
PRE = f"{RELATIONS}.{ASSOCIATIONS}" # {PROJECT}.


RESULT_PATH = f"{PRE}.result"
ORG_PATH = f"{PRE}.organization"
PROGRAMME_PATH = f"{PRE}.programme"
CATEGORIES_PATH = f"{RELATIONS}.categories.category" # {PROJECT}.


class CordisExtractor:
    """
    Extracts structured data from CORDIS source documents and creates corresponding
    Python objects according to the defined schema.
    """

    def extract(self, data: Dict[str, Any]) -> CordisProject:
        """Main extraction method that processes the raw data into a CordisProject object."""

        project_data = data.get(PROJECT, {})

        return CordisProject(
            id_original=project_data.get("id"),
            title=project_data.get("title"),
            acronym=project_data.get("acronym"),
            status=project_data.get("status"),
            start_date=self._parse_date(project_data.get("startDate")),
            end_date=self._parse_date(project_data.get("endDate")),
            ec_signature_date=self._parse_date(project_data.get("ecSignatureDate")),
            total_cost=self._parse_float(project_data.get("totalCost")),
            ec_max_contribution=self._parse_float(
                project_data.get("ecMaxContribution")
            ),
            objective=project_data.get("objective"),

            call_identifier=self._get_call_info(project_data, "identifier"),
            call_title=self._get_call_info(project_data, "title"),
            call_rcn=self._get_call_info(project_data, "rcn"),
            dois=self._extract_dois(project_data),
            fundingprogrammes=self._extract_fundingprogrammes(project_data),
            research_outputs=self._extract_research_outputs(project_data),
            institutions=self._extract_institutions(project_data),
            topics=self._extract_topics(project_data),
            weblinks=self._extract_weblinks(project_data),
        )

    def _extract_research_outputs(self, data: dict) -> List[ResearchOutput]:
        """Extracts research outputs from the project data."""
        results = []
        for result_data in self.ensure_list(self._get_nested(data, RESULT_PATH)):
            if result_data.get("id") is None:
                continue
            result = ResearchOutput(
                id_original=result_data.get("id"),
                type=result_data.get("@type"),
                title=result_data.get("title"),
                publication_date=self._parse_date(result_data.get("contentUpdateDate")),
                journal=self._get_nested(result_data, "details.journalTitle"),
                summary=result_data.get("description"),
                comment=result_data.get("teaser"),
                institutions=self._extract_result_institutions(result_data),
                topics=self._extract_result_topics(result_data),
                people=self._extract_result_people(result_data),
                weblinks=self._extract_result_weblinks(result_data),
            )
            results.append(result)
        return results

    def _extract_institutions(self, data: dict) -> List[Institution]:
        """Extracts institutions from the project data."""
        institutions = []
        for org_data in self.ensure_list(self._get_nested(data, ORG_PATH)):
            if org_data.get("legalName") is None:
                continue
            institution = Institution(
                name=org_data.get("legalName"),
                sme=self._parse_bool(org_data.get("@sme")),
                address_street=self._get_nested(org_data, "address.street"),
                address_postbox=self._get_nested(org_data, "address.postBox"),
                address_postalcode=self._get_nested(org_data, "address.postalCode"),
                address_city=self._get_nested(org_data, "address.city"),
                address_country=self._get_nested(org_data, "address.country"),
                address_geolocation=self._get_nested(org_data, "address.geolocation"),
                url=self._get_nested(org_data, "address.url"),
                short_name=org_data.get("shortName"),
                vat_number=org_data.get("vatNumber"),
                people=self._extract_organization_people(org_data),
            )
            institutions.append(institution)
        return institutions

    def _extract_organization_people(self, org_data: dict) -> List[Person]:
        """Extracts people associated with an organization."""
        people = []
        person_path = "relations.associations.person"
        for person_data in self.ensure_list(self._get_nested(org_data, person_path)):
            if person_data.get("first_name") is None:
                continue
            person = Person(
                title=person_data.get("title"),
                first_name=person_data.get("firstNames"),
                last_name=person_data.get("lastName"),
                telephone_number=self._get_nested(
                    person_data, "address.telephoneNumber"
                ),
                name=None,  # Organizations don't use the single name field
            )
            people.append(person)
        return people

    def _extract_result_people(self, result_data: dict) -> List[Person]:
        """Extracts people (authors) from a research output."""
        authors = []
        authors_str = self._get_nested(result_data, "details.authors")
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

    def _extract_fundingprogrammes(self, data: dict) -> List[FundingProgramme]:
        """Extracts funding programmes from the project data."""
        programmes = []
        for prog_data in self.ensure_list(self._get_nested(data, PROGRAMME_PATH)):
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

    def _extract_topics(self, data: dict) -> List[Topic]:
        """Extracts topics (categories) from the project data."""
        topics = []
        for cat_data in self.ensure_list(self._get_nested(data, CATEGORIES_PATH)):
            if cat_data.get("title") is None:
                continue
            topic = Topic(
                name=cat_data.get("title"),
                code=cat_data.get("code"),
                description=cat_data.get("description"),
                cordis_classification=cat_data.get("@classification"),
            )
            topics.append(topic)
        return topics

    def _extract_result_topics(self, result_data: dict) -> List[Topic]:
        """Extracts topics from a research output."""
        topics = []
        cat_path = "relations.categories.category"
        for cat_data in self.ensure_list(self._get_nested(result_data, cat_path)):
            if cat_data.get("title") is None:
                continue
            topic = Topic(
                name=cat_data.get("title"),
                code=cat_data.get("code"),
                description=cat_data.get("description"),
                cordis_classification=cat_data.get("@classification"),
            )
            topics.append(topic)
        return topics

    def _extract_dois(self, data: dict) -> List[str]:
        """Extracts DOIs from the project data."""
        dois = []
        doi_data = self._get_nested(data, "identifiers.doi")
        if doi_data:
            dois.extend(self.ensure_list(doi_data))
        return dois

    def _extract_weblinks(self, data: dict) -> List[Weblink]:
        """Extracts weblinks from the project data."""
        weblinks = []
        for link_data in self.ensure_list(self._get_nested(data, f"{PRE}.webLink")):
            if link_data.get("physUrl") is None:
                continue
            weblink = Weblink(
                url=link_data.get("physUrl"), title=link_data.get("title")
            )
            weblinks.append(weblink)
        return weblinks

    def _extract_result_weblinks(self, result_data: dict) -> List[Weblink]:
        """Extracts weblinks from a research output."""
        weblinks = []
        for link_data in self.ensure_list(
            self._get_nested(result_data, "relations.associations.webLink")
        ):
            if link_data.get("physUrl") is None:
                    continue
            weblink = Weblink(
                url=link_data.get("physUrl"), title=link_data.get("title")
            )
            weblinks.append(weblink)
        return weblinks

    def _extract_result_institutions(self, result_data: dict) -> List[Institution]:
        """Extracts institutions from a research output."""
        institutions = []
        org_path = "relations.associations.organization"
        for org_data in self.ensure_list(self._get_nested(result_data, org_path)):
            if org_data.get("legalName") is None:
                    continue
            institution = Institution(
                name=org_data.get("legalName"),
                sme=self._parse_bool(org_data.get("@sme")),
                address_street=self._get_nested(org_data, "address.street"),
                address_postbox=self._get_nested(org_data, "address.postBox"),
                address_postalcode=self._get_nested(org_data, "address.postalCode"),
                address_city=self._get_nested(org_data, "address.city"),
                address_country=self._get_nested(org_data, "address.country"),
                address_geolocation=self._get_nested(org_data, "address.geolocation"),
                url=self._get_nested(org_data, "address.url"),
                short_name=org_data.get("shortName"),
                vat_number=org_data.get("vatNumber"),
                people=[],  # Result institutions don't have associated people
            )
            institutions.append(institution)
        return institutions

    def _get_call_info(self, data: dict, field: str) -> Optional[str]:
        """Gets call information from the first call entry."""
        calls = self._get_nested(data, f"{PRE}.call")
        if calls:
            first_call = self.ensure_list(calls)[0]
            return first_call.get(field)
        return None

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[date]:
        """Parses date strings in YYYY-MM-DD format."""
        if date_str:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _parse_float(value: Any) -> Optional[float]:
        """Parses float values, handling potential type issues."""
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _parse_bool(value: Any) -> Optional[bool]:
        """Parses boolean values, handling string representations."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() == "true"
        return None

    @staticmethod
    def _get_nested(data: dict, path: str) -> Any:
        """
        Safely gets nested dictionary values using dot notation.
        Example: _get_nested(data, "a.b.c") is equivalent to data.get("a", {}).get("b", {}).get("c")
        """
        current = data
        for part in path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part, {})
        return current if current != {} else None

    @staticmethod
    def ensure_list(value: Any) -> list:
        """
        Ensures the input value is a list. If it's not a list, wraps it in one.
        If the value is None, returns an empty list.
        """
        if value is None:
            return []
        return value if isinstance(value, list) else [value]
