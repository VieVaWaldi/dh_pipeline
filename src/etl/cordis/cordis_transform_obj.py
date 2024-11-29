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
    address_geolocation: Optional[list[int]]
    url: Optional[str]
    short_name: Optional[str]
    vat_number: Optional[str]

    # For projects only
    ec_contribution: Optional[float]
    net_ec_contribution: Optional[float]
    total_cost: Optional[float]
    type: Optional[str]
    organization_id: Optional[str]
    rcn: Optional[str]

    people: List[Person]


@dataclass
class ResearchOutput:
    id_original: str  # id in source
    type: str  # @type in source
    doi: str
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
    doi: str
    title: str
    acronym: Optional[str]
    status: Optional[str]
    start_date: Optional[date]
    end_date: Optional[date]
    ec_signature_date: Optional[date]
    total_cost: Optional[float]
    ec_max_contribution: Optional[float]
    objective: Optional[str]

    call_identifier: Optional[str]
    call_title: Optional[str]
    call_rcn: Optional[str]

    fundingprogrammes: List[FundingProgramme]
    research_outputs: List[ResearchOutput]
    institutions: List[Institution]
    topics: List[Topic]
    weblinks: List[Weblink]


PROJECT = "project"
RELATIONS = "relations"
ASSOCIATIONS = "associations"
PRE = f"{RELATIONS}.{ASSOCIATIONS}"


RESULT_PATH = f"{PRE}.result"
ORG_PATH = f"{PRE}.organization"
PROGRAMME_PATH = f"{PRE}.programme"
CATEGORIES_PATH = f"{RELATIONS}.categories.category"


class CordisTransformObj:
    """
    Extracts structured data from CORDIS source documents and creates corresponding
    Python objects according to the defined schema.
    """

    def get(self, data: Dict[str, Any]) -> CordisProject:
        """Main getion method that processes the raw data into a CordisProject object."""

        project_data = data.get(PROJECT, {})

        return CordisProject(
            id_original=project_data.get("id"),
            doi=self._get_doi(project_data, "identifiers.grantDoi"),
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
            fundingprogrammes=self._get_fundingprogrammes(project_data),
            research_outputs=self._get_research_outputs(project_data),
            institutions=self._get_institutions(project_data),
            topics=self._get_topics(project_data),
            weblinks=self._get_weblinks(project_data),
        )

    def _get_research_outputs(self, data: dict) -> List[ResearchOutput]:
        """Extracts research outputs from the project data."""
        results = []
        for result_data in self.ensure_list(self._get_nested(data, RESULT_PATH)):
            if result_data.get("id") is None:
                continue
            # fixes doi null violation. ToDo we need sanitization
            title = result_data.get("title").replace('–', '-').replace('—', '-')
            result = ResearchOutput(
                id_original=result_data.get("id"),
                doi=self._get_doi(result_data, "identifiers.doi"),
                type=result_data.get("@type"),
                title=title,
                publication_date=self._parse_date(result_data.get("contentUpdateDate")),
                journal=self._get_nested(result_data, "details.journalTitle"),
                summary=result_data.get("description"),
                comment=result_data.get("teaser"),
                institutions=self._get_result_institutions(result_data),
                topics=self._get_result_topics(result_data),
                people=self._get_result_people(result_data),
                weblinks=self._get_result_weblinks(result_data),
            )
            results.append(result)
        return results

    def _get_institutions(self, data: dict) -> List[Institution]:
        """Extracts institutions from the project data."""
        institutions = []
        for org_data in self.ensure_list(self._get_nested(data, ORG_PATH)):
            if org_data.get("legalName") is None:
                continue
            coordinates = self._parse_geolocation(
                self._get_nested(org_data, "address.geolocation")
            )
            institution = Institution(
                name=org_data.get("legalName"),
                sme=self._parse_bool(org_data.get("@sme")),
                address_street=self._get_nested(org_data, "address.street"),
                address_postbox=self._get_nested(org_data, "address.postBox"),
                address_postalcode=self._get_nested(org_data, "address.postalCode"),
                address_city=self._get_nested(org_data, "address.city"),
                address_country=self._get_nested(org_data, "address.country"),
                address_geolocation=coordinates,
                url=self._get_nested(org_data, "address.url"),
                short_name=org_data.get("shortName"),
                vat_number=org_data.get("vatNumber"),
                people=self._get_organization_people(org_data),
                #
                ec_contribution=self._parse_float(org_data.get("@ecContribution")),
                net_ec_contribution=self._parse_float(
                    org_data.get("@netEcContribution")
                ),
                total_cost=self._parse_float(org_data.get("@totalCost")),
                type=org_data.get("@type"),
                organization_id=org_data.get("id"),
                rcn=org_data.get("rcn"),
            )
            institutions.append(institution)
        return institutions

    def _get_organization_people(self, org_data: dict) -> List[Person]:
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

    def _get_result_people(self, result_data: dict) -> List[Person]:
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

    def _get_fundingprogrammes(self, data: dict) -> List[FundingProgramme]:
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

    def _get_topics(self, data: dict) -> List[Topic]:
        """Extracts topics (categories) from the project data."""
        topics = []
        for cat_data in self.ensure_list(self._get_nested(data, CATEGORIES_PATH)):
            if cat_data.get("title") is None:
                continue
            classification = cat_data.get("@classification")
            if not isinstance(classification, str) or classification != "euroSciVoc":
                continue
            topic = Topic(name=cat_data.get("title"), code=cat_data.get("code"))
            topics.append(topic)
        return topics

    def _get_result_topics(self, result_data: dict) -> List[Topic]:
        """Extracts topics from a research output."""
        topics = []
        cat_path = "relations.categories.category"
        for cat_data in self.ensure_list(self._get_nested(result_data, cat_path)):
            if cat_data.get("title") is None:
                continue
            classification = cat_data.get("@classification")
            if not isinstance(classification, str) or classification != "euroSciVoc":
                continue
            topic = Topic(name=cat_data.get("title"), code=cat_data.get("code"))
            topics.append(topic)
        return topics

    def _get_doi(self, data: dict, path: str) -> str:
        """Extracts DOIs from the project data."""
        doi_data = self._get_nested(data, path)
        return doi_data if doi_data else None

    def _get_weblinks(self, data: dict) -> List[Weblink]:
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

    def _get_result_weblinks(self, result_data: dict) -> List[Weblink]:
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

    def _get_result_institutions(self, result_data: dict) -> List[Institution]:
        """Extracts institutions from a research output."""
        institutions = []
        org_path = "relations.associations.organization"
        for org_data in self.ensure_list(self._get_nested(result_data, org_path)):
            if org_data.get("legalName") is None:
                continue
            coordinates = self._parse_geolocation(
                self._get_nested(org_data, "address.geolocation")
            )
            institution = Institution(
                name=org_data.get("legalName"),
                sme=self._parse_bool(org_data.get("@sme")),
                address_street=self._get_nested(org_data, "address.street"),
                address_postbox=self._get_nested(org_data, "address.postBox"),
                address_postalcode=self._get_nested(org_data, "address.postalCode"),
                address_city=self._get_nested(org_data, "address.city"),
                address_country=self._get_nested(org_data, "address.country"),
                address_geolocation=coordinates,
                url=self._get_nested(org_data, "address.url"),
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

    def _parse_geolocation(self, geolocation: str) -> Optional[list]:
        """
        Parse geolocation string and return as [lon, lat] array.
        Returns None if coordinates are invalid.
        """
        if not geolocation:
            return None

        cleaned = geolocation.replace("(", "").replace(")", "")
        try:
            lat, lon = map(lambda x: float(x.strip()), cleaned.split(","))
            if not geolocation.startswith("("):
                lat, lon = lon, lat
            if lat < -90 or lat > 90 or lon < -180 or lon > 180:
                return None

            return [lon, lat]
        except (ValueError, TypeError):
            return None

    def _get_call_info(self, data: dict, field: str) -> Optional[str]:
        """Gets call information from the first call entry."""
        calls = self._get_nested(data, f"{PRE}.call")
        if calls:
            first_call = self.ensure_list(calls)[0]
            return first_call.get(field)
        return None

    """ HELPER METHODS  """

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[date]:
        """Parses date strings in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format."""
        if date_str:
            try:
                # First try YYYY-MM-DD format
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                try:
                    # Then try YYYY-MM-DD HH:MM:SS format
                    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()
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
