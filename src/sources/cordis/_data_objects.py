from dataclasses import dataclass
from datetime import date
from typing import List, Optional


@dataclass
class Person:
    title: Optional[str]
    name: Optional[str]  # For result authors which just have a name
    first_name: Optional[str]
    last_name: Optional[str]
    telephone_number: Optional[str]


@dataclass
class Topic:
    name: str
    level: int


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
    sme: Optional[bool]
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
    id_original: str
    type: str
    doi: str
    title: str
    publication_date: Optional[date]  # contentUpdateDate in source
    journal: Optional[str]
    summary: Optional[str]  # description in source
    comment: Optional[str]  # teaser in source

    institutions: List[Institution]
    topics: List[Topic]
    people: List[Person]
    weblinks: List[Weblink]


@dataclass
class CordisProject:
    id_original: str
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
