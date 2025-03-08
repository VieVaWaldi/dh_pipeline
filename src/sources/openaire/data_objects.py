from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class Subject:
    scheme: str
    value: str
    provenance_type: Optional[str] = None
    trust: Optional[float] = None


@dataclass
class Country:
    code: str
    label: Optional[str]


@dataclass
class Organization:
    id: Optional[str]
    legal_name: str
    legal_short_name: Optional[str]
    is_first_listed: bool
    geolocation: Optional[list[float]]

    alternative_names: List[str]
    website_url: Optional[str]
    country: Optional[Country]
    # pids: List[PID]

    # For relation context
    relation_type: Optional[str] = None  # e.g., "hasParticipant"
    validation_date: Optional[datetime] = None
    validated: Optional[bool] = None


@dataclass
class FundingLevel:
    id: str
    name: str
    description: Optional[str]
    parent: Optional[dict] = None
    class_type: Optional[str] = None  # e.g., "nsf:fundingStream"


@dataclass
class Funder:
    id: str
    short_name: str
    name: str
    jurisdiction: str


@dataclass
class FundingTree:
    funder: Funder
    funding_level_0: Optional[FundingLevel] = None
    funding_level_1: Optional[FundingLevel] = None


@dataclass
class Grant:
    currency: Optional[str]
    funded_amount: Optional[float]
    total_cost: Optional[float]


@dataclass
class H2020Programme:
    code: str
    description: Optional[str]


@dataclass
class Funding:
    funding_stream: dict
    jurisdiction: str
    name: str
    short_name: str


@dataclass
class Measure:
    id: str
    score: str


@dataclass
class OpenaireProject:
    id_original: str
    id_openaire: Optional[str]
    code: str
    title: str
    doi: Optional[str]
    acronym: Optional[str]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    duration: Optional[str]
    summary: Optional[str]

    # Flags and mandates
    ec_article29_3: Optional[bool]
    open_access_mandate_publications: Optional[bool]
    open_access_mandate_dataset: Optional[bool]
    ecsc39: Optional[bool]

    # Financial info
    total_cost: Optional[float]
    funded_amount: Optional[float]
    granted: Optional[Grant]

    # Web and call info
    website_url: Optional[str]
    call_identifier: Optional[str]

    # Classifications and keywords
    subjects: List[Subject]
    keywords: Optional[str]

    # Related entities
    funding_tree: Optional[FundingTree]
    fundings: List[Funding]
    h2020_programmes: List[H2020Programme]
    organizations: List[Organization]

    # Metrics
    measures: List[Measure]
