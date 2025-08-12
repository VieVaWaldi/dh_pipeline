from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    DateTime,
    ForeignKey,
    Text,
    Float,
    Boolean,
    Date,
    ARRAY,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


""" Base """


class Project(Base):
    __tablename__ = "project"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    id_original = Column(Text, unique=True, nullable=False)
    id_openaire = Column(Text, unique=True, nullable=False)
    code = Column(Text, nullable=False)
    title = Column(Text, nullable=False)
    doi = Column(Text)
    acronym = Column(Text)
    start_date = Column(Date)
    end_date = Column(Date)
    duration = Column(Text)
    summary = Column(Text)
    keywords = Column(Text)

    # Flags and mandates
    ec_article29_3 = Column(Boolean)
    open_access_mandate_publications = Column(Boolean)
    open_access_mandate_dataset = Column(Boolean)
    ecsc39 = Column(Boolean)

    # Financial info
    total_cost = Column(Float)
    funded_amount = Column(Float)

    # Web and call info
    website_url = Column(Text)
    call_identifier = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    research_outputs = relationship(
        "ResearchOutput",
        secondary="openaire.j_project_researchoutput",
        back_populates="projects",
    )
    organizations = relationship(
        "Organization",
        secondary="openaire.j_project_organization",
        back_populates="projects",
    )
    subjects = relationship(
        "Subject", secondary="openaire.j_project_subject", back_populates="projects"
    )
    measures = relationship(
        "Measure", secondary="openaire.j_project_measure", back_populates="projects"
    )
    funders = relationship(
        "Funder", secondary="openaire.j_project_funder", back_populates="projects"
    )
    funding_streams = relationship(
        "FundingStream",
        secondary="openaire.j_project_funding_stream",
        back_populates="projects",
    )
    h2020_programmes = relationship(
        "H2020Programme",
        secondary="openaire.j_project_h2020_programme",
        back_populates="projects",
    )


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    id_original = Column(Text, unique=True, nullable=False)
    main_title = Column(Text, nullable=False)
    sub_title = Column(Text)
    publication_date = Column(Date)
    publisher = Column(Text)
    type = Column(Text)

    language_code = Column(Text)
    language_label = Column(Text)

    open_access_color = Column(Text)
    publicly_funded = Column(Boolean)
    is_green = Column(Boolean)
    is_in_diamond_journal = Column(Boolean)

    description = Column(Text)

    citation_count = Column(Float)
    influence = Column(Float)
    popularity = Column(Float)
    impulse = Column(Float)
    citation_class = Column(Text)
    influence_class = Column(Text)
    impulse_class = Column(Text)
    popularity_class = Column(Text)

    container_id = Column(Integer, ForeignKey("openaire.container.id"))

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    projects = relationship(
        "Project",
        secondary="openaire.j_project_researchoutput",
        back_populates="research_outputs",
    )
    authors = relationship(
        "Author",
        secondary="openaire.j_researchoutput_author",
        back_populates="research_outputs",
    )
    organizations = relationship(
        "Organization",
        secondary="openaire.j_researchoutput_organization",
        back_populates="research_outputs",
    )
    container = relationship("Container", back_populates="research_outputs")


class Author(Base):
    __tablename__ = "author"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    full_name = Column(Text, unique=True, nullable=False)
    first_name = Column(Text)
    surname = Column(Text)
    pid = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    research_outputs = relationship(
        "ResearchOutput",
        secondary="openaire.j_researchoutput_author",
        back_populates="authors",
    )


class Container(Base):
    __tablename__ = "container"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    issn_printed = Column(Text)
    issn_online = Column(Text)
    issn_linking = Column(Text)

    volume = Column(Text)
    issue = Column(Text)
    start_page = Column(Text)
    end_page = Column(Text)
    edition = Column(Text)

    conference_place = Column(Text)
    conference_date = Column(Date)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    research_outputs = relationship("ResearchOutput", back_populates="container")


class Organization(Base):
    __tablename__ = "organization"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    original_id = Column(Text)
    legal_name = Column(Text, unique=True, nullable=False)
    legal_short_name = Column(Text)
    is_first_listed = Column(Boolean, default=False, nullable=False)
    geolocation = Column(ARRAY(Float))
    alternative_names = Column(ARRAY(Text))
    website_url = Column(Text)
    country_code = Column(Text)
    country_label = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    projects = relationship(
        "Project",
        secondary="openaire.j_project_organization",
        back_populates="organizations",
    )

    research_outputs = relationship(
        "ResearchOutput",
        secondary="openaire.j_researchoutput_organization",
        back_populates="organizations",
    )


""" Entities """


class Subject(Base):
    __tablename__ = "subject"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    value = Column(Text, unique=True, nullable=False)
    scheme = Column(Text)
    provenance_type = Column(Text)
    trust = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    projects = relationship(
        "Project", secondary="openaire.j_project_subject", back_populates="subjects"
    )


class Measure(Base):
    __tablename__ = "measure"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    score = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    projects = relationship(
        "Project", secondary="openaire.j_project_measure", back_populates="measures"
    )


""" Funding Entities """


class Funder(Base):
    __tablename__ = "funder"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    original_id = Column(Text, unique=True, nullable=False)
    name = Column(Text, unique=True, nullable=False)
    short_name = Column(Text, nullable=False)
    jurisdiction = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    projects = relationship(
        "Project", secondary="openaire.j_project_funder", back_populates="funders"
    )


class FundingStream(Base):
    __tablename__ = "funding_stream"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    original_id = Column(Text, unique=True, nullable=False)
    name = Column(Text, nullable=False)
    description = Column(Text)
    parent_id = Column(Integer, ForeignKey("openaire.funding_stream.id"))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Self-referential relationship for hierarchy
    parent = relationship("FundingStream", remote_side=[id], backref="children")

    # Relationships
    projects = relationship(
        "Project",
        secondary="openaire.j_project_funding_stream",
        back_populates="funding_streams",
    )


class H2020Programme(Base):
    __tablename__ = "h2020_programme"
    __table_args__ = {"schema": "openaire"}

    id = Column(Integer, primary_key=True)
    code = Column(Text, unique=True, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    projects = relationship(
        "Project",
        secondary="openaire.j_project_h2020_programme",
        back_populates="h2020_programmes",
    )


""" Junction Tables """


class JunctionProjectResearchOutput(Base):
    __tablename__ = "j_project_researchoutput"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    researchoutput_id = Column(
        Integer,
        ForeignKey("openaire.researchoutput.id", ondelete="CASCADE"),
        primary_key=True,
    )
    relation_type = Column(Text)


class JunctionProjectOrganization(Base):
    __tablename__ = "j_project_organization"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    organization_id = Column(
        Integer,
        ForeignKey("openaire.organization.id", ondelete="CASCADE"),
        primary_key=True,
    )
    relation_type = Column(Text)
    validation_date = Column(Date)
    validated = Column(Boolean)


class JunctionProjectSubject(Base):
    __tablename__ = "j_project_subject"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    subject_id = Column(
        Integer, ForeignKey("openaire.subject.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectMeasure(Base):
    __tablename__ = "j_project_measure"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    measure_id = Column(
        Integer, ForeignKey("openaire.measure.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectFunder(Base):
    __tablename__ = "j_project_funder"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    funder_id = Column(
        Integer, ForeignKey("openaire.funder.id", ondelete="CASCADE"), primary_key=True
    )
    currency = Column(Text)
    funded_amount = Column(Float)
    total_cost = Column(Float)


class JunctionProjectFundingStream(Base):
    __tablename__ = "j_project_funding_stream"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    funding_stream_id = Column(
        Integer,
        ForeignKey("openaire.funding_stream.id", ondelete="CASCADE"),
        primary_key=True,
    )


class JunctionProjectH2020Programme(Base):
    __tablename__ = "j_project_h2020_programme"
    __table_args__ = {"schema": "openaire"}

    project_id = Column(
        Integer, ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    h2020_programme_id = Column(
        Integer,
        ForeignKey("openaire.h2020_programme.id", ondelete="CASCADE"),
        primary_key=True,
    )


class JunctionResearchOutputAuthor(Base):
    __tablename__ = "j_researchoutput_author"
    __table_args__ = {"schema": "openaire"}

    research_output_id = Column(
        Integer,
        ForeignKey("openaire.researchoutput.id", ondelete="CASCADE"),
        primary_key=True,
    )
    author_id = Column(
        Integer, ForeignKey("openaire.author.id", ondelete="CASCADE"), primary_key=True
    )
    rank = Column(Float)


class JunctionResearchOutputOrganization(Base):
    __tablename__ = "j_researchoutput_organization"
    __table_args__ = {"schema": "openaire"}

    research_output_id = Column(
        Integer,
        ForeignKey("openaire.researchoutput.id", ondelete="CASCADE"),
        primary_key=True,
    )
    organization_id = Column(
        Integer,
        ForeignKey("openaire.organization.id", ondelete="CASCADE"),
        primary_key=True,
    )
    relation_type = Column(Text)
    country_code = Column(Text)
    country_label = Column(Text)
