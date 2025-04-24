from datetime import datetime
from typing import List, Optional

from sqlalchemy import ForeignKey, Text, Float, Boolean, Date, ARRAY
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


""" Base """


class Project(Base):
    __tablename__ = "project"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    id_openaire: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    doi: Mapped[Optional[str]] = mapped_column(Text)
    acronym: Mapped[Optional[str]] = mapped_column(Text)
    start_date: Mapped[Optional[datetime]] = mapped_column(Date)
    end_date: Mapped[Optional[datetime]] = mapped_column(Date)
    duration: Mapped[Optional[str]] = mapped_column(Text)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    keywords: Mapped[Optional[str]] = mapped_column(Text)

    # Flags and mandates
    ec_article29_3: Mapped[Optional[bool]] = mapped_column(Boolean)
    open_access_mandate_publications: Mapped[Optional[bool]] = mapped_column(Boolean)
    open_access_mandate_dataset: Mapped[Optional[bool]] = mapped_column(Boolean)
    ecsc39: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Financial info
    total_cost: Mapped[Optional[float]] = mapped_column(Float)
    funded_amount: Mapped[Optional[float]] = mapped_column(Float)

    # Web and call info
    website_url: Mapped[Optional[str]] = mapped_column(Text)
    call_identifier: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    research_outputs: Mapped[List["ResearchOutput"]] = relationship(
        secondary="openaire.j_project_researchoutput", back_populates="projects"
    )
    organizations: Mapped[List["Organization"]] = relationship(
        secondary="openaire.j_project_organization", back_populates="projects"
    )
    subjects: Mapped[List["Subject"]] = relationship(
        secondary="openaire.j_project_subject", back_populates="projects"
    )
    measures: Mapped[List["Measure"]] = relationship(
        secondary="openaire.j_project_measure", back_populates="projects"
    )
    funders: Mapped[List["Funder"]] = relationship(
        secondary="openaire.j_project_funder", back_populates="projects"
    )
    funding_streams: Mapped[List["FundingStream"]] = relationship(
        secondary="openaire.j_project_funding_stream", back_populates="projects"
    )
    h2020_programmes: Mapped[List["H2020Programme"]] = relationship(
        secondary="openaire.j_project_h2020_programme", back_populates="projects"
    )


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    # WIP as per schema
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_researchoutput", back_populates="research_outputs"
    )


class Organization(Base):
    __tablename__ = "organization"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    original_id: Mapped[Optional[str]] = mapped_column(Text)
    legal_name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    legal_short_name: Mapped[Optional[str]] = mapped_column(Text)
    is_first_listed: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    geolocation: Mapped[Optional[list[float]]] = mapped_column(ARRAY(Float))
    alternative_names: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    website_url: Mapped[Optional[str]] = mapped_column(Text)
    country_code: Mapped[Optional[str]] = mapped_column(Text)
    country_label: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_organization", back_populates="organizations"
    )


""" Entities """


class Subject(Base):
    __tablename__ = "subject"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    scheme: Mapped[str] = mapped_column(Text)
    provenance_type: Mapped[Optional[str]] = mapped_column(Text)
    trust: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_subject", back_populates="subjects"
    )


class Measure(Base):
    __tablename__ = "measure"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    score: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_measure", back_populates="measures"
    )


""" Funding Entities """


class Funder(Base):
    __tablename__ = "funder"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    original_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    short_name: Mapped[str] = mapped_column(Text, nullable=False)
    jurisdiction: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_funder", back_populates="funders"
    )


class FundingStream(Base):
    __tablename__ = "funding_stream"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    original_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    parent_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("openaire.funding_stream.id")
    )
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Self-referential relationship for hierarchy
    parent: Mapped[Optional["FundingStream"]] = relationship(
        "FundingStream", remote_side=[id], backref="children"
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_funding_stream", back_populates="funding_streams"
    )


class H2020Programme(Base):
    __tablename__ = "h2020_programme"
    __table_args__ = {"schema": "openaire"}

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    projects: Mapped[List["Project"]] = relationship(
        secondary="openaire.j_project_h2020_programme",
        back_populates="h2020_programmes",
    )


""" Junction Tables """


class JunctionProjectResearchOutput(Base):
    __tablename__ = "j_project_researchoutput"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.researchoutput.id", ondelete="CASCADE"), primary_key=True
    )
    relation_type: Mapped[Optional[str]] = mapped_column(Text)


class JunctionProjectOrganization(Base):
    __tablename__ = "j_project_organization"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.organization.id", ondelete="CASCADE"), primary_key=True
    )
    relation_type: Mapped[Optional[str]] = mapped_column(Text)
    validation_date: Mapped[Optional[datetime]] = mapped_column(Date)
    validated: Mapped[Optional[bool]] = mapped_column(Boolean)


class JunctionProjectSubject(Base):
    __tablename__ = "j_project_subject"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    subject_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.subject.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectMeasure(Base):
    __tablename__ = "j_project_measure"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    measure_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.measure.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectFunder(Base):
    __tablename__ = "j_project_funder"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    funder_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.funder.id", ondelete="CASCADE"), primary_key=True
    )
    currency: Mapped[Optional[str]] = mapped_column(Text)
    funded_amount: Mapped[Optional[float]] = mapped_column(Float)
    total_cost: Mapped[Optional[float]] = mapped_column(Float)


class JunctionProjectFundingStream(Base):
    __tablename__ = "j_project_funding_stream"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    funding_stream_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.funding_stream.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectH2020Programme(Base):
    __tablename__ = "j_project_h2020_programme"
    __table_args__ = {"schema": "openaire"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.project.id", ondelete="CASCADE"), primary_key=True
    )
    h2020_programme_id: Mapped[int] = mapped_column(
        ForeignKey("openaire.h2020_programme.id", ondelete="CASCADE"), primary_key=True
    )
