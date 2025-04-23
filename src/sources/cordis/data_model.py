from datetime import datetime
from typing import List, Optional

from sqlalchemy import ForeignKey, Text, Float, Boolean, Date, ARRAY
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


""" Base """


class Person(Base):
    __tablename__ = "person"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[Optional[str]] = mapped_column(Text)
    name: Mapped[Optional[str]] = mapped_column(Text)
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    telephone_number: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    institutions: Mapped[List["Institution"]] = relationship(
        secondary="cordis.j_institution_person", back_populates="persons"
    )
    research_outputs: Mapped[List["ResearchOutput"]] = relationship(
        secondary="cordis.j_researchoutput_person", back_populates="persons"
    )


class Topic(Base):
    __tablename__ = "topic"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    level: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    projects: Mapped[List["Project"]] = relationship(
        secondary="cordis.j_project_topic", back_populates="topics"
    )
    research_outputs: Mapped[List["ResearchOutput"]] = relationship(
        secondary="cordis.j_researchoutput_topic", back_populates="topics"
    )


class Weblink(Base):
    __tablename__ = "weblink"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    projects: Mapped[List["Project"]] = relationship(
        secondary="cordis.j_project_weblink", back_populates="weblinks"
    )
    research_outputs: Mapped[List["ResearchOutput"]] = relationship(
        secondary="cordis.j_researchoutput_weblink", back_populates="weblinks"
    )


class FundingProgramme(Base):
    __tablename__ = "fundingprogramme"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(Text)
    short_title: Mapped[Optional[str]] = mapped_column(Text)
    framework_programme: Mapped[Optional[str]] = mapped_column(Text)
    pga: Mapped[Optional[str]] = mapped_column(Text)
    rcn: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    projects: Mapped[List["Project"]] = relationship(
        secondary="cordis.j_project_fundingprogramme",
        back_populates="funding_programmes",
    )


""" Main Tables """


class Institution(Base):
    __tablename__ = "institution"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    legal_name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    sme: Mapped[Optional[bool]] = mapped_column(Boolean)
    url: Mapped[Optional[str]] = mapped_column(Text)
    short_name: Mapped[Optional[str]] = mapped_column(Text)
    vat_number: Mapped[Optional[str]] = mapped_column(Text)
    street: Mapped[Optional[str]] = mapped_column(Text)
    postbox: Mapped[Optional[str]] = mapped_column(Text)
    postalcode: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(Text)
    geolocation: Mapped[Optional[list[float]]] = mapped_column(ARRAY(Float))
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    persons: Mapped[List["Person"]] = relationship(
        secondary="cordis.j_institution_person", back_populates="institutions"
    )
    projects: Mapped[List["Project"]] = relationship(
        secondary="cordis.j_project_institution", back_populates="institutions"
    )
    research_outputs: Mapped[List["ResearchOutput"]] = relationship(
        secondary="cordis.j_researchoutput_institution", back_populates="institutions"
    )


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    from_pdf: Mapped[bool] = mapped_column(Boolean)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    doi: Mapped[Optional[str]] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    publication_date: Mapped[Optional[datetime]] = mapped_column(Date)
    journal: Mapped[Optional[str]] = mapped_column(Text)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    comment: Mapped[Optional[str]] = mapped_column(Text)
    fulltext: Mapped[Optional[str]] = mapped_column(Text)
    funding_number: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    projects: Mapped[List["Project"]] = relationship(
        secondary="cordis.j_project_researchoutput", back_populates="research_outputs"
    )
    persons: Mapped[List["Person"]] = relationship(
        secondary="cordis.j_researchoutput_person", back_populates="research_outputs"
    )
    topics: Mapped[List["Topic"]] = relationship(
        secondary="cordis.j_researchoutput_topic", back_populates="research_outputs"
    )
    weblinks: Mapped[List["Weblink"]] = relationship(
        secondary="cordis.j_researchoutput_weblink", back_populates="research_outputs"
    )
    institutions: Mapped[List["Institution"]] = relationship(
        secondary="cordis.j_researchoutput_institution",
        back_populates="research_outputs",
    )


class Project(Base):
    __tablename__ = "project"
    __table_args__ = {"schema": "cordis"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    doi: Mapped[Optional[str]] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    acronym: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[Optional[str]] = mapped_column(Text)
    start_date: Mapped[Optional[datetime]] = mapped_column(Date)
    end_date: Mapped[Optional[datetime]] = mapped_column(Date)
    ec_signature_date: Mapped[Optional[datetime]] = mapped_column(Date)
    total_cost: Mapped[Optional[float]] = mapped_column(Float)
    ec_max_contribution: Mapped[Optional[float]] = mapped_column(Float)
    objective: Mapped[Optional[str]] = mapped_column(Text)
    call_identifier: Mapped[Optional[str]] = mapped_column(Text)
    call_title: Mapped[Optional[str]] = mapped_column(Text)
    call_rcn: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    research_outputs: Mapped[List["ResearchOutput"]] = relationship(
        secondary="cordis.j_project_researchoutput", back_populates="projects"
    )
    topics: Mapped[List["Topic"]] = relationship(
        secondary="cordis.j_project_topic", back_populates="projects"
    )
    weblinks: Mapped[List["Weblink"]] = relationship(
        secondary="cordis.j_project_weblink", back_populates="projects"
    )
    funding_programmes: Mapped[List["FundingProgramme"]] = relationship(
        secondary="cordis.j_project_fundingprogramme", back_populates="projects"
    )
    institutions: Mapped[List["Institution"]] = relationship(
        secondary="cordis.j_project_institution", back_populates="projects"
    )


""" Junction Tables """

""" Junction Institution """


class JunctionInstitutionPerson(Base):
    __tablename__ = "j_institution_person"
    __table_args__ = {"schema": "cordis"}

    institution_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.institution.id", ondelete="CASCADE"), primary_key=True
    )
    person_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.person.id", ondelete="CASCADE"), primary_key=True
    )


""" Junction ResearchOutput """


class JunctionResearchOutputPerson(Base):
    __tablename__ = "j_researchoutput_person"
    __table_args__ = {"schema": "cordis"}

    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.researchoutput.id", ondelete="CASCADE"), primary_key=True
    )
    person_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.person.id", ondelete="CASCADE"), primary_key=True
    )
    person_position: Mapped[int] = mapped_column(nullable=False)


class JunctionResearchOutputTopic(Base):
    __tablename__ = "j_researchoutput_topic"
    __table_args__ = {"schema": "cordis"}

    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.researchoutput.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.topic.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionResearchOutputWeblink(Base):
    __tablename__ = "j_researchoutput_weblink"
    __table_args__ = {"schema": "cordis"}

    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.researchoutput.id", ondelete="CASCADE"), primary_key=True
    )
    weblink_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.weblink.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionResearchOutputInstitution(Base):
    __tablename__ = "j_researchoutput_institution"
    __table_args__ = {"schema": "cordis"}

    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.researchoutput.id", ondelete="CASCADE"), primary_key=True
    )
    institution_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.institution.id", ondelete="CASCADE"), primary_key=True
    )


""" Junction Project """


class JunctionProjectTopic(Base):
    __tablename__ = "j_project_topic"
    __table_args__ = {"schema": "cordis"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.project.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.topic.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectWeblink(Base):
    __tablename__ = "j_project_weblink"
    __table_args__ = {"schema": "cordis"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.project.id", ondelete="CASCADE"), primary_key=True
    )
    weblink_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.weblink.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectFundingProgramme(Base):
    __tablename__ = "j_project_fundingprogramme"
    __table_args__ = {"schema": "cordis"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.project.id", ondelete="CASCADE"), primary_key=True
    )
    fundingprogramme_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.fundingprogramme.id", ondelete="CASCADE"), primary_key=True
    )


class JunctionProjectInstitution(Base):
    __tablename__ = "j_project_institution"
    __table_args__ = {"schema": "cordis"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.project.id", ondelete="CASCADE"), primary_key=True
    )
    institution_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.institution.id", ondelete="CASCADE"), primary_key=True
    )

    institution_position: Mapped[int] = mapped_column(nullable=False)
    ec_contribution: Mapped[Optional[float]] = mapped_column(Float)
    net_ec_contribution: Mapped[Optional[float]] = mapped_column(Float)
    total_cost: Mapped[Optional[float]] = mapped_column(Float)
    type: Mapped[Optional[str]] = mapped_column(Text)
    organization_id: Mapped[Optional[str]] = mapped_column(Text)
    rcn: Mapped[Optional[int]] = mapped_column()


class JunctionProjectResearchOutput(Base):
    __tablename__ = "j_project_researchoutput"
    __table_args__ = {"schema": "cordis"}

    project_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.project.id", ondelete="CASCADE"), primary_key=True
    )
    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("cordis.researchoutput.id", ondelete="CASCADE"), primary_key=True
    )
