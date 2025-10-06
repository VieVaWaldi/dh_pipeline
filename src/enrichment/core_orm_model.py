from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    DateTime,
    Date,
    Text,
    ForeignKey,
    Numeric,
    Float,
    Boolean,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class TopicOA(Base):
    __tablename__ = "topicoa"
    __table_args__ = {"schema": "core"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    subfield_id = Column(Text, nullable=False)
    field_id = Column(Text, nullable=False)
    domain_id = Column(Text, nullable=False)

    topic_name = Column(Text, nullable=False)
    subfield_name = Column(Text, nullable=False)
    field_name = Column(Text, nullable=False)
    domain_name = Column(Text, nullable=False)

    keywords = Column(Text, nullable=False)
    summary = Column(Text, nullable=False)
    wikipedia_url = Column(Text, nullable=False)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationship to projects through junction table
    projects = relationship(
        "Project",
        secondary="core.j_project_topicoa",
        back_populates="topics_oa",
    )

    # Relationship to research outputs through junction table
    research_outputs = relationship(
        "ResearchOutput",
        secondary="core.j_researchoutput_topicoa",
        back_populates="topics_oa",
    )


class Project(Base):
    __tablename__ = "project"
    __table_args__ = {"schema": "core"}

    id = Column(Text, primary_key=True)
    source_id = Column(Text)
    source_system = Column(Text)
    cordis_id = Column(Text)
    doi = Column(Text)
    title = Column(Text)
    acronym = Column(Text)
    status = Column(Text)

    start_date = Column(Date)
    end_date = Column(Date)
    ec_signature_date = Column(Date)

    total_cost = Column(Numeric)  # double precision maps to Numeric
    ec_max_contribution = Column(Numeric)
    funded_amount = Column(Numeric)
    funder_total_cost = Column(Numeric)

    objective = Column(Text)
    call_identifier = Column(Text)
    call_title = Column(Text)
    call_rcn = Column(Text)
    id_original = Column(Text)
    id_openaire = Column(Text)
    code = Column(Text)
    duration = Column(Text)
    keywords = Column(Text)
    website_url = Column(Text)

    funder_name = Column(Text)
    funder_short_name = Column(Text)
    funder_jurisdiction = Column(Text)
    currency = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationship to topics through junction table
    topics_oa = relationship(
        "TopicOA",
        secondary="core.j_project_topicoa",
        back_populates="projects",
    )


class JProjectTopicOA(Base):
    __tablename__ = "j_project_topicoa"
    __table_args__ = {"schema": "core"}

    project_id = Column(
        Text,
        ForeignKey("core.project.id", ondelete="CASCADE"),
        primary_key=True,
    )
    topic_id = Column(
        Integer,
        ForeignKey("core.topicoa.id", ondelete="CASCADE"),
        primary_key=True,
    )
    score = Column(Float)

    created_at = Column(DateTime, default=datetime.now)

    # Direct relationships to the entities
    project = relationship("Project")
    topic_oa = relationship("TopicOA")


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    __table_args__ = {"schema": "core"}

    id = Column(Text, primary_key=True)
    source_id = Column(Text)
    source_system = Column(Text)
    doi = Column(Text)
    original_id = Column(Text)

    publication_date = Column(DateTime)  # timestamp without time zone
    updated_date = Column(DateTime)  # timestamp without time zone

    type = Column(Text)
    language_code = Column(Text)
    language_label = Column(Text)

    title = Column(Text)
    sub_title = Column(Text)
    abstract = Column(Text)
    fulltext = Column(Text)
    comment = Column(Text)

    # Journal/Publication details
    journal_name = Column(Text)
    journal_number = Column(Text)
    publisher = Column(Text)
    volume = Column(Text)
    issue = Column(Text)
    edition = Column(Text)

    # Page information
    start_page = Column(Text)
    end_page = Column(Text)
    page_range = Column(Text)

    # ISSN variants
    issn = Column(Text)
    issn_printed = Column(Text)
    issn_online = Column(Text)
    issn_linking = Column(Text)

    # Conference details
    conference_place = Column(Text)
    conference_date = Column(Date)

    # Funding and access
    funding_number = Column(Text)
    open_access_color = Column(Text)
    publicly_funded = Column(Boolean)
    is_green = Column(Boolean)
    is_in_diamond_journal = Column(Boolean)

    # Metrics (using Numeric for double precision)
    citation_count = Column(Numeric)
    influence = Column(Numeric)
    popularity = Column(Numeric)
    impulse = Column(Numeric)

    # Classification
    citation_class = Column(Text)
    influence_class = Column(Text)
    impulse_class = Column(Text)
    popularity_class = Column(Text)

    # Timestamps with timezone
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationship to topics through junction table
    topics_oa = relationship(
        "TopicOA",
        secondary="core.j_researchoutput_topicoa",
        back_populates="research_outputs",
    )


class JResearchOutputTopicOA(Base):
    __tablename__ = "j_researchoutput_topicoa"
    __table_args__ = {"schema": "core"}

    researchoutput_id = Column(
        Text,
        ForeignKey("core.researchoutput.id", ondelete="CASCADE"),
        primary_key=True,
    )
    topic_id = Column(
        Integer,
        ForeignKey("core.topicoa.id", ondelete="CASCADE"),
        primary_key=True,
    )
    score = Column(Float)

    created_at = Column(DateTime, default=datetime.now)

    # Direct relationships to the entities
    research_output = relationship("ResearchOutput")
    topic_oa = relationship("TopicOA")
