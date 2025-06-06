from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Date, Text, ForeignKey, Float
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

"""
ToDo:
- Split these models into folder with the right __init__.py and prohibiting the circular imports.
"""


Base = declarative_base()


source_type_enum = ENUM(
    "cordis", "arxiv", "coreac", "openaire", name="source_type", schema="core"
)


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    __table_args__ = {"schema": "core"}

    id = Column(Integer, primary_key=True)
    source_id = Column(String, nullable=False)
    source_system = Column(source_type_enum, nullable=False)

    doi = Column(String)
    arxiv_id = Column(String)

    publication_date = Column(Date)
    updated_date = Column(Date)
    language_code = Column(String)
    type = Column(String)

    title = Column(Text, nullable=False)
    abstract = Column(Text)
    full_text = Column(Text)
    comment = Column(Text)
    funding_number = Column(String)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    topics_openalex = relationship(
        "TopicOpenalexKeywordDensity",
        secondary="core.j_researchoutput_topic_openalex_keyword_density",
        back_populates="research_outputs",
    )


class TopicOpenalexKeywordDensity(Base):
    __tablename__ = "topic_openalex_keyword_density"
    __table_args__ = {"schema": "core"}

    id = Column(Integer, primary_key=True)

    subfield_id = Column(Integer, nullable=False)
    field_id = Column(Integer, nullable=False)
    domain_id = Column(Integer, nullable=False)

    topic_name = Column(Text, unique=True, nullable=False)
    subfield_name = Column(Text, nullable=False)
    field_name = Column(Text, nullable=False)
    domain_name = Column(Text, nullable=False)

    keywords = Column(Text)
    summary = Column(Text)
    wikipedia_url = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    research_outputs = relationship(
        "ResearchOutput",
        secondary="core.j_researchoutput_topic_openalex_keyword_density",
        back_populates="topics_openalex",
    )


class JResearchOutputTopicOpenalexKeywordDensity(Base):
    __tablename__ = "j_researchoutput_topic_openalex_keyword_density"
    __table_args__ = {"schema": "core"}

    researchoutput_id = Column(
        Integer,
        ForeignKey("core.researchoutput.id", ondelete="CASCADE"),
        primary_key=True,
    )
    topic_openalex_keyword_density_id = Column(
        Integer,
        ForeignKey("core.topic_openalex_keyword_density.id", ondelete="CASCADE"),
        primary_key=True,
    )
    score = Column(Float)

    research_output = relationship("ResearchOutput")
    topic_openalex = relationship("TopicOpenalexKeywordDensity")
