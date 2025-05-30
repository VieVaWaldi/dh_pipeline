from datetime import datetime
from typing import Optional

from sqlalchemy import Date, Text, ForeignKey
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


"""
ToDo:
- Split these models into folder with the right __init__.py and prohibiting the circular imports.
"""


class Base(DeclarativeBase):
    pass


source_type_enum = ENUM(
    'cordis', 'arxiv', 'coreac', 'openaire', name='source_type', schema='core'
)


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    __table_args__ = {"schema": "core"}

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[str] = mapped_column(nullable=False)
    source_system: Mapped[str] = mapped_column(source_type_enum, nullable=False)

    doi: Mapped[Optional[str]] = mapped_column()
    arxiv_id: Mapped[Optional[str]] = mapped_column()

    publication_date: Mapped[Optional[datetime]] = mapped_column(Date)
    updated_date: Mapped[Optional[datetime]] = mapped_column(Date)
    language_code: Mapped[Optional[str]] = mapped_column()
    type: Mapped[Optional[str]] = mapped_column()
    
    title: Mapped[str] = mapped_column(Text, nullable=False)
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    full_text: Mapped[Optional[str]] = mapped_column(Text)
    comment: Mapped[Optional[str]] = mapped_column(Text)
    funding_number: Mapped[Optional[str]] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    topics_openalex: Mapped[list["TopicOpenalexKeywordDensity"]] = relationship(
        "TopicOpenalexKeywordDensity",
        secondary="core.j_researchoutput_topic_openalex_keyword_density",
        back_populates="research_outputs"
    )


class TopicOpenalexKeywordDensity(Base):
    __tablename__ = "topic_openalex_keyword_density"
    __table_args__ = {"schema": "core"}

    id: Mapped[int] = mapped_column(primary_key=True)

    subfield_id: Mapped[int] = mapped_column(nullable=False)
    field_id: Mapped[int] = mapped_column(nullable=False)
    domain_id: Mapped[int] = mapped_column(nullable=False)

    topic_name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    subfield_name: Mapped[str] = mapped_column(Text, nullable=False)
    field_name: Mapped[str] = mapped_column(Text, nullable=False)
    domain_name: Mapped[str] = mapped_column(Text, nullable=False)

    keywords: Mapped[Optional[str]] = mapped_column(Text)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    wikipedia_url: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    research_outputs: Mapped[list["ResearchOutput"]] = relationship(
        "ResearchOutput",
        secondary="core.j_researchoutput_topic_openalex_keyword_density",
        back_populates="topics_openalex"
    )


class JResearchOutputTopicOpenalexKeywordDensity(Base):
    __tablename__ = "j_researchoutput_topic_openalex_keyword_density"
    __table_args__ = {"schema": "core"}

    researchoutput_id: Mapped[int] = mapped_column(
        ForeignKey("core.researchoutput.id", ondelete="CASCADE"),
        primary_key=True
    )
    topic_openalex_keyword_density_id: Mapped[int] = mapped_column(
        ForeignKey("core.topic_openalex_keyword_density.id", ondelete="CASCADE"),
        primary_key=True
    )
    score: Mapped[Optional[float]] = mapped_column()

    research_output: Mapped["ResearchOutput"] = relationship("ResearchOutput")
    topic_openalex: Mapped["TopicOpenalexKeywordDensity"] = relationship("TopicOpenalexKeywordDensity")
