from datetime import datetime
from typing import List, Optional

from sqlalchemy import ForeignKey, Text, ARRAY, String
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Work(Base):
    __tablename__ = "work"
    __table_args__ = {"schema": "coreac"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[str] = mapped_column(unique=True)

    title: Mapped[str] = mapped_column(unique=True, nullable=False)
    doi: Mapped[Optional[str]] = mapped_column(unique=True)
    arxiv_id: Mapped[Optional[str]] = mapped_column(unique=True)
    mag_id: Mapped[Optional[str]] = mapped_column(unique=True)
    pubmed_id: Mapped[Optional[str]] = mapped_column(unique=True)
    oai_ids: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))

    language_code: Mapped[Optional[str]] = mapped_column()
    language_name: Mapped[Optional[str]] = mapped_column()
    document_type: Mapped[Optional[str]] = mapped_column()
    field_of_study: Mapped[Optional[str]] = mapped_column()
    abstract: Mapped[Optional[str]] = mapped_column(Text())
    fulltext: Mapped[Optional[str]] = mapped_column(Text())

    publisher: Mapped[Optional[str]] = mapped_column()
    authors: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    contributors: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    journals_title: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    download_url: Mapped[Optional[str]] = mapped_column()
    outputs: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    source_fulltext_urls: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))

    year_published: Mapped[Optional[str]] = mapped_column()
    created_date: Mapped[Optional[datetime]] = mapped_column()
    updated_date: Mapped[Optional[datetime]] = mapped_column()
    published_date: Mapped[Optional[datetime]] = mapped_column()
    deposited_date: Mapped[Optional[datetime]] = mapped_column()
    accepted_date: Mapped[Optional[datetime]] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    links: Mapped[List["Link"]] = relationship(
        secondary="coreac.j_work_link", back_populates="works"
    )
    references: Mapped[List["Reference"]] = relationship(
        secondary="coreac.j_work_reference", back_populates="works"
    )
    data_providers: Mapped[List["DataProvider"]] = relationship(
        secondary="coreac.j_work_data_provider", back_populates="works"
    )


class Link(Base):
    __tablename__ = "link"
    __table_args__ = {"schema": "coreac"}

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[Optional[str]] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    works: Mapped[List["Work"]] = relationship(
        secondary="coreac.j_work_link", back_populates="links"
    )


class Reference(Base):
    __tablename__ = "reference"
    __table_args__ = {"schema": "coreac"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[Optional[str]] = mapped_column()
    title: Mapped[str] = mapped_column(unique=True, nullable=False)
    authors: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    date: Mapped[Optional[datetime]] = mapped_column()
    doi: Mapped[Optional[str]] = mapped_column()
    raw: Mapped[Optional[str]] = mapped_column(Text())
    cites: Mapped[Optional[str]] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    works: Mapped[List["Work"]] = relationship(
        secondary="coreac.j_work_reference", back_populates="references"
    )


class DataProvider(Base):
    __tablename__ = "data_provider"
    __table_args__ = {"schema": "coreac"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[Optional[str]] = mapped_column()
    name: Mapped[str] = mapped_column(nullable=False)
    url: Mapped[Optional[str]] = mapped_column()
    logo: Mapped[Optional[str]] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    works: Mapped[List["Work"]] = relationship(
        secondary="coreac.j_work_data_provider", back_populates="data_providers"
    )


""" Junction tables """


class WorkLink(Base):
    __tablename__ = "j_work_link"
    __table_args__ = {"schema": "coreac"}

    work_id: Mapped[int] = mapped_column(ForeignKey("coreac.work.id"), primary_key=True)
    link_id: Mapped[int] = mapped_column(ForeignKey("coreac.link.id"), primary_key=True)


class WorkReference(Base):
    __tablename__ = "j_work_reference"
    __table_args__ = {"schema": "coreac"}

    work_id: Mapped[int] = mapped_column(ForeignKey("coreac.work.id"), primary_key=True)
    reference_id: Mapped[int] = mapped_column(
        ForeignKey("coreac.reference.id"), primary_key=True
    )


class WorkDataProvider(Base):
    __tablename__ = "j_work_data_provider"
    __table_args__ = {"schema": "coreac"}

    work_id: Mapped[int] = mapped_column(ForeignKey("coreac.work.id"), primary_key=True)
    data_provider_id: Mapped[int] = mapped_column(
        ForeignKey("coreac.data_provider.id"), primary_key=True
    )
