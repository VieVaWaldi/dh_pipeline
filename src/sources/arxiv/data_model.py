from datetime import datetime
from typing import List, Optional

from sqlalchemy import ForeignKey, Text, ARRAY, String
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Entry(Base):
    __tablename__ = "entry"
    __table_args__ = {"schema": "arxiv"}

    id: Mapped[int] = mapped_column(primary_key=True)
    id_original: Mapped[str] = mapped_column(unique=True)
    title: Mapped[str] = mapped_column(unique=True, nullable=False)

    doi: Mapped[Optional[str]] = mapped_column(unique=True)
    summary: Mapped[Optional[str]] = mapped_column()
    full_text: Mapped[Optional[str]] = mapped_column()
    journal_ref: Mapped[Optional[str]] = mapped_column()
    comment: Mapped[Optional[str]] = mapped_column()
    primary_category: Mapped[Optional[str]] = mapped_column()
    category_term: Mapped[Optional[str]] = mapped_column()
    categories: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    published_date: Mapped[Optional[datetime]] = mapped_column()
    updated_date: Mapped[Optional[datetime]] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    authors: Mapped[List["Author"]] = relationship(
        secondary="arxiv.j_entry_author", back_populates="entries"
    )
    links: Mapped[List["Link"]] = relationship(
        secondary="arxiv.j_entry_link", back_populates="entries"
    )


class Author(Base):
    __tablename__ = "author"
    __table_args__ = {"schema": "arxiv"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True, nullable=False)
    affiliation: Mapped[Optional[str]] = mapped_column()
    affiliations: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    entries: Mapped[List["Entry"]] = relationship(
        secondary="arxiv.j_entry_author", back_populates="authors"
    )


class Link(Base):
    __tablename__ = "link"
    __table_args__ = {"schema": "arxiv"}

    id: Mapped[int] = mapped_column(primary_key=True)
    href: Mapped[str] = mapped_column(nullable=False)
    title: Mapped[Optional[str]] = mapped_column()
    rel: Mapped[Optional[str]] = mapped_column()
    type: Mapped[Optional[str]] = mapped_column()

    created_at: Mapped[datetime] = mapped_column(default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        default=datetime.now, onupdate=datetime.now
    )

    # Relationships
    entries: Mapped[List["Entry"]] = relationship(
        secondary="arxiv.j_entry_link", back_populates="links"
    )


""" Junction tables """


class EntryAuthor(Base):
    __tablename__ = "j_entry_author"
    __table_args__ = {"schema": "arxiv"}

    entry_id: Mapped[int] = mapped_column(
        ForeignKey("arxiv.entry.id"), primary_key=True
    )
    author_id: Mapped[int] = mapped_column(
        ForeignKey("arxiv.author.id"), primary_key=True
    )
    author_position: Mapped[int] = mapped_column(nullable=False)


class EntryLink(Base):
    __tablename__ = "j_entry_link"
    __table_args__ = {"schema": "arxiv"}

    entry_id: Mapped[int] = mapped_column(
        ForeignKey("arxiv.entry.id"), primary_key=True
    )
    link_id: Mapped[int] = mapped_column(ForeignKey("arxiv.link.id"), primary_key=True)
