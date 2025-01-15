from sqlalchemy import (
    Integer,
    ForeignKey,
    Boolean,
)
from sqlalchemy.orm import mapped_column, relationship, Mapped

from datamodels.digicher.entities import Base

"""
JUNCTION Tables --- Publications
"""


class ResearchOutputsPeople(Base):
    __tablename__ = "researchoutputs_people"

    publication_id: Mapped[int] = mapped_column(
        ForeignKey("researchoutputs.id", ondelete="CASCADE"), primary_key=True
    )
    person_id: Mapped[int] = mapped_column(
        ForeignKey("people.id", ondelete="CASCADE"), primary_key=True
    )
    person_position: Mapped[int] = mapped_column(nullable=False)

    publication: Mapped["ResearchOutputs"] = relationship(back_populates="people")
    person: Mapped["People"] = relationship(back_populates="researchoutputs")


class ResearchOutputsTopics(Base):
    __tablename__ = "researchoutputs_topics"

    publication_id = mapped_column(
        Integer, ForeignKey("researchoutputs.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id = mapped_column(
        Integer, ForeignKey("topics.id", ondelete="CASCADE"), primary_key=True
    )
    is_primary = mapped_column(Boolean, default=True)

    publication: Mapped["ResearchOutputs"] = relationship(back_populates="topics")
    topic: Mapped["Topics"] = relationship(back_populates="researchoutputs")


class ResearchOutputsWeblinks(Base):
    __tablename__ = "researchoutputs_weblinks"

    publication_id = mapped_column(
        Integer, ForeignKey("researchoutputs.id", ondelete="CASCADE"), primary_key=True
    )
    weblink_id = mapped_column(
        Integer, ForeignKey("weblinks.id", ondelete="CASCADE"), primary_key=True
    )

    publication: Mapped["ResearchOutputs"] = relationship(back_populates="weblinks")
    weblink: Mapped["Weblinks"] = relationship(back_populates="researchoutputs")


"""
JUNCTION Tables --- Institutions
"""


class InstitutionsPeople(Base):
    __tablename__ = "institutions_people"

    institution_id = mapped_column(
        Integer, ForeignKey("institutions.id", ondelete="CASCADE"), primary_key=True
    )
    person_id = mapped_column(
        Integer, ForeignKey("people.id", ondelete="CASCADE"), primary_key=True
    )

    institution: Mapped["Institutions"] = relationship(back_populates="people")
    person: Mapped["People"] = relationship(back_populates="institutions")


class InstitutionsResearchOutputs(Base):
    __tablename__ = "institutions_researchoutputs"

    institution_id = mapped_column(
        Integer, ForeignKey("institutions.id", ondelete="CASCADE"), primary_key=True
    )
    publication_id = mapped_column(
        Integer, ForeignKey("researchoutputs.id", ondelete="CASCADE"), primary_key=True
    )

    institution: Mapped["Institutions"] = relationship(back_populates="researchoutputs")
    publication: Mapped["ResearchOutputs"] = relationship(back_populates="institutions")


"""
JUNCTION Tables --- Projects
"""


class ProjectsTopics(Base):
    __tablename__ = "projects_topics"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id: Mapped[int] = mapped_column(
        ForeignKey("topics.id", ondelete="CASCADE"), primary_key=True
    )
    is_primary: Mapped[bool] = mapped_column(default=True)

    project: Mapped["Projects"] = relationship(back_populates="topics")
    topic: Mapped["Topics"] = relationship(back_populates="projects")


class ProjectsWeblinks(Base):
    __tablename__ = "projects_weblinks"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    weblink_id: Mapped[int] = mapped_column(
        ForeignKey("weblinks.id", ondelete="CASCADE"), primary_key=True
    )

    project: Mapped["Projects"] = relationship(back_populates="weblinks")
    weblink: Mapped["Weblinks"] = relationship(back_populates="projects")


class ProjectsResearchOutputs(Base):
    __tablename__ = "projects_researchoutputs"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    publication_id: Mapped[int] = mapped_column(
        ForeignKey("researchoutputs.id", ondelete="CASCADE"), primary_key=True
    )

    project: Mapped["Projects"] = relationship(back_populates="researchoutputs")
    publication: Mapped["ResearchOutputs"] = relationship(back_populates="projects")


class ProjectsInstitutions(Base):
    __tablename__ = "projects_institutions"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    institution_id: Mapped[int] = mapped_column(
        ForeignKey("institutions.id", ondelete="CASCADE"), primary_key=True
    )
    institution_position: Mapped[int] = mapped_column()
    ec_contribution: Mapped[int] = mapped_column()
    net_ec_contribution: Mapped[int] = mapped_column()
    total_cost: Mapped[int] = mapped_column()
    type: Mapped[str] = mapped_column()
    organization_id: Mapped[str] = mapped_column()
    rcn: Mapped[int] = mapped_column()

    project: Mapped["Projects"] = relationship(back_populates="institutions")
    institution: Mapped["Institutions"] = relationship(back_populates="projects")


class ProjectsFundingProgrammes(Base):
    __tablename__ = "projects_fundingprogrammes"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    fundingprogramme_id: Mapped[int] = mapped_column(
        ForeignKey("fundingprogrammes.id", ondelete="CASCADE"), primary_key=True
    )

    project: Mapped["Projects"] = relationship(back_populates="fundingprogrammes")
    fundingprogramme: Mapped["FundingProgrammes"] = relationship(
        back_populates="projects"
    )
