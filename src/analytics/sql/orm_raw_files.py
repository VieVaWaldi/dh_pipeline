from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Float, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class RawFiles(Base):
    __tablename__ = "raw_files"
    __table_args__ = (
        UniqueConstraint("analysis_date", "source_query_id", name="uq_analysis_source"),
        {"schema": "analytics"},
    )

    id = Column(Integer, primary_key=True)
    analysis_date = Column(DateTime, nullable=False, default=datetime.now)
    source_query_id = Column(String, nullable=False)
    total_disk_usage_gb = Column(Float, nullable=False)
    file_types_total = Column(JSONB, nullable=False)
    checkpoints = Column(JSONB, nullable=False)