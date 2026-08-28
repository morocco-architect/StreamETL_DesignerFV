from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from database import Base

# Portable in tests/SQLite, native JSONB in PostgreSQL production.
JSON_DOCUMENT = JSON().with_variant(JSONB, "postgresql")


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, default="Pipeline_Sans_Nom")
    nodes = Column(JSON_DOCUMENT, nullable=False)
    edges = Column(JSON_DOCUMENT, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
