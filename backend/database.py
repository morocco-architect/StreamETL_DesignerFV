"""
StreamETL Designer — Database connectivity (PostgreSQL / SQLAlchemy)

Fixes applied vs. the original version:
- Reads the connection string from config.py (single source of truth)
  instead of duplicating the os.getenv call.
- get_db() no longer lets a Postgres outage crash the request: it raises a
  clean HTTP 503 instead of an unhandled OperationalError, so the rest of
  the API (Kafka topics, code generation, execution) keeps working even
  when the "save pipeline" feature's database is unavailable.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError
from fastapi import HTTPException

from config import DATABASE_URL

if DATABASE_URL.startswith("sqlite"):
    # Useful for tests and lightweight local validation without PostgreSQL.
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
else:
    engine = create_engine(
        DATABASE_URL,
        connect_args={"client_encoding": "utf8"},
        pool_pre_ping=True,
    )
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """FastAPI dependency yielding a DB session.

    Raises HTTP 503 instead of propagating a raw connection error when
    Postgres is unreachable, so pipeline-persistence endpoints fail
    gracefully while the rest of the API stays usable.
    """
    db = SessionLocal()
    try:
        yield db
    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Impossible de se connecter à la base de données PostgreSQL. "
            "Vérifiez que le conteneur 'postgres' est démarré.",
        ) from exc
    finally:
        db.close()
