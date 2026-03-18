"""
Database engine and session factory.

All database access flows through get_session(). Services never create engines
or sessions directly — this module owns connection pooling and lifecycle.

Usage:
    from shared.db import get_session

    with get_session() as session:
        cities = session.execute(select(City)).scalars().all()
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from shared.config.settings import get_settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    """Return the shared SQLAlchemy engine. Creates it on first call.

    Uses SSL in production, plain connection in development.
    Pool size tuned for our workload: 5 connections base, burst to 10.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        url = settings.database_url_with_ssl if settings.is_production else settings.database_url
        _engine = create_engine(
            url,
            pool_size=5,
            max_overflow=5,
            pool_pre_ping=True,
            echo=False,
        )
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """Return the shared session factory. Creates it on first call."""
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a transactional session that auto-commits on success, rolls back on error.

    Usage:
        with get_session() as session:
            session.add(city)
            # commits automatically when the block exits cleanly
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def reset_engine() -> None:
    """Dispose engine and clear cached factories. For testing only."""
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None
