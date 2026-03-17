"""
Alembic migration environment.

Loads database URL from Settings (not alembic.ini) so credentials stay in .env.
Imports all models so autogenerate can detect schema changes.
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path so `shared.*` imports resolve
# when running `alembic upgrade head` from the CLI.
_project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv

load_dotenv(_project_root / ".env")

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from shared.config.settings import get_settings
from shared.db.models import Base  # noqa: F401 — registers all models with metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    settings = get_settings()
    return settings.database_url_with_ssl if settings.is_production else settings.database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — generates SQL without connecting."""
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database connection."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
