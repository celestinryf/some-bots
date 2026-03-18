FROM python:3.12-slim

# Non-root user for security
RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

# Install dependencies first (cache layer)
COPY services/data-ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install shared package
COPY shared/ /app/shared/
RUN pip install --no-cache-dir -e /app/shared/

# Copy service code
# Copy service code
COPY services/data-ingestion/src/ /app/src/
COPY services/data-ingestion/data/ /app/data/

# Own everything by app user
RUN chown -R app:app /app

USER app

ENV PYTHONPATH=/app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "from shared.config.settings import get_settings; from sqlalchemy import create_engine, text; s = get_settings(); url = s.database_url_with_ssl if s.is_production else s.database_url; e = create_engine(url); c = e.connect(); c.execute(text('SELECT 1')); c.close(); e.dispose()" || exit 1

CMD ["python", "-m", "src.main"]
