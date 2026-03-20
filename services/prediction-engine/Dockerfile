FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

COPY services/prediction-engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
RUN pip install --no-cache-dir -e /app/shared/

COPY services/prediction-engine/src/ /app/src/

RUN chown -R app:app /app

USER app

ENV PYTHONPATH=/app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "from shared.config.settings import get_settings; from sqlalchemy import create_engine, text; s = get_settings(); url = s.database_url_with_ssl if s.is_production else s.database_url; e = create_engine(url); c = e.connect(); c.execute(text('SELECT 1')); c.close(); e.dispose()" || exit 1

CMD ["python", "-m", "src.main"]
