FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

COPY services/notification-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
RUN pip install --no-cache-dir -e /app/shared/

COPY services/notification-service/src/ /app/src/

RUN chown -R app:app /app

USER app

ENV PYTHONPATH=/app

# HEALTHCHECK: TODO — add when service has a main entrypoint

CMD ["python", "-m", "src.main"]
