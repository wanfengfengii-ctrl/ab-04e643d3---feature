# API image: builds the FastAPI/asyncpg service and the one-shot verifier.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt /srv/
RUN pip install --no-cache-dir -r /srv/requirements.txt

COPY app/ /srv/app/
COPY tests/ /srv/tests/
COPY scripts/ /srv/scripts/

EXPOSE 8080

# Default production entrypoint: the API server. The one-shot acceptance
# service is selected with a different command in docker-compose (`verify`).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
