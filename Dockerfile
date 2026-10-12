FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements.txt

FROM base AS dev
RUN pip install -r requirements-dev.txt
COPY . .
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS prod
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./alembic.ini
RUN useradd --create-home --shell /bin/bash chronos && chown -R chronos:chronos /srv
USER chronos
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
