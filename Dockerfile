FROM python:3.12-slim

# ca-certificates for HTTPS to S3 / STAC. git is intentionally NOT installed:
# code is baked into the image (COPY below), not cloned at pod startup.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency layer — re-resolves ONLY when requirements.txt changes, so code-only
# rebuilds reuse it (with GHA cache, those finish in seconds). The weekly cron
# builds with --no-cache so this layer actually re-resolves the unpinned deps.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Run as an unprivileged user (NRP incident, 2026-11-06). Nothing here needs
# root: the server listens on 8000 and writes only spill files under /tmp. The
# uid is numeric so Kubernetes can check runAsNonRoot.
RUN useradd --system --uid 10001 --user-group --create-home --home-dir /home/app app
ENV HOME=/home/app PYTHONDONTWRITEBYTECODE=1
USER 10001

# Pre-install DuckDB extensions so pods start offline-capable. Installed as the
# app user, so they land in its default extension directory
# (/home/app/.duckdb/extensions/) where the runtime finds them.
RUN python -c "import duckdb; c = duckdb.connect(); c.sql('INSTALL httpfs; INSTALL spatial; INSTALL h3 FROM community')"

# Application code last, so a code change rebuilds only this cheap layer.
COPY . /app

# Version stamp, passed by docker.yml (APP_VERSION = the git tag on release builds,
# else "main"; GIT_SHA = the commit). Last so a version/sha change rebuilds only this
# trivial layer, never the deps. Read at runtime by server.py (issue #221).
ARG APP_VERSION=dev
ARG GIT_SHA=unknown
ENV APP_VERSION=$APP_VERSION GIT_SHA=$GIT_SHA

CMD ["python", "server.py"]
