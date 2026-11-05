# Single image for both services (M0-T07): the service is selected by the
# compose `command:` (python -m app.main | python -m mock.main).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv/app

COPY pyproject.toml ./
COPY app/ ./app/
COPY mock/ ./mock/

# Install the distribution, create a non-root runtime user, and pre-create the
# data directory (compose mounts a named volume here; first use inherits the
# appuser ownership from this layer).
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 appuser \
    && mkdir -p /srv/app/data \
    && chown -R appuser:appuser /srv/app

USER appuser
