FROM python:3.12-slim AS builder
# Python
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  # Pip
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  # Poetry's configuration:
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_CREATE=false \
  POETRY_CACHE_DIR='/var/cache/pypoetry' \
  POETRY_HOME='/usr/local' \
  POETRY_VERSION=1.8.3
RUN apt-get update && apt-get install -y curl
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="$POETRY_HOME/bin:$VIRTUAL_ENV/bin:$PATH"

COPY poetry.lock pyproject.toml README.md ./
COPY cognite_toolkit/. ./cognite_toolkit/
# Ensure we get the exact version of the dependencies
RUN poetry install --without dev --sync

# Building final image
FROM python:3.12-slim-bookworm
COPY --from=builder /usr/local/lib/python3.11/site-packages  /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /cognite_toolkit /cognite_toolkit

RUN mkdir /app
WORKDIR /app


CMD ["cdf", "--help"]
