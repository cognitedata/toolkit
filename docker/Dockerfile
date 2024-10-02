FROM python:3.11-slim AS builder

ADD src /app
WORKDIR /

RUN pip3 install poetry
RUN poetry config virtualenvs.create false

COPY poetry.lock .
COPY pyproject.toml .

# By default poetry does NOT export dev dependencies here
RUN poetry export -f requirements.txt --output /requirements.txt --without-hashes

# We are installing a dependency here directly into our app source dir
RUN pip3 install --target=/app -r /requirements.txt --upgrade

FROM python:3.11-bookworm
COPY --from=builder /app /app
ENV PYTHONPATH /app
CMD ["cdf", "--help"]
