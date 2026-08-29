FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.7 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app

# Dependency layer: cached until pyproject.toml/uv.lock change.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev --group webserver

COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --group webserver

ENV PATH="/app/.venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/home

RUN mkdir -p "$DAGSTER_HOME"
COPY docker/dagster/dagster.yaml docker/dagster/workspace.yaml /opt/dagster/home/
