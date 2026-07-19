# syntax=docker/dockerfile:1
# Build context is the REPO ROOT (see docker-compose.yml: context: .. ,
# dockerfile: shared_data_service/Dockerfile) so the uv path dependency
# ../rabbit-client-python resolves during `uv sync --locked`.
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
# The service lives at /app, the library at /rabbit-client-python, so the
# ../rabbit-client-python path source resolves and the venv lands at its
# final /app/.venv path (script shebangs stay valid in the runtime stage).
WORKDIR /app
# Layer 1 — third-party dependencies only. Copy just the metadata both
# resolutions need (the library's pyproject + README feed hatchling's
# metadata build), so neither library nor service code edits bust this
# slow layer.
COPY rabbit-client-python/pyproject.toml rabbit-client-python/README.md /rabbit-client-python/
COPY shared_data_service/pyproject.toml shared_data_service/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev --no-install-package rabbit-client
# Layer 2 — the client library itself. --no-editable: install rabbit-client
# as a regular site-packages package so the venv does not point back at the
# builder-only /rabbit-client-python tree.
COPY rabbit-client-python /rabbit-client-python
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM python:3.12-slim
RUN groupadd -r sds && useradd -r -g sds -d /app sds
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY shared_data_service/app ./app
COPY shared_data_service/alembic ./alembic
COPY shared_data_service/alembic.ini shared_data_service/main.py ./
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER sds
EXPOSE 8080
CMD ["python", "main.py"]
