# syntax=docker/dockerfile:1
# Build context is the REPO ROOT (see docker-compose.yml: context: .. ,
# dockerfile: shared_data_service/Dockerfile) so the uv path dependency
# ../rabbit-client-python resolves during `uv sync --locked`.
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/
# UV_PROJECT_ENVIRONMENT: build the venv at its final /app/.venv path so
# script shebangs (alembic, uvicorn, ...) stay valid in the runtime stage.
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/app/.venv
# Keep the repo-relative layout: the lib at /src/rabbit-client-python, the
# service at /src/shared_data_service, so ../rabbit-client-python resolves.
COPY rabbit-client-python /src/rabbit-client-python
WORKDIR /src/shared_data_service
COPY shared_data_service/pyproject.toml shared_data_service/uv.lock ./
# --no-editable: install simple-rabbit as a regular site-packages package so
# the venv does not point back at the builder-only /src tree.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev --no-editable

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
