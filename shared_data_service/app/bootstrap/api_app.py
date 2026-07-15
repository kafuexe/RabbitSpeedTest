"""Uvicorn entrypoint factory (import string for multi-worker runs)."""
from __future__ import annotations

from fastapi import FastAPI

from app.api.app import create_app
from app.bootstrap.container import Container
from app.config.settings import Settings


def create_app_from_env() -> FastAPI:
    return create_app(Container(Settings()))
