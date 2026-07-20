"""Per-module test data for the generic contract suite.

This mapping is the ONLY per-module code in tests/module_contract/ — one
entry per spec, enforced at collection time (see conftest.py). It lives in
tests so production code stays test-free.

Conventions the contract relies on:
- make_valid_data() and make_second_valid_data() return spec.data instances
  with the SAME id but different mutable content (drives replay-conflict,
  duplicate-delivery, and version-wins scenarios).
- make_valid_create() returns a POST body whose id matches the data ids.
- make_invalid_update_cases() are PATCH bodies that must 422.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable

import pytest
from pydantic import BaseModel

from app.modules import ALL_SPECS
from app.modules.project import ProjectData
from app.modules.user import UserData

# The one parametrization every contract file uses — declared once so the
# suites can never drift apart.
module_specs = pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s.name)


@dataclass(frozen=True)
class ModuleFixtures:
    path: str
    make_valid_data: Callable[[], BaseModel]
    make_second_valid_data: Callable[[], BaseModel]
    make_valid_create: Callable[[], dict[str, Any]]
    make_valid_update: Callable[[], dict[str, Any]]
    make_invalid_update_cases: Callable[[], list[dict[str, Any]]]


_USER_ID = uuid.UUID("00000000-0000-0000-0000-00000000c0de")
_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-00000000cafe")


def _user_data() -> UserData:
    return UserData(id=_USER_ID, name="Ada Lovelace", email="ada@example.com",
                    attributes={"role": "engineer"}, version=1)


def _user_data_2() -> UserData:
    return UserData(id=_USER_ID, name="Grace Hopper", email="grace@example.com",
                    attributes={"role": "admiral"}, version=1)


def _project_data() -> ProjectData:
    return ProjectData(id=_PROJECT_ID, name="Apollo",
                       description="Guidance computer rewrite",
                       owner_email="margaret@example.com",
                       attributes={"tier": "gold"}, version=1)


def _project_data_2() -> ProjectData:
    return ProjectData(id=_PROJECT_ID, name="Gemini",
                       description="Rendezvous program",
                       owner_email="jim@example.com",
                       attributes={"tier": "silver"}, version=1)


FIXTURES: dict[str, ModuleFixtures] = {
    "user": ModuleFixtures(
        path="/users",  # top-level unscoped route (scoped is /{project_id}/user)
        make_valid_data=_user_data,
        make_second_valid_data=_user_data_2,
        # Derived from the data builder so the POST body cannot drift from
        # what "same content on replay" means. project_id is set by the
        # scoped route, never the body, so it is excluded here.
        make_valid_create=lambda: _user_data().model_dump(
            mode="json", exclude={"version", "project_id"}
        ),
        make_valid_update=lambda: {"name": "Ada K."},
        make_invalid_update_cases=lambda: [
            {"name": "   "},              # blank-after-strip
            {"email": "ops@backend"},     # API email stays strict
        ],
    ),
    "project": ModuleFixtures(
        path="/project",  # singular (Amendment 2 CHANGE 1)
        make_valid_data=_project_data,
        make_second_valid_data=_project_data_2,
        make_valid_create=lambda: _project_data().model_dump(
            mode="json", exclude={"version"}
        ),
        make_valid_update=lambda: {"description": "Now with lunar module"},
        make_invalid_update_cases=lambda: [
            {"name": "   "},              # blank-after-strip
            {"description": "x" * 2001},  # over the 2000-char rule
        ],
    ),
}
