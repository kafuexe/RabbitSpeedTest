"""Project REST endpoints — thin translation only: schema in, service call,
schema out. No business logic, no repository access."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Query, Response, status

from app.modules.project.business import (
    ProjectChanges,
    ProjectData,
    ProjectService,
)
from app.modules.project.schemas import (
    ProjectCreate,
    ProjectOut,
    ProjectPageOut,
    ProjectUpdate,
)


def build_project_router(service: ProjectService) -> APIRouter:
    router = APIRouter(prefix="/projects", tags=["projects"])

    @router.post("", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
    async def create_project(payload: ProjectCreate, response: Response) -> ProjectOut:
        project, created = await service.create(
            ProjectData(
                id=payload.id or uuid.uuid4(),
                name=payload.name,
                description=payload.description,
                owner_email=str(payload.owner_email),
                attributes=payload.attributes,
            )
        )
        if not created:
            response.status_code = status.HTTP_200_OK
        return ProjectOut.model_validate(project)

    @router.get("/{project_id}", response_model=ProjectOut)
    async def get_project(project_id: uuid.UUID) -> ProjectOut:
        return ProjectOut.model_validate(await service.get(project_id))

    @router.patch("/{project_id}", response_model=ProjectOut)
    async def update_project(
        project_id: uuid.UUID, payload: ProjectUpdate
    ) -> ProjectOut:
        project = await service.update(
            project_id,
            ProjectChanges(
                name=payload.name,
                description=payload.description,
                owner_email=(
                    str(payload.owner_email)
                    if payload.owner_email is not None
                    else None
                ),
                attributes=payload.attributes,
            ),
            expected_version=payload.expected_version,
        )
        return ProjectOut.model_validate(project)

    @router.get("", response_model=ProjectPageOut)
    async def list_projects(
        limit: int = Query(default=50),
        offset: int = Query(default=0),
        sort: str | None = Query(default=None, description="field or -field"),
        name: str | None = Query(default=None),
        owner_email: str | None = Query(default=None),
    ) -> ProjectPageOut:
        page = await service.list_page(
            limit=limit, offset=offset, sort=sort,
            filters={"name": name, "owner_email": owner_email},
        )
        return ProjectPageOut(
            items=[ProjectOut.model_validate(p) for p in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )

    return router
