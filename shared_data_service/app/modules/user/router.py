"""User REST endpoints — thin translation only: schema in, service call,
schema out. No business logic, no repository access."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Query, Response, status

from app.modules.user.business import UserChanges, UserData, UserService
from app.modules.user.schemas import UserCreate, UserOut, UserPageOut, UserUpdate


def build_user_router(service: UserService) -> APIRouter:
    router = APIRouter(prefix="/users", tags=["users"])

    @router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
    async def create_user(payload: UserCreate, response: Response) -> UserOut:
        user, created = await service.create_user(
            UserData(
                id=payload.id or uuid.uuid4(),
                name=payload.name,
                email=str(payload.email),
                attributes=payload.attributes,
            )
        )
        if not created:
            response.status_code = status.HTTP_200_OK
        return UserOut.model_validate(user)

    @router.get("/{user_id}", response_model=UserOut)
    async def get_user(user_id: uuid.UUID) -> UserOut:
        return UserOut.model_validate(await service.get_user(user_id))

    @router.patch("/{user_id}", response_model=UserOut)
    async def update_user(user_id: uuid.UUID, payload: UserUpdate) -> UserOut:
        user = await service.update_user(
            user_id,
            UserChanges(
                name=payload.name,
                email=str(payload.email) if payload.email is not None else None,
                attributes=payload.attributes,
            ),
            expected_version=payload.expected_version,
        )
        return UserOut.model_validate(user)

    @router.get("", response_model=UserPageOut)
    async def list_users(
        limit: int = Query(default=50),
        offset: int = Query(default=0),
        sort: str | None = Query(default=None, description="field or -field"),
        name: str | None = Query(default=None),
        email: str | None = Query(default=None),
    ) -> UserPageOut:
        page = await service.list_users(
            limit=limit, offset=offset, sort=sort, name=name, email=email
        )
        return UserPageOut(
            items=[UserOut.model_validate(u) for u in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )

    return router
