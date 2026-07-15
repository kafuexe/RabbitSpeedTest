"""Domain exceptions shared by all modules. The API edge maps them to HTTP
statuses; the consumer edge treats them as permanent (log + ack)."""
from __future__ import annotations


class DomainError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class NotFoundError(DomainError):
    """Entity does not exist (HTTP 404)."""


class ConflictError(DomainError):
    """State conflict: version mismatch or contradictory replay (HTTP 409)."""


class InvalidInputError(DomainError):
    """Business validation failed (HTTP 400)."""


class InvalidQueryError(InvalidInputError):
    """Bad filter/sort/pagination parameters (HTTP 400)."""
