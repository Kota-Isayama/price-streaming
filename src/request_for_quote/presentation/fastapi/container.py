# presentation/fastapi/container.py

from dataclasses import dataclass

from request_for_quote.infrastructure.postgres.base import (
    AsyncSessionMaker,
)


@dataclass(frozen=True)
class ApiContainer:
    session_maker: AsyncSessionMaker
    