import dataclasses
import datetime
from typing import Self

from request_for_quote.domain.shared.error import DomainError


@dataclasses.dataclass(frozen=True)
class AwareDateTime:
    value: datetime.datetime

    def __post_init__(self) -> None:
        if self.value.tzinfo is None:
            raise InvalidAwareDateTime.have_no_timezone()

    @classmethod
    def now(cls) -> Self:
        return cls(datetime.datetime.now(tz=datetime.timezone.utc))


class InvalidAwareDateTime(DomainError):
    @classmethod
    def have_no_timezone(cls) -> Self:
        msg = f"AwareDateTime must have timezone."
        return cls(msg)
    