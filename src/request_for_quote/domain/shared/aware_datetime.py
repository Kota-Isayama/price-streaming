import dataclasses
import datetime
from typing import Self


@dataclasses.dataclass(frozen=True)
class AwareDateTime:
    value: datetime.datetime

    def __post_init__(self) -> None:
        if self.value.tzinfo is None:
            raise ValueError("AwareDateTime must have timezone.")

    @classmethod
    def now(cls) -> Self:
        return cls(datetime.datetime.now(tz=datetime.timezone.utc))
    