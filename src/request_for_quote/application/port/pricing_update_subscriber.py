import abc
import dataclasses
from decimal import Decimal
from typing import AsyncIterator


@dataclasses.dataclass(frozen=True)
class PricingUpdate:
    request_id: str
    par_rate: Decimal


class IPricingUpdateSubscriber(abc.ABC):
    @abc.abstractmethod
    async def set_request_ids(
        self,
        request_ids: set[str],
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def updates(
        self,
    ) -> AsyncIterator[PricingUpdate]:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError
    