import abc
import dataclasses
from typing import AsyncIterator

from request_for_quote.domain.market.market import MarketDataId, MarketDataValue


@dataclasses.dataclass(frozen=True)
class MarketDataUpdate:
    market_data_id: MarketDataId
    value: MarketDataValue


class IMarketDataSubscriber(abc.ABC):
    @abc.abstractmethod
    def subscribe(self) -> AsyncIterator[MarketDataUpdate]:
        raise NotImplementedError
    