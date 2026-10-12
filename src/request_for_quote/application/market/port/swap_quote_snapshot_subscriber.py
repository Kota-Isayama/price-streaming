import abc
from typing import AsyncIterator

from request_for_quote.domain.market.swap_quote import SwapQuoteSnapshot


class ISwapQuoteSnapshotSubscriber(abc.ABC):
    @abc.abstractmethod
    def subscribe(
        self,
    ) -> AsyncIterator[SwapQuoteSnapshot]:
        raise NotImplementedError
    