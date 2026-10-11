import abc

from request_for_quote.domain.market import (
    MarketCurve,
)


class IMarketCurveStore(abc.ABC):
    @abc.abstractmethod
    def put(
        self,
        curve: MarketCurve,
    ) -> None:
        raise NotImplementedError
    