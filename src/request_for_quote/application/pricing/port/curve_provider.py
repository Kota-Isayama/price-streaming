import abc

from request_for_quote.domain.market import (
    CurveId,
    MarketCurve,
)


class CurveNotFoundError(
    LookupError
):
    pass


class ICurveProvider(abc.ABC):
    @abc.abstractmethod
    def get(
        self,
        curve_id: CurveId,
    ) -> MarketCurve:
        raise NotImplementedError
    