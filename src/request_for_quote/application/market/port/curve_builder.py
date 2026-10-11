import abc

from request_for_quote.domain.market import (
    CurveId,
    MarketCurve,
    SwapQuoteSnapshot,
)
from request_for_quote.domain.product.interest_rate.convention import SwapConvention


class ICurveBuilder(abc.ABC):
    @abc.abstractmethod
    def build(
        self,
        *,
        curve_id: CurveId,
        snapshot: SwapQuoteSnapshot,
        convention: SwapConvention,
    ) -> MarketCurve:
        raise NotImplementedError
    