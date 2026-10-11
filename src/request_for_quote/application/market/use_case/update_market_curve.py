from request_for_quote.application.market.port.mrket_curve_store import IMarketCurveStore
from request_for_quote.domain.market.market_curve import MarketCurve


class UpdateMarketCurveUseCase:
    def __init__(
        self,
        curve_store: IMarketCurveStore,
    ) -> None:
        self._curve_store = curve_store

    def execute(self, curve: MarketCurve) -> None:
        self._curve_store.put(curve=curve)
        