from request_for_quote.application.market.port.mrket_curve_store import IMarketCurveStore
from request_for_quote.application.pricing.port.curve_provider import ICurveProvider, CurveNotFoundError
from request_for_quote.domain.market.identifier import CurveId
from request_for_quote.domain.market.market_curve import MarketCurve


class InMemoryMarketCurveStore(
    IMarketCurveStore,
    ICurveProvider,
):
    def __init__(self) -> None:
        self._curves: dict[CurveId, MarketCurve] = {}

    def put(self, curve: MarketCurve) -> None:
        self._curves[curve.curve_id] = curve

    def get(self, curve_id: CurveId) -> MarketCurve:
        try:
            return self._curves[
                curve_id
            ]
        except KeyError as exc:
            raise CurveNotFoundError(
                "Market curve not found: "
                f"{curve_id.to_str()}"
            ) from exc
        