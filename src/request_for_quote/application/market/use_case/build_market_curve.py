from request_for_quote.application.market.port.curve_builder import ICurveBuilder
from request_for_quote.application.market.port.mrket_curve_store import IMarketCurveStore
from request_for_quote.domain.market.identifier import CurveId
from request_for_quote.domain.market.market_curve import MarketCurve
from request_for_quote.domain.market.swap_quote import SwapQuoteSnapshot
from request_for_quote.domain.product.interest_rate.convention import SwapConvention


class BuildMarketCurveUseCase:
    def __init__(
        self,
        curve_builder: ICurveBuilder,
        curve_store: IMarketCurveStore,
    ) -> None:
        self._curve_builder = curve_builder
        self._curve_store = curve_store

    def execute(
        self,
        *,
        curve_id: CurveId,
        snapshot: SwapQuoteSnapshot,
        convention: SwapConvention,
    ) -> MarketCurve:
        curve = self._curve_builder.build(
            curve_id=curve_id,
            snapshot=snapshot,
            convention=convention,
        )

        self._curve_store.put(curve)

        return curve
    