from datetime import (
    date,
    datetime,
    timezone,
)
from decimal import Decimal

from request_for_quote.application.market.use_case.update_market_curve import (
    UpdateMarketCurveUseCase,
)
from request_for_quote.domain.market import (
    CurveId,
    CurveNode,
    MarketCurve,
)
from request_for_quote.domain.market.market_curve import CurveInterpolation
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.application.market.adapter.market_curve_store.in_memory.market_curve_store import (
    InMemoryMarketCurveStore,
)


def test_update_market_curve() -> None:
    store = InMemoryMarketCurveStore()

    use_case = UpdateMarketCurveUseCase(
        curve_store=store,
    )

    curve = MarketCurve(
        curve_id=CurveId(
            "JPY-OIS"
        ),
        reference_date=date(
            2026,
            10,
            11,
        ),
        as_of=AwareDateTime(
            datetime(
                2026,
                10,
                11,
                12,
                0,
                tzinfo=timezone.utc,
            ),
        ),
        nodes=(
            CurveNode(
                pillar_date=date(
                    2026,
                    10,
                    11,
                ),
                discount_factor=Decimal(
                    "1"
                ),
            ),
            CurveNode(
                pillar_date=date(
                    2031,
                    10,
                    11,
                ),
                discount_factor=Decimal(
                    "0.95"
                ),
            ),
        ),
        interpolation=CurveInterpolation.LOG_LINEAR_DISCOUNT,
    )

    use_case.execute(
        curve
    )

    assert (
        store.get(
            curve.curve_id
        )
        == curve
    )
