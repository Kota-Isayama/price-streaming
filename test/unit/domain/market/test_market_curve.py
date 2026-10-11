from datetime import (
    date,
    datetime,
    timezone,
)
from decimal import Decimal

from request_for_quote.domain.market import (
    CurveId,
    CurveNode,
    MarketCurve,
    CurveInterpolation,
)
from request_for_quote.domain.shared.aware_datetime import AwareDateTime


def test_create_market_curve() -> None:
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
                    2027,
                    10,
                    11,
                ),
                discount_factor=Decimal(
                    "0.993"
                ),
            ),
            CurveNode(
                pillar_date=date(
                    2031,
                    10,
                    11,
                ),
                discount_factor=Decimal(
                    "0.951"
                ),
            ),
        ),
        interpolation=CurveInterpolation.LOG_LINEAR_DISCOUNT,
    )

    assert curve.curve_id == CurveId(
        "JPY-OIS"
    )

    assert (
        curve.nodes[0].discount_factor
        == Decimal("1")
    )