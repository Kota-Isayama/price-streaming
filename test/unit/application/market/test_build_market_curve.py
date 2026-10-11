from datetime import (
    date,
    datetime,
    timezone,
)
from decimal import Decimal

from request_for_quote.application.market.port.curve_builder import (
    ICurveBuilder,
)
from request_for_quote.application.market.use_case.build_market_curve import (
    BuildMarketCurveUseCase,
)
from request_for_quote.domain.market import (
    CurveId,
    CurveNode,
    MarketCurve,
    QuoteSetId,
    SwapQuote,
    SwapQuoteSnapshot,
    Tenor,
    TenorUnit,
)
from request_for_quote.domain.market.market_curve import CurveInterpolation
from request_for_quote.domain.product.interest_rate import (
    BusinessCalendarId,
    BusinessDayConvention,
    DayCountFraction,
    FloatingRateIndex,
    PeriodFrequency,
    SwapConvention,
)
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.application.market.adapter.market_curve_store.in_memory.market_curve_store import (
    InMemoryMarketCurveStore,
)


class FakeCurveBuilder(
    ICurveBuilder
):
    def build(
        self,
        *,
        curve_id: CurveId,
        snapshot: SwapQuoteSnapshot,
        convention: SwapConvention,
    ) -> MarketCurve:
        return MarketCurve(
            curve_id=curve_id,
            reference_date=date(
                2026,
                10,
                11,
            ),
            as_of=snapshot.as_of,
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


def test_build_market_curve() -> None:
    store = InMemoryMarketCurveStore()

    use_case = BuildMarketCurveUseCase(
        curve_builder=FakeCurveBuilder(),
        curve_store=store,
    )

    curve_id = CurveId(
        "JPY-OIS"
    )

    snapshot = SwapQuoteSnapshot(
        quote_set_id=QuoteSetId(
            "JPY-OIS-SWAP-QUOTES"
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
        quotes=(
            SwapQuote(
                tenor=Tenor(
                    1,
                    TenorUnit.YEAR,
                ),
                par_rate=Decimal(
                    "0.007"
                ),
            ),
            SwapQuote(
                tenor=Tenor(
                    5,
                    TenorUnit.YEAR,
                ),
                par_rate=Decimal(
                    "0.010"
                ),
            ),
        ),
    )

    convention = SwapConvention(
        floating_rate_index=FloatingRateIndex(
            "JPY-TONA"
        ),
        fixed_leg_frequency=PeriodFrequency.ANNUAL,
        fixed_leg_day_count=(
            DayCountFraction.ACT_365_FIXED
        ),
        business_calendar_id=BusinessCalendarId(
            "JP"
        ),
        business_day_convention=(
            BusinessDayConvention.MODIFIED_FOLLOWING
        ),
        settlement_days=2,
    )

    curve = use_case.execute(
        curve_id=curve_id,
        snapshot=snapshot,
        convention=convention,
    )

    assert store.get(
        curve_id
    ) == curve
