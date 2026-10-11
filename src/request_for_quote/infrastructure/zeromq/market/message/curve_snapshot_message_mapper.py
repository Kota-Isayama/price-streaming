from request_for_quote.domain.market import (
    CurveId,
    CurveInterpolation,
    CurveNode,
    MarketCurve,
)
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.infrastructure.zeromq.market.message.curve_snapshot_message import (
    CurveSnapshotMessage,
)


class CurveSnapshotMapper:
    @staticmethod
    def to_domain(
        message: CurveSnapshotMessage,
    ) -> MarketCurve:
        return MarketCurve(
            curve_id=CurveId(
                message.curve_id
            ),
            reference_date=(
                message.reference_date
            ),
            as_of=AwareDateTime(message.as_of),
            nodes=tuple(
                CurveNode(
                    pillar_date=(
                        node.pillar_date
                    ),
                    discount_factor=(
                        node.discount_factor
                    ),
                )
                for node
                in message.nodes
            ),
            interpolation=CurveInterpolation(
                message.interpolation
            ),
        )
    