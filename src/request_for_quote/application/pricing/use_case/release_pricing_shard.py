from request_for_quote.application.port.pricing_session_registry import (
    IPricingSessionRegistry,
)
from request_for_quote.application.pricing.model.pricing_shard import (
    PricingShardId,
)
from request_for_quote.application.pricing.port.pricing_shard_resolver import (
    IPricingShardResolver,
)


class ReleasePricingShardUseCase:
    """
    shard ownershipを失ったときに、

        InMemory Registry
            ↓
        このshardのSessionだけremove

    する。

    現在はI/Oが無いのでsync method。
    """

    def __init__(
        self,
        *,
        session_registry: IPricingSessionRegistry,
        shard_resolver: IPricingShardResolver,
    ) -> None:
        self._session_registry = (
            session_registry
        )
        self._shard_resolver = shard_resolver

    def execute(
        self,
        shard_id: PricingShardId,
    ) -> None:
        # removeしながらiterateしないよう
        # snapshotを取る。
        sessions = list(
            self._session_registry.list_all()
        )

        for session in sessions:
            request_id = str(
                session.request.request_id
            )

            session_shard = (
                self._shard_resolver.resolve(
                    request_id
                )
            )

            if session_shard != shard_id:
                continue

            self._session_registry.remove(
                request_id
            )

            print(
                "[SESSION RELEASED] "
                f"request_id={request_id} "
                f"shard={shard_id.value}"
            )
            