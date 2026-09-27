from typing import Callable

from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.port.pricing_session_unit_of_work import IPricingSessionUnitOfWork
from request_for_quote.application.pricing.port.pricing_shard_resolver import IPricingShardResolver


class RestorePricingShardUseCase:
    """
    Shradのownerになった時に、
        DB snapshot
        ↓
        このshardに属するSessionだけ抽出
        ↓
        InMemory Registryへrestore
    するApplication UseCase。

    Kafkaの存在は全く知らない。    
    """
    def __init__(
        self,
        *,
        session_uow_factory: Callable[
            [],
            IPricingSessionUnitOfWork,
        ],
        session_registry: IPricingSessionRegistry,
        shard_resolver: IPricingShardResolver,
    ) -> None:
        self._session_uow_factory = session_uow_factory
        self._session_registry = session_registry
        self._shard_resolver = shard_resolver

    async def execute(
        self,
        shard_id: PricingShardId,
    ) -> None:
        # DB I/Oなのでasync
        async with self._session_uow_factory() as uow:
            sessions = await uow.get_pricing_session_store().list_active()

        for session in sessions:
            request_id = session.request.request_id
            session_shard = self._shard_resolver.resolve(request_id)

            if session_shard != shard_id:
                continue

            self._session_registry.save(session)

            print(
                "[SESSION RESTORED] "
                f"request_id={request_id} "
                f"shard={shard_id.value}"
            )
            