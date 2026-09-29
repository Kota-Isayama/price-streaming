import asyncio
from tkinter import NO
from typing import AsyncIterator
import uuid

from rstream import EventContext, OffsetNotFound, OffsetSpecification, OffsetType

from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.model.pricing_shard_ownership_change import PricingShardAcquired, PricingShardOwnershipChange, PricingShardOwnershipTransitionId, PricingShardReleased

from request_for_quote.application.pricing.port.pricing_shared_ownership import IPricingShardOwnership


class RabbitmqPricingShardOwnership(IPricingShardOwnership):
    def __init__(
        self,
        super_stream: str,
        consumer_group: str,
    ) -> None:
        self._super_stream = super_stream
        self._consumer_group = consumer_group

        self._changes: asyncio.Queue[
            PricingShardOwnershipChange
        ] = asyncio.Queue()

    async def changes(self) -> AsyncIterator[PricingShardOwnershipChange]:
        while True:
            yield await self._changes.get()

    async def complete(self) -> None:
        # SAC側で待機しているrebalance callbackは
        # Application完了まで待たせない　← なぜ？
        # PcCではno-opで十分
        return None

    async def on_consumer_update(
        self,
        is_active: bool,
        context: EventContext,
    ) -> OffsetSpecification:
        shard_id = self._shard_id_from_stream(
            context.stream,
        )

        if is_active:
            await self._changes.put(
                PricingShardAcquired(
                    transition_id=self._new_transition_id(),
                    shard_id=shard_id,
                )
            )

            try:
                stored_offset = await context.consumer.query_offset(
                    context.stream,
                    subscriber_name=self._consumer_group,
                )

                return OffsetSpecification(
                    offset_type=OffsetType.OFFSET,
                    offset=stored_offset+1,
                )
            except OffsetNotFound:
                return OffsetSpecification(
                    offset_type=OffsetType.OFFSET,
                    offset=0,
                )
        
        await self._changes.put(
            PricingShardReleased(
                transition_id=self._new_transition_id(),
                shard_id=shard_id,
            )
        )

        return OffsetSpecification(
            offset_type=OffsetType.OFFSET,
            offset=0,
        )

    def _shard_id_from_stream(
        self,
        stream: str,
    ) -> PricingShardId:
        # 例
        # rfq-pricing-event-3 -> 3
        prefix = f"{self._super_stream}"

        if not stream.startswith(prefix):
            raise ValueError(
                f"Unexpected stream name: {stream}"
            )

        return PricingShardId(int(stream.removeprefix(prefix)))

    @staticmethod
    def _new_transition_id() -> PricingShardOwnershipTransitionId:
        return PricingShardOwnershipTransitionId(
            str(uuid.uuid4())
        )
