import asyncio
from typing import AsyncIterator, Sequence
import uuid

from aiokafka import ConsumerRebalanceListener, TopicPartition

from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.model.pricing_shard_ownership_change import PricingShardAcquired, PricingShardOwnershipChange, PricingShardReleased
from request_for_quote.application.pricing.model.pricing_shard_ownership_change import PricingShardOwnershipTransitionId
from request_for_quote.application.pricing.port.pricing_shared_ownership import IPricingShardOwnership


class KafkaPricingShardOwnership(
    ConsumerRebalanceListener,
    IPricingShardOwnership,
):
    """
    Kafka RebalanceとApplication Runtimeの橋渡し。
    
    
    ┌──────────────────────────────────────────┐
    │ Kafka                                    │
    │                                          │
    │ on_partitions_revoked(partition=2)       │
    │       │                                  │
    │       ▼                                  │
    │ PricingShardReleased(shard=2)            │
    │       │                                  │
    │       │ asyncio.Queue                    │
    └───────┼──────────────────────────────────┘
            ▼
    Application Runtime
            │
            ├─ shard consumer停止
            ├─ Session release
            │
            ▼
       complete(change)
            │
            │ Future.set_result()
            ▼
    Kafka callback再開
            │
            ▼
    on_partitions_revoked() return

    
    asyncio.Queue:
        Kafka callback → Runtime へChangeを渡す。

    asyncio.Future:
        Runtime → Kafka callback へ
        「release/restore終わったよ」と返す。
    """
    def __init__(self, *, topic: str) -> None:
        self._topic = topic

        # Kafka callbackからApplication RunTimeへ
        # Ownership Changeを渡すためのmailbox
        self._changes: asyncio.Queue[PricingShardOwnershipChange] = asyncio.Queue()

        # Application側の処理完了をもつFuture
        #
        # transition_id
        # ↓
        # Future
        #
        # callback側はawait Futureする
        self._completions: dict[
            PricingShardOwnershipTransitionId,
            asyncio.Future[None],
        ] = {}

    async def changes(
        self,
    ) -> AsyncIterator[PricingShardOwnershipChange]:
        """
        Application Runtimeはこちらを読む。
        
        Queueが空ならawaitで待つ。
        その間、他のTaskは普通に動ける。
        """
        while True:
            change = await self._changes.get()
            yield change

    async def complete(
        self,
        change: PricingShardOwnershipChange,
    ) -> None:
        """
        Runtime側から、
            restore / release完了
        をKafka callbackに渡す。
        """
        future = self._completions.get(
            change.transition_id,
        )

        if future is None:
            raise RuntimeError(
                "Unknown ownership transitions: "
                f"{change.transition_id}"
            )

        if not future.done():
            future.set_result(None)

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        """
        Kafkaが
            「あなたはこれらのpartitionを失います」
        と通知してきた時に呼ばれる。
        ここではもうSession Registryを直接触らない。
        """

        changes = [
            PricingShardReleased(
                transition_id=self._new_transition_id(),
                shard_id=PricingShardId(tp.partition),
            )
            for tp in revoked
            if tp.topic == self._topic
        ]

        # Application側のreleaseが完了するまで
        # このcallbackはreturnされない。
        await self._publish_and_wait(changes)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        """
        Kafkaが
            「これらのpartitionのownerになりました」
        と通知してきた時に呼ばれる。
        """
        changes = [
            PricingShardAcquired(
                transition_id=self._new_transition_id(),
                shard_id=PricingShardId(tp.partition),
            )
            for tp in assigned
            if tp.topic == self._topic
        ]

        # Application側のacquireが完了するまで
        # このcallbackはreturnされない。
        await self._publish_and_wait(changes)

    async def _publish_and_wait(self, changes: Sequence[PricingShardOwnershipChange]) -> None:
        """
        ここがFutureを使う核心。
        
        例:
            Kafka callback
                ↓
            Released shard-2
                ↓ Queue
            Runtime
                ↓
            release
                ↓
            complete()
                ↓
            Future完了
                ↓
            callback return
        """
        if not changes:
            return

        loop = asyncio.get_running_loop()

        futures: list[asyncio.Future[None]] = []

        for change in changes:
            future = loop.create_future()

            self._completions[
                change.transition_id
            ] = future

            futures.append(future)

            # Runtimeへ通知
            await self._changes.put(change)

        try:
            # 全Shardのrestore/releaseが終わるまで待つ。
            await asyncio.gather(*futures)
        finally:
            for change in changes:
                self._completions.pop(
                    change.transition_id,
                    None,
                )

    def cancel_pending(self) -> None:
        """
        Process shutdown用。
        
        callbackがFuture待ち中だった場合に解除する。
        """
        for future in self._completions.values():
            if not future.done():
                future.cancel()

    @staticmethod
    def _new_transition_id(
    ) -> PricingShardOwnershipTransitionId:
        return PricingShardOwnershipTransitionId(
            value=str(uuid.uuid4())
        )
    