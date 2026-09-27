import abc
from collections.abc import AsyncIterator

from request_for_quote.application.pricing.model.pricing_shard_ownership_change import (
    PricingShardOwnershipChange,
)


class IPricingShardOwnership(abc.ABC):
    """
    「どのshardをこのWorkerが所有しているか」をApplicationへ知らせるPort。

    Kafka実装:
        Consumer Group rebalance

    Super Stream:
        stream partition ownership

    Quorum Queue版:
        Postgres Lease Coordinator
    """

    @abc.abstractmethod
    def changes(
        self,
    ) -> AsyncIterator[PricingShardOwnershipChange]:
        raise NotImplementedError

    @abc.abstractmethod
    async def complete(
        self,
        change: PricingShardOwnershipChange,
    ) -> None:
        """
        Application側のrestore/releaseが完了したことを通知する。

        Kafkaではこれが特に重要。

        on_partitions_revoked()
            ↓
        ReleasedをApplicationへ通知
            ↓
        Session release
            ↓
        complete()
            ↓
        Kafka callbackをreturnできる

        という同期に使う。
        """

        raise NotImplementedError