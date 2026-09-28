"""後でちゃんと理解する!!"""

import asyncio
import uuid
from collections.abc import (
    AsyncIterator,
    Callable,
)
from datetime import (
    datetime,
    timedelta,
    timezone,
)

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)

from request_for_quote.application.pricing.model.pricing_shard import (
    PricingShardId,
)
from request_for_quote.application.pricing.model.pricing_shard_ownership_change import (
    PricingShardAcquired,
    PricingShardOwnershipChange,
    PricingShardOwnershipTransitionId,
    PricingShardReleased,
)
from request_for_quote.application.pricing.port.pricing_shared_ownership import (
    IPricingShardOwnership,
)

from request_for_quote.infrastructure.postgres.models import (
    PricingShardLeaseOrm,
)


class PostgresPricingShardOwnership(
    IPricingShardOwnership,
):
    """
    RabbitMQ Quorum版のownership source。

    Kafka:
        rebalance callback
            ↓
        PricingShardAcquired/Released

    RabbitMQ Quorum:
        Postgres Lease
            ↓
        PricingShardAcquired/Released

    Application Runtimeから見ると同じ。
    """

    def __init__(
        self,
        *,
        session_maker: async_sessionmaker[
            AsyncSession
        ],
        worker_id: str,
        shard_count: int,
        lease_seconds: int,
        renew_interval_seconds: float,
        max_shards_per_worker: int,
    ) -> None:
        self._session_maker = (
            session_maker
        )
        self._worker_id = worker_id
        self._shard_count = shard_count

        self._lease_duration = timedelta(
            seconds=lease_seconds
        )

        self._renew_interval = (
            renew_interval_seconds
        )

        self._max_shards = (
            max_shards_per_worker
        )

        self._owned: set[
            PricingShardId
        ] = set()

    async def changes(
        self,
    ) -> AsyncIterator[
        PricingShardOwnershipChange
    ]:
        """
        定期的に、

          1. 自分が持っているLeaseをrenew
          2. 失ったLeaseをReleasedとして通知
          3. 上限まで空いているShardをclaim
          4. 新しく取れたShardをAcquiredとして通知

        する。
        """

        while True:
            lost = await self._renew_owned()

            for shard_id in lost:
                self._owned.discard(
                    shard_id
                )

                yield PricingShardReleased(
                    transition_id=(
                        self._new_transition_id()
                    ),
                    shard_id=shard_id,
                )

            capacity = (
                self._max_shards
                - len(self._owned)
            )

            if capacity > 0:
                acquired = (
                    await self._claim_available(
                        limit=capacity
                    )
                )

                for shard_id in acquired:
                    self._owned.add(
                        shard_id
                    )

                    yield PricingShardAcquired(
                        transition_id=(
                            self._new_transition_id()
                        ),
                        shard_id=shard_id,
                    )

            await asyncio.sleep(
                self._renew_interval
            )

    async def complete(
        self,
        change: PricingShardOwnershipChange,
    ) -> None:
        """
        Kafka版ではrebalance callbackを解放するため重要。

        Postgres lease版には待っているbroker callbackがないため、
        現時点ではno-op。

        ただしPortは共通のまま。
        """
        return None

    async def _renew_owned(
        self,
    ) -> set[PricingShardId]:
        if not self._owned:
            return set()

        now = datetime.now(
            timezone.utc
        )

        new_expiry = (
            now + self._lease_duration
        )

        successfully_renewed: set[
            PricingShardId
        ] = set()

        async with self._session_maker() as session:
            async with session.begin():

                for shard_id in self._owned:
                    row = await session.get(
                        PricingShardLeaseOrm,
                        shard_id.value,
                        with_for_update=True,
                    )

                    if row is None:
                        continue

                    if row.owner_id != self._worker_id:
                        continue

                    # 自分がownerなら延長。
                    row.lease_until = new_expiry

                    successfully_renewed.add(
                        shard_id
                    )

        return (
            self._owned
            - successfully_renewed
        )

    async def _claim_available(
        self,
        *,
        limit: int,
    ) -> list[PricingShardId]:
        if limit <= 0:
            return []

        now = datetime.now(
            timezone.utc
        )

        new_expiry = (
            now + self._lease_duration
        )

        acquired: list[
            PricingShardId
        ] = []

        async with self._session_maker() as session:
            async with session.begin():

                # SKIP LOCKED:
                # 複数Workerが同時にclaimしても、
                # 同じrowを取り合いにくくする。
                result = await session.execute(
                    select(
                        PricingShardLeaseOrm
                    )
                    .where(
                        (
                            PricingShardLeaseOrm
                            .owner_id
                            .is_(None)
                        )
                        |
                        (
                            PricingShardLeaseOrm
                            .lease_until
                            < now
                        )
                    )
                    .order_by(
                        PricingShardLeaseOrm
                        .shard_id
                    )
                    .with_for_update(
                        skip_locked=True
                    )
                    .limit(limit)
                )

                rows = result.scalars().all()

                for row in rows:
                    row.owner_id = (
                        self._worker_id
                    )
                    row.lease_until = (
                        new_expiry
                    )

                    # 新ownerになるたび増加。
                    row.epoch += 1

                    acquired.append(
                        PricingShardId(
                            row.shard_id
                        )
                    )

        return acquired

    @staticmethod
    def _new_transition_id(
    ) -> PricingShardOwnershipTransitionId:
        return (
            PricingShardOwnershipTransitionId(
                value=str(uuid.uuid4())
            )
        )