from typing import Callable

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition

from request_for_quote.application.port.pricing_session_registry import (
    IPricingSessionRegistry,
)
from request_for_quote.application.pricing.port.pricing_session_store import (
    IPricingSessionStore,
)
from request_for_quote.application.pricing.port.pricing_session_unit_of_work import IPricingSessionUnitOfWork

from .pricing_partition_router import (
    KafkaPricingPartitionRouter,
)


class PricingRebalanceListener(
    ConsumerRebalanceListener
):
    def __init__(
        self,
        *,
        topic: str,
        all_partitions: set[int],
        session_uow_factory: Callable[[], IPricingSessionUnitOfWork],
        session_registry: IPricingSessionRegistry,
        partition_router: KafkaPricingPartitionRouter,
    ) -> None:
        self._topic = topic
        self._all_partitions = all_partitions
        self._session_uow_factory = session_uow_factory
        self._session_registry = session_registry
        self._partition_router = partition_router

    async def on_partitions_revoked(
        self,
        revoked: set[TopicPartition],
    ) -> None:
        revoked_partition_ids = {
            tp.partition
            for tp in revoked
            if tp.topic == self._topic
        }

        for session in self._session_registry.list_all():
            request_id = str(
                session.request.request_id
            )

            partition = (
                self._partition_router.partition_for(
                    request_id=request_id,
                    partitions=self._all_partitions,
                )
            )

            if partition not in revoked_partition_ids:
                continue

            self._session_registry.remove(
                request_id
            )

            print(
                f"[SESSION RELEASED] "
                f"request_id={request_id} "
                f"partition={partition}"
            )

    async def on_partitions_assigned(
        self,
        assigned: set[TopicPartition],
    ) -> None:
        assigned_partition_ids = {
            tp.partition
            for tp in assigned
            if tp.topic == self._topic
        }

        async with self._session_uow_factory() as uow:
            sessions = await uow.get_pricing_session_store().list_active()

        for session in sessions:
            request_id = str(
                session.request.request_id
            )

            partition = (
                self._partition_router.partition_for(
                    request_id=request_id,
                    partitions=self._all_partitions,
                )
            )

            if partition not in assigned_partition_ids:
                continue

            self._session_registry.save(session)

            print(
                f"[SESSION RESTORED] "
                f"request_id={request_id} "
                f"partition={partition}"
            )