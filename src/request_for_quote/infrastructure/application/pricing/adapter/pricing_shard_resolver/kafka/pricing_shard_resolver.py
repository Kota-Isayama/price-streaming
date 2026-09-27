from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.port.pricing_shard_resolver import IPricingShardResolver
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.pricing_partition_router import KafkaPricingPartitionRouter


class KafkaPricingShardResolver(IPricingShardResolver):
    """
    ApplicationのPricingShardIdと
    Kafka partitionの対応づけをするAdapter。
    """
    def __init__(
        self,
        *,
        router: KafkaPricingPartitionRouter,
        partitions: set[int],
    ) -> None:
        self._router = router
        self._partitions = partitions

    def resolve(
        self,
        request_id: str,
    ) -> PricingShardId:
        partition = self._router.partition_for(
            request_id=request_id,
            partitions=set(self._partitions),
        )

        return PricingShardId(partition)
    