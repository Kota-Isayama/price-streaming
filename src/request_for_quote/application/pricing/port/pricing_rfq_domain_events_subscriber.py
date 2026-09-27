import abc
from collections.abc import AsyncIterator

from request_for_quote.application.pricing.model.pricing_shard import (
    PricingShardId,
)
from request_for_quote.application.pricing.model.rfq_domain_event_delivery import (
    RfqDomainEventDelivery,
)


class IPricingRfqDomainEventsSubscriber(abc.ABC):
    """
    Pricing Worker用RFQ Domain Event Subscriber。

    Applicationから見ると、

        subscribe(shard-2)
            ↓
        shard-2のイベントだけ流れてくる

    という契約。

    Kafkaが内部的に1 Consumerで複数partitionを読んでいることは
    Applicationには見せない。
    """

    @abc.abstractmethod
    def subscribe(
        self,
        shard_id: PricingShardId,
    ) -> AsyncIterator[RfqDomainEventDelivery]:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(
        self,
        delivery: RfqDomainEventDelivery,
    ) -> None:
        """
        このdeliveryの処理が正常終了したことをBrokerへ通知する。

        Kafka:
            offset commit

        Super Stream:
            offset store

        Quorum Queue:
            message ack
        """

        raise NotImplementedError
    