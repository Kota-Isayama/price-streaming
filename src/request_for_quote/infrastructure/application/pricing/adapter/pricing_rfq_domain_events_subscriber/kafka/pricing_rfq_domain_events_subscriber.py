import asyncio
from dataclasses import dataclass
from typing import AsyncIterator
import uuid

from aiokafka import AIOKafkaConsumer, TopicPartition

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.model.rfq_domain_event_delivery import RfqDomainEventDelivery, RfqDomainEventDeliveryId
from request_for_quote.application.pricing.port.pricing_rfq_domain_events_subscriber import IPricingRfqDomainEventsSubscriber
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.kafka_integration_event_publisher import IntegrationEventEnvelope


@dataclass(frozen=True)
class _KafkaPendingDelivery:
    """
    Application ModelにはKafka情報を持たせないので、
    Adapter内部だけで保持する情報。

    delivery_id
    
    TopicPartition + offset

    ack(delivery)されたときに使う。
    """

    shard_id: PricingShardId
    topic_partition: TopicPartition
    offset: int


class KafkaRfqDomainEventsSubscriber(
    IPricingRfqDomainEventsSubscriber
):
    """
    役割は2つ。
    
    1. run()
        Kafka Consumerから全partitionのmessageを読む
        ↓
        partitionを見てshardごとのasyncio.Queueへ振り分ける。
        
    2. subscribe(shard_id)
        そのshard専用Queueだけを読む。

        
    Kafkaの実体:

        AIOKafkaConsumer
             │
             │
             │ p0 / p2 / p5 が混在
             ▼
            run()
             │
       message.partition
             │
       ┌─────┼─────┐
       ▼     ▼     ▼
     Queue0 Queue2 Queue5
       │     │     │
       ▼     ▼     ▼
      Task0 Task2 Task5


    Application側には、

        subscribe(shard_id)

    という綺麗なinterfaceだけを見せる。
    """
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
    ) -> None:
        self._consumer = consumer

        # Shardごとのmailbox
        #
        # PricingShardId(2)
        # ↓
        # asyncio.Queue(...)
        self._queues: dict[PricingShardId, asyncio.Queue[RfqDomainEventDelivery]] = {}

        # Application上のdelivery_idと
        # Kafka native情報の対応表
        self._pendings: dict[RfqDomainEventDeliveryId, _KafkaPendingDelivery] = {}

    async def run(self) -> None:
        """
        Kafkaからmessageを読む「唯一のTask」。
        
        重要
        AIOKafkaConsumerをshardごとのTaskから
        同時に直接読むのではない。
        
        1. Consumer
            ↓
        このrun()が全部を読む。
            ↓
        Queueへdemultiplexする。
        """
        print("[KAFKA DISPATCHER STARTED]")

        async for message in self._consumer:
            print(
                "[KAFKA MESSAGE RECEIVED] "
                f"topic={message.topic} "
                f"partition={message.partition} "
                f"offset={message.offset}"
            )
            shard_id = PricingShardId(message.partition)

            envelope = IntegrationEventEnvelope.model_validate_json(message.value.decode("utf-8"))

            print(
                "[KAFKA ENVELOPE] "
                f"type={envelope.message_type} "
                f"version={envelope.schema_version}"
            )

            event = self._deserialize(envelope)

            delivery_id = RfqDomainEventDeliveryId(value=str(uuid.uuid4()))

            delivery = RfqDomainEventDelivery(
                delivery_id=delivery_id,
                shard_id=shard_id,
                event=event,
            )

            # ACK時に必要なKafka情報を
            # Infrastructure内部に保存する。
            self._pendings[
                delivery_id
            ] = _KafkaPendingDelivery(
                shard_id=shard_id,
                topic_partition=TopicPartition(
                    message.topic,
                    message.partition,
                ),
                offset=message.offset,
            )

            # Runtime側のsubscribe(shard_id)と同じQueueを取得する。
            queue = self._queues.setdefault(
                shard_id,
                asyncio.Queue(),
            )

            # Domain Eventは通常高頻度ではないので
            # 第一実装ではunbounded Queueにする
            #
            # Queueに入れたら、このdispatcher自体は
            # 次のKafka messageをとりにいける。
            queue.put_nowait(delivery)

    async def subscribe(
        self,
        shard_id: PricingShardId,
    ) -> AsyncIterator[RfqDomainEventDelivery]:
        """
        特定shard専用のStream。
        
        Runtimeからは、
            async for delivery in subscriber.subscribe(shard_2)
        と呼ばれる。

        Queueが空なら
            await queue.get()
        で待つ。
        その間Event loopは別Taskを実行できる。
        """
        queue = self._queues.setdefault(
            shard_id,
            asyncio.Queue(),
        )

        try:
            while True:
                delivery = await queue.get()
                yield delivery
        finally:
            # ownershipを失ってconsumer Taskがcancelされると
            # このfinallyに入る
            #
            # 未Ack messageはcommitしない
            #
            # つまり、新OwnerでKafkaから再配送される。
            current_queue = self._queues.get(shard_id)

            if current_queue is queue:
                self._queues.pop(
                    shard_id,
                    None,
                )

            pending_ids = [
                delivery_id
                for delivery_id, pending in self._pendings.items()
                if pending.shard_id == shard_id
            ]

            for delivery_id in pending_ids:
                self._pendings.pop(
                    delivery_id,
                    None,
                )

    async def ack(
        self,
        delivery: RfqDomainEventDelivery,
    ) -> None:
        """
        Shard内は逐次処理なので、
        
            offset 10 handle
            offset 10 commit
            offset 11 handle
            offset 11 commit
            
        になる。
        
        したがってout-of-order ACK管理は不要
        """
        pending = self._pendings.pop(
            delivery.delivery_id,
            None,
        )

        if pending is None:
            raise RuntimeError(
                "Unknown RFQ domain event delivery: "
                f"{delivery.delivery_id}"
            )

        # Kafka commitには「次に読むoffset」を渡す。
        #
        # message.offset = 10
        # ↓
        # commit 11
        await self._consumer.commit(
            {
                pending.topic_partition: pending.offset + 1,
            }
        )

    def _deserialize(
        self,
        envelope: IntegrationEventEnvelope,
    ) -> RfqRegisteredIntegrationEvent:
        match (
            envelope.message_type,
            envelope.schema_version,
        ):
            case ("rfq.registered", 1):
                return (
                    RfqRegisteredIntegrationEvent
                    .model_validate(
                        {
                            "event_id":
                                envelope.message_id,
                            **envelope.payload,
                        }
                    )
                )

            case _:
                raise ValueError(
                    "Unsupported RFQ domain event: "
                    f"type={envelope.message_type}, "
                    f"schema_version="
                    f"{envelope.schema_version}"
                )
            