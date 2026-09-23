import asyncio
from gc import enable

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.notification.use_case.notify_assigned_trader_on_rfq_registered import NotifyAssignedTraderOnRfqRegisteredUseCase
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber
from request_for_quote.application.pricing import session
from request_for_quote.infrastructure.application.adapter.notification_publisher.redis.redis_notification_publisher import RedisNotificationPublisher
from request_for_quote.infrastructure.application.adapter.notification_repository.postgres.sql_alchemy_notification_repository import SqlAlchemyNotificationRepository
from request_for_quote.infrastructure.application.adapter.notification_unit_of_work.postgres.sql_alchemy_notification_unit_of_work import SqlAlchemyNotificationUnitOfWork
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.kafka_rfq_domain_events_subscriber import KafkaRfqDomainEventsSubscriber
from request_for_quote.infrastructure.postgres.base import create_engine, create_session_maker


DATABASE_URL = (
    "postgresql+asyncpg://"
    "postgres:postgres@localhost:5432/request_for_quote"
)

RFQ_DOMAIN_EVENTS_TOPIC = "rfq-domain-events"
NOTIFICATION_REDIS_URL = "redis://localhost:6379"


async def consume_rfq_domain_events(
    subscriber: IRfqDomainEventsSubscriber,
    notify_on_rfq_registered: NotifyAssignedTraderOnRfqRegisteredUseCase,
) -> None:
    async for event in subscriber.subscribe():
        match event:
            case RfqRegisteredIntegrationEvent():
                await notify_on_rfq_registered.execute(event)
            case _:
                raise ValueError(
                    "Unsupported RFQ domain event: "
                    f"{type(event).__name__}"
                )

        # NotificationがDBへ正常に保存された後で
        # Kafka offsetをcommitする
        await subscriber.ack()


async def main() -> None:
    # Infra preparation
    engine = create_engine(DATABASE_URL)
    session_maker = create_session_maker(engine)

    # UoW factory
    notification_uow_factory = lambda: SqlAlchemyNotificationUnitOfWork(session_maker)

    # Redis
    redis_client = redis.Redis.from_url(NOTIFICATION_REDIS_URL, decode_responses=True)
    redis_publisher = RedisNotificationPublisher(redis_client=redis_client)

    # Application UseCase
    notify_on_rfq_registered = NotifyAssignedTraderOnRfqRegisteredUseCase(
        notification_uow_factory=notification_uow_factory,
        notification_publisher=redis_publisher,
    )

    # Kafka
    consumer = AIOKafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id="notification-workers",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        session_timeout_ms=6000,
        heartbeat_interval_ms=2000,
    )

    consumer.subscribe(
        topics=[
            RFQ_DOMAIN_EVENTS_TOPIC
        ]
    )

    subscriber = KafkaRfqDomainEventsSubscriber(consumer=consumer)

    # Process lifecycle
    await consumer.start()

    try:
        await consume_rfq_domain_events(
            subscriber=subscriber,
            notify_on_rfq_registered=notify_on_rfq_registered,
        )
    finally:
        await consumer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
