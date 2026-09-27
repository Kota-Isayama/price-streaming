import asyncio
from gc import enable

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis

from request_for_quote.application.integration.events.rfq import RfqRegisteredIntegrationEvent
from request_for_quote.application.notification.use_case.notify_assigned_trader_on_rfq_registered import NotifyAssignedTraderOnRfqRegisteredUseCase
from request_for_quote.application.port.rfq_domain_events_subscriber import IRfqDomainEventsSubscriber

from request_for_quote.bootstrap.processes.notification_worker import bootstrap_notification_worker
from request_for_quote.bootstrap.settings.notification_worker import NotificationWorkerSettings
from request_for_quote.infrastructure.application.adapter.notification_publisher.redis.redis_notification_publisher import RedisNotificationPublisher
from request_for_quote.infrastructure.application.adapter.notification_repository.postgres.sql_alchemy_notification_repository import SqlAlchemyNotificationRepository
from request_for_quote.infrastructure.application.adapter.notification_unit_of_work.postgres.sql_alchemy_notification_unit_of_work import SqlAlchemyNotificationUnitOfWork
from request_for_quote.infrastructure.application.adapter.rfq_domain_events_subscriber.kafka.kafka_rfq_domain_events_subscriber import KafkaRfqDomainEventsSubscriber


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
    settings = NotificationWorkerSettings.load()

    async with bootstrap_notification_worker(settings=settings) as worker_container:
        # UoW factory
        notification_uow_factory = lambda: SqlAlchemyNotificationUnitOfWork(worker_container.session_maker)

        # Application UseCase
        notify_on_rfq_registered = NotifyAssignedTraderOnRfqRegisteredUseCase(
            notification_uow_factory=notification_uow_factory,
            notification_publisher=worker_container.notification_updates,
        )

        await consume_rfq_domain_events(
            subscriber=worker_container.rfq_events,
            notify_on_rfq_registered=notify_on_rfq_registered,
        )


if __name__ == "__main__":
    asyncio.run(main())
