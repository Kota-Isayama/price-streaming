import asyncio
import logging

from aiokafka import AIOKafkaProducer


from request_for_quote.application.outbox.use_case.publish_outbox_events import PublishOutboxEventsUseCase
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.event_router import KafkaEventRouter
from request_for_quote.infrastructure.application.adapter.integration_event_publisher.kafka.kafka_integration_event_publisher import KafkaIntegrationEventPublisher
from request_for_quote.infrastructure.application.adapter.outbox_delivery_uow.sql_alchemy_outbox_delivery_uow import SqlAlchemyOutboxDeliveryUnitOfWork
from request_for_quote.infrastructure.postgres.base import create_engine, create_session_maker


logger = logging.getLogger(__name__)

DATABASE_URL = (
    "postgresql+asyncpg://"
    "postgres:postgres@localhost:5432/request_for_quote"
)

PRICING_LIFECYCLE_TOPIC = "rfq-domain-events"


class OutboxWorker:
    def __init__(
        self,
        *,
        use_case: PublishOutboxEventsUseCase,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._use_case = use_case
        self._poll_interval_seconds = poll_interval_seconds

    async def run(self) -> None:
        while True:
            try:
                result = await self._use_case.execute(limit=100)

                if result.claimed == 0:
                    await asyncio.sleep(self._poll_interval_seconds)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Outbox worker failed"
                )

                await asyncio.sleep(self._poll_interval_seconds)


async def main() -> None:
    engine = create_engine(DATABASE_URL)
    session_maker = create_session_maker(engine)
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
    )
    await kafka_producer.start()

    outbox_uow_factory = lambda: (
        SqlAlchemyOutboxDeliveryUnitOfWork(
            session_maker
        )
    )

    publisher = KafkaIntegrationEventPublisher(
        producer=kafka_producer,
        router=KafkaEventRouter(),
    )

    publish_outbox_use_case = (
        PublishOutboxEventsUseCase(
            uow_factory=outbox_uow_factory,
            publisher=publisher,
        )
    )

    worker = OutboxWorker(
        use_case=publish_outbox_use_case,
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

