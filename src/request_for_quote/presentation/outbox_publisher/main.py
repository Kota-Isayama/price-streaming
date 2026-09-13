import asyncio

from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from request_for_quote.application.port.pricing_lifecycle_publisher import (
    IPricingLifecyclePublisher,
)
from request_for_quote.infrastructure.application.adapter.outbox_repository.pricing_lifecycle_mapper import (
    to_pricing_lifecycle_event,
)
from request_for_quote.infrastructure.application.adapter.outbox_repository.sql_alchemy_repository import (
    SqlAlchemyOutboxRepository,
)
from request_for_quote.infrastructure.application.adapter.pricing_lifecycle_publisher.kafka_pricing_lifecycle_pubisher import (
    KafkaPricingLifecyclePublisher,
)
from request_for_quote.infrastructure.postgres.base import (
    create_engine,
    create_session_maker,
)


DATABASE_URL = (
    "postgresql+asyncpg://"
    "postgres:postgres@localhost:5432/request_for_quote"
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "rfq-pricing-lifecycle"


async def publish_pending_events(
    session_maker: async_sessionmaker[AsyncSession],
    publisher: IPricingLifecyclePublisher,
) -> None:
    while True:
        async with session_maker() as session:
            outbox_repository = SqlAlchemyOutboxRepository(
                session
            )

            events = await outbox_repository.list_pending(
                limit=100
            )

            for outbox_event in events:
                lifecycle_event = to_pricing_lifecycle_event(
                    outbox_event
                )

                await publisher.publish(
                    lifecycle_event
                )

                print(
                    f"[EVENT] "
                    f"published {outbox_event.event_id} "
                    f"{type(lifecycle_event)}"
                )

                await outbox_repository.mark_published(
                    outbox_event.event_id
                )

            await session.commit()

        await asyncio.sleep(1)


async def main() -> None:
    engine = create_engine(DATABASE_URL)
    session_maker = create_session_maker(engine)

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )

    await producer.start()

    try:
        publisher = KafkaPricingLifecyclePublisher(
            producer=producer,
            topic=TOPIC,
        )

        await publish_pending_events(
            session_maker=session_maker,
            publisher=publisher,
        )

    finally:
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())