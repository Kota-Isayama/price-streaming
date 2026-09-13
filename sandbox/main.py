import asyncio

from aiokafka import AIOKafkaConsumer


TOPIC = "rfq-pricing-lifecycle"

async def main() -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="rfq-pricing-dev",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    await consumer.start()

    try:
        async for message in consumer:
            print(
                f"partition={message.partition} "
                f"offset={message.offset} "
                f"key={message.key!r} "
                f"value={message.value!r}"
            )

            await consumer.commit()

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
    