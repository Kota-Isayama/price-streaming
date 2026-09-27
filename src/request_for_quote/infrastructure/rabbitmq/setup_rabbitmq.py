import asyncio
from math import e

import aio_pika

from request_for_quote.infrastructure.rabbitmq.topology import declare_integration_event_topology


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://rfq:rfq@localhost:5672/rfq",
    )

    async with connection:
        channel = await connection.channel()

        exchange, queue = await declare_integration_event_topology(channel)

        print(f"exchange: {exchange.name}")
        print(f"queue: {queue.name}")


if __name__ == "__main__":
    asyncio.run(main())
    