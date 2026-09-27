import asyncio

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
)

INTEGRATION_EVENT_EXCHANGE = "integration.events"
NOTIFICATION_EVENTS_QUEUE = "notification.events"


async def declare_integration_event_topology(
    channel: AbstractChannel,
) -> tuple[AbstractExchange, AbstractQueue]:
    exchange = await channel.declare_exchange(
        INTEGRATION_EVENT_EXCHANGE,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    queue = await channel.declare_queue(
        NOTIFICATION_EVENTS_QUEUE,
        durable=True,
    )

    await queue.bind(
        exchange,
        routing_key="rfq.created",
    )

    await queue.bind(
        exchange,
        routing_key="quote.created",
    )

    return exchange, queue
