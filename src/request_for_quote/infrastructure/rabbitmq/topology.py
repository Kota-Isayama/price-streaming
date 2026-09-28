import aio_pika
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
)


async def declare_pricing_topology(
    *,
    channel: AbstractRobustChannel,
    exchange_name: str,
    queue_prefix: str,
    shard_count: int,
) -> tuple[AbstractRobustExchange, dict[int, AbstractRobustQueue]]:
    """
    topology:

        rfq.pricing exchange
              │
              ├── pricing.0 -> rfq.pricing.0
              ├── pricing.1 -> rfq.pricing.1
              ├── pricing.2 -> rfq.pricing.2
              └── pricing.3 -> rfq.pricing.3

    各Queue:
      - durable
      - Quorum Queue
      - Single Active Consumer
    """
    exchange = await channel.declare_exchange(
        exchange_name,
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    queues: dict[int, AbstractRobustQueue] = {}

    for shard_id in range(shard_count):
        queue_name = f"{queue_prefix}.{shard_id}"
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-queue_type": "quorum",
                "x-single-active-consumer": True,
            },
        )

        await queue.bind(
            exchange,
            routing_key=f"pricing.{shard_id}",
        )

        queues[shard_id] = queue

    return exchange, queues
