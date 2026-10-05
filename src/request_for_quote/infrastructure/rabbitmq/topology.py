import aio_pika

from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
)


async def declare_pricing_exchange(
    *,
    channel: AbstractRobustChannel,
    exchange_name: str,
) -> AbstractRobustExchange:
    return await channel.declare_exchange(
        exchange_name,
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
    )


async def declare_quorum_pricing_topology(
    *,
    channel: AbstractRobustChannel,
    exchange_name: str,
    queue_prefix: str,
    shard_count: int,
) -> tuple[
    AbstractRobustExchange,
    dict[int, AbstractRobustQueue],
]:
    exchange = await declare_pricing_exchange(
        channel=channel,
        exchange_name=exchange_name,
    )

    queues: dict[int, AbstractRobustQueue] = {}

    for shard_id in range(shard_count):
        queue = await channel.declare_queue(
            f"{queue_prefix}.{shard_id}",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-single-active-consumer": True,
            },
        )

        await queue.bind(
            exchange,
            routing_key=f"pricing.{shard_id}",
        )

        queues[shard_id] = queue

    return exchange, queues


async def bind_super_stream_pricing_topology(
    *,
    channel: AbstractRobustChannel,
    exchange_name: str,
    super_stream: str,
    partition_count: int,
) -> AbstractRobustExchange:
    """
    既存Super Streamの各partition streamを、
    共通pricing exchangeへ追加でbindする。

    例:

        rfq.pricing
            |
            | pricing.0
            v
        rfq-pricing-events-0

        rfq.pricing
            |
            | pricing.1
            v
        rfq-pricing-events-1
    """

    exchange = await declare_pricing_exchange(
        channel=channel,
        exchange_name=exchange_name,
    )

    for shard_id in range(partition_count):
        partition_stream_name = (
            f"{super_stream}-{shard_id}"
        )

        # Super Stream自体はすでに
        # rabbitmq-streams add_super_stream で作成済み。
        #
        # type等を再declareせず、既存Streamを取得する。
        partition_stream = await channel.get_queue(
            partition_stream_name,
            ensure=True,
        )

        await partition_stream.bind(
            exchange,
            routing_key=f"pricing.{shard_id}",
        )

    return exchange
