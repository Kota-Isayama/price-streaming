import asyncio
import signal

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetType,
    SuperStreamConsumer,
    SuperStreamCreationOption,
    amqp_decoder,
)


SUPER_STREAM = "rfq-pricing-events"


async def on_message(
    message: AMQPMessage,
    context: MessageContext,
) -> None:
    print(
        "received: "
        f"stream={context.stream}, "
        f"offset={context.offset}, "
        f"body={message.body}"
    )


async def main() -> None:
    creation_option = SuperStreamCreationOption(
        n_partitions=4,
    )

    consumer = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="rfq",
        username="rfq",
        password="rfq",
        super_stream=SUPER_STREAM,
        super_stream_creation_option=creation_option,
    )

    loop = asyncio.get_running_loop()

    loop.add_signal_handler(
        signal.SIGINT,
        lambda: asyncio.create_task(
            consumer.close()
        ),
    )

    await consumer.start()

    await consumer.subscribe(
        callback=on_message,
        decoder=amqp_decoder,
        offset_specification=(
            ConsumerOffsetSpecification(
                OffsetType.FIRST,
                None,
            )
        ),
    )

    print(
        f"consuming super stream: "
        f"{SUPER_STREAM}"
    )

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
