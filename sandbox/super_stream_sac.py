import asyncio
import logging
import signal
import sys
from collections import defaultdict

from rstream import (
    AMQPMessage,
    ConsumerOffsetSpecification,
    EventContext,
    MessageContext,
    OffsetNotFound,
    OffsetSpecification,
    OffsetType,
    SuperStreamConsumer,
    amqp_decoder,
)


SUPER_STREAM = "rfq-pricing-events"

# SACのgroup名。
# 全Pricing Workerで同じ値を使う。
CONSUMER_GROUP = "pricing-workers"

INSTANCE_ID = sys.argv[1] if len(sys.argv) >= 2 else "worker-unknown"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)

async def on_message(
    message: AMQPMessage,
    context: MessageContext,
) -> None:
    logging.info(
        "[%s] MESSAGE "
        "stream=%s offset=%s body=%s",
        INSTANCE_ID,
        context.stream,
        context.offset,
        message.body,
    )

    # 公式exampleと同様、
    # 動作確認のため毎message offsetを保存する。
    #
    # 公式サンプルにもある通り、
    # productionで毎message storeするのは
    # performance上推奨されない。
    if context.subscriber_name is not None:
        await context.consumer.store_offset(
            stream=context.stream,
            offset=context.offset,
            subscriber_name=context.subscriber_name,
        )


async def consumer_update_handler(
    is_active: bool,
    context: EventContext,
) -> OffsetSpecification:
    if is_active:
        logging.info(
            "[%s] ACQUIRED partition=%s",
            INSTANCE_ID,
            context.stream,
        )

        try:
            offset = await context.consumer.query_offset(
                stream=context.stream,
                subscriber_name=CONSUMER_GROUP,
            )

            logging.info(
                "[%s] resume partition=%s "
                "stored_offset=%s",
                INSTANCE_ID,
                context.stream,
                offset,
            )

            return OffsetSpecification(
                offset_type=OffsetType.OFFSET,
                offset=offset + 1,
            )

        except OffsetNotFound:
            logging.info(
                "[%s] no stored offset" 
                "partition=%s; start from beginning",
                INSTANCE_ID,
                context.stream,
            )

            return OffsetSpecification(
                offset_type=OffsetType.OFFSET,
                offset=0,
            )

    # inactiveになった場合
    logging.info(
        "[%s] Released partition=%s",
        INSTANCE_ID,
        context.stream,
    )

    # inactive時にもOffsetSpecificationを返す必要があるため、
    # 公式exampleに合わせて0を返す。
    return OffsetSpecification(
        OffsetType.OFFSET,
        0,
    )

async def main() -> None:
    consumer = SuperStreamConsumer(
        host="localhost",
        port=5552,
        vhost="rfq",
        username="rfq",
        password="rfq",
        super_stream=SUPER_STREAM,
    )

    loop = asyncio.get_running_loop()

    loop.add_signal_handler(
        signal.SIGINT,
        lambda: asyncio.create_task(
            consumer.close(),
        ),
    )

    properties: dict[str, str] = defaultdict(str)

    properties["single-active-consumer"] = "true"
    properties["name"] = CONSUMER_GROUP
    properties["super-stream"] = SUPER_STREAM

    await consumer.subscribe(
        callback=on_message,
        offset_specification=ConsumerOffsetSpecification(
            offset_type=OffsetType.FIRST,
        ),
        decoder=amqp_decoder,

        # SAC用properties
        properties=properties,

        # server-side offset trackingで使われるname
        subscriber_name=CONSUMER_GROUP,

        # active/inactive切替通知
        consumer_update_listener=(
            consumer_update_handler
        ),
    )

    logging.info(
        "[%s] consumer started",
        INSTANCE_ID,
    )

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
