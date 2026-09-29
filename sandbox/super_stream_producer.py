import asyncio
import json

from rstream import (
    AMQPMessage,
    RouteType,
    SuperStreamProducer,
    SuperStreamCreationOption,
)

SUPER_STREAM = "rfq-pricing-events"

async def routing_extractor(
    message: AMQPMessage,
) -> str:
    """
    同じRFQ IDは必ず同じpartitionに送られるためのkey
    
    RouteType.Hashの場合、この戻り値をrstreamがhashしてpartitionを決定する。
    """
    return str(
        message.application_properties["rfq_id"],  # なんだこれ
    )


async def main() -> None:
    creation_option = SuperStreamCreationOption(
        n_partitions=4,
    )

    async with SuperStreamProducer(
        host="localhost",
        port=5552,
        vhost="rfq",
        username="rfq",
        password="rfq",
        super_stream=SUPER_STREAM,
        super_stream_creation_option=creation_option,
        routing_extractor=routing_extractor,
        routing=RouteType.Hash,
    ) as producer:
        rfq_ids = [
            "rfq-A",
            "rfq-B",
            "rfq-A",
            "rfq-C",
            "rfq-B",
            "rfq-A",
            "rfq-A",
            "rfq-D",
            "rfq-E",
            "rfq-F",
            "rfq-G",
            "rfq-G",
        ]

        for index, rfq_id in enumerate(rfq_ids):
            payload = {
                "sequence": index,
                "rfq_id": rfq_id,
                "event_type": "RfqRegistered",
            }

            message = AMQPMessage(
                body=json.dumps(payload).encode("utf-8"),
                application_properties={
                    "rfq_id": rfq_id,
                },
            )

            await producer.send(message)

            print(
                f"published: "
                f"rfq_id={rfq_id}, "
                f"sequence={index}"
            )

if __name__ == "__main__":
    asyncio.run(main())
