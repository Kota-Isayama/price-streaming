import asyncio
import json

import zmq
import zmq.asyncio

ENDPOINT = "tcp://127.0.0.1:5555"

async def main() -> None:
    context = zmq.asyncio.Context.instance()

    socket = context.socket(zmq.PUB)
    socket.bind(ENDPOINT)

    print(
        f"[MARKET DATA] ",
        f"publisher started: {ENDPOINT}"
    )

    rate = 100

    try:
        while True:
            rate += 1

            value = f"0.{rate:04d}"

            await socket.send_multipart(
                [
                    b"JPY-OIS",
                    json.dumps(
                        {"value": value}
                    ).encode("utf-8")
                ]
            )

            print(
                f"[PUBLISH] "
                f"JPY-OIS={value}"
            )

            await asyncio.sleep(2)

    finally:
        socket.close(linger=0)


if __name__ == "__main__":
    asyncio.run(main())
    