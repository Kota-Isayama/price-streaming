import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from request_for_quote.presentation.fastapi.dependencies import NotificationSubscriberDep


router = APIRouter(prefix="/notifications")


@router.websocket("/ws")
async def notification_websocket(
    websocket: WebSocket,
    subscriber: NotificationSubscriberDep,
) -> None:
    await websocket.accept()

    # TODO:
    # 認証済みUserから取得する。
    # PoCでは一旦固定/Queryでも可。
    recipient = "Kota"

    async def send_notifications() -> None:
        async for notification in subscriber.subscribe(recipient):
            await websocket.send_json(
                notification.model_dump(
                    mode="json"
                )
            )

    async def receive_until_disconnect() -> None:
        try:
            while True:
                await websocket.receive()
        except WebSocketDisconnect:
            pass

    send_task = asyncio.create_task(
        send_notifications()
    )

    receive_task = asyncio.create_task(
        receive_until_disconnect()
    )

    done, pending = await asyncio.wait(
        {
            send_task,
            receive_task,
        },
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()

    await asyncio.gather(
        *pending,
        return_exceptions=True,
    )

    for task in done:
        exception = task.exception()

        if exception is not None:
            raise exception
        