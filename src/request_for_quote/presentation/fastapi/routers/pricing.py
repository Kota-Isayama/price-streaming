import asyncio
import enum
from typing import Literal

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from request_for_quote.application.pricing.use_case.pricing_subscription_service import PricingSubscriptionService
from request_for_quote.presentation.fastapi.dependencies import PricingSubscriptionServiceDep

router = APIRouter(prefix="/pricing")


class MessageType(enum.StrEnum):
    SUBSCRIPTION_SET = "subscription_set"
    PRICE_UPDATE = "price_update"


class SetPricingSubscription(BaseModel):
    message_type: Literal[MessageType.SUBSCRIPTION_SET] = MessageType.SUBSCRIPTION_SET
    request_ids: list[str]


class PricingUpdateMessage(BaseModel):
    message_type: Literal[MessageType.PRICE_UPDATE] = MessageType.PRICE_UPDATE
    request_id: str
    par_rate: str


@router.websocket("/ws")
async def pricing_websocket(
    websocket: WebSocket,
    service: PricingSubscriptionServiceDep,
) -> None:
    await websocket.accept()

    receive_commands_task = asyncio.create_task(
        _receive_commands(
            websocket,
            service,
        )
    )

    send_updates_task = asyncio.create_task(
        _send_updates(
            websocket,
            service,
        )
    )

    try:
        done, pending = await asyncio.wait(
            {
                receive_commands_task,
                send_updates_task,
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
            task.result()
    
    except WebSocketDisconnect:
        pass
    


async def _receive_commands(
    websocket: WebSocket,
    service: PricingSubscriptionService,
) -> None:
    while True:
        payload = await websocket.receive_json()

        message = SetPricingSubscription.model_validate(
            payload,
        )

        await service.set_subscription(
            set(message.request_ids),
        )


async def _send_updates(
    websocket: WebSocket,
    service: PricingSubscriptionService,
) -> None:
    async for update in service.watch_price_updates():
        await websocket.send_json(
            PricingUpdateMessage(
                request_id=update.request_id,
                par_rate=str(update.par_rate),
            ).model_dump(mode="json"),
        )
