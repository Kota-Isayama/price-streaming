from datetime import datetime, timezone
from typing import Callable
import uuid

from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork


class StopRfqPricingUseCase:
    def __init__(
        self,
        uow_factory: Callable[[], IRfqUnitOfWork],
    ) -> None:
        self._uow_factory = uow_factory

    async def execute(
        self,
        rfq_id: str,
    ) -> None:
        async with self._uow_factory() as uow:
            await uow.outbox.add(
                OutboxEvent(
                    event_id=str(uuid.uuid4()),
                    event_type="rfq_pricing_stopped",
                    payload={
                        "rfq_id": rfq_id,
                    },
                    created_at=datetime.now(timezone.utc),
                )
            )

            await uow.commit()