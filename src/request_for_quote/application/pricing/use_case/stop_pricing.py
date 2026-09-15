from typing import Callable

from request_for_quote.application.port.pricing_session_registry import IPricingSessionRegistry
from request_for_quote.application.pricing.port.pricing_session_unit_of_work import IPricingSessionUnitOfWork


class StopPricingUseCase:
    def __init__(
        self,
        pricing_session_uow_factory: Callable[[], IPricingSessionUnitOfWork],
        session_store: IPricingSessionRegistry,
    ) -> None:
        self._pricing_session_uow_factory = pricing_session_uow_factory
        self._session_store = session_store

    async def execute(
        self,
        request_id: str,
    ) -> None:
        session = self._session_store.get(request_id)

        if session is None:
            return

        async with self._pricing_session_uow_factory() as uow:
            await uow.get_pricing_session_store().save(session.deactivated())
            await uow.commit()
        self._session_store.remove(request_id)

        print(
            f"[STOP] "
            f"request={request_id}"
        )

        