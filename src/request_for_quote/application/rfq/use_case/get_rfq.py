from typing import Callable

from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork
from request_for_quote.domain.request_for_quote.request_for_quote import RequestForQuote


class GetRfqUseCase:
    def __init__(
        self,
        uow_factory: Callable[[], IRfqUnitOfWork],
    ) -> None:
        self._uow_factory = uow_factory

    async def execute(self, rfq_id: str) -> RequestForQuote:
        async with self._uow_factory() as uow:
            result = await uow.get_rfq_repository().get_by_id(rfq_id)
        if result is None:
            raise RuntimeError("Not found.")
        return result
                                                            