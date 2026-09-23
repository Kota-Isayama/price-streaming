from typing import Callable

from request_for_quote.application.rfq.unit_of_work import IRfqUnitOfWork
from request_for_quote.domain.request_for_quote.request_for_quote import RequestForQuote


class ListRfqsUseCase:
    def __init__(
        self,
        uow_factory: Callable[[], IRfqUnitOfWork],
    ) -> None:
        self._uow_factory = uow_factory

    async def execute(self) -> list[RequestForQuote]:
        async with self._uow_factory() as uow:
            return await uow.get_rfq_repository().list_all()
