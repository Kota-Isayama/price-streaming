import abc

from request_for_quote.domain.request_for_quote.request_for_quote import RequestForQuote


class IRfqRepository(abc.ABC):
    @abc.abstractmethod
    async def save(self, rfq: RequestForQuote) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_by_id(self, rfq_id: str) -> RequestForQuote | None:
        raise NotImplementedError
    