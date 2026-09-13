# application/ports/pricing_request_revision_repository.py

import abc
from typing import Protocol

from request_for_quote.domain.pricing.request import SwapPricingRequest



class IPricingRequestRepository(abc.ABC):
    @abc.abstractmethod
    async def save(
        self,
        pricing_request: SwapPricingRequest,
    ) -> None:
        raise NotImplementedError


    @abc.abstractmethod
    async def get_by_id_and_revision(
        self,
        request_id: str,
        revision: int,
    ) -> SwapPricingRequest:
        raise NotImplementedError
    