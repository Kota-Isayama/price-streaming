# application/ports/pricing_request_revision_repository.py

from typing import Protocol

from request_for_quote.application.models.pricing_request_revision import (
    PricingRequestRevision,
)


class PricingRequestRevisionRepository(Protocol):
    async def save(
        self,
        revision: PricingRequest,
    ) -> None:
        ...