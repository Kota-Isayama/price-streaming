import copy
from decimal import Decimal
from typing import Self

from request_for_quote.domain.product import Product
from request_for_quote.domain.product.swap import InterestRateSwap
from request_for_quote.domain.request_for_quote.events import RfqRegistredDomainEvent
from request_for_quote.domain.shared.aware_datetime import AwareDateTime

class RequestForQuote:
    def __init__(
        self,
        rfq_id: str,
        revision: int,
        product: InterestRateSwap,
        assigned_trader: str,
        registered_by: str,
    ) -> None:
        self._rfq_id = rfq_id
        self._revision = revision
        self._product = product
        self._assigned_trader = assigned_trader
        self._registered_by = registered_by

        self._domain_events: list[RfqRegistredDomainEvent] = []

    def _record_event(self, event: RfqRegistredDomainEvent) -> None:
        self._domain_events.append(event)

    def pull_domain_events(self) -> list[RfqRegistredDomainEvent]:
        events = list(self._domain_events)
        self._domain_events.clear()
        return events

    @classmethod
    def register(
        cls,
        *,
        rfq_id: str,
        product: InterestRateSwap,
        assigned_trader: str,
        registered_by: str,
        occurred_at: AwareDateTime,
    ) -> Self:
        rfq = cls(
            rfq_id=rfq_id,
            revision=1,
            product=product,
            assigned_trader=assigned_trader,
            registered_by=registered_by,
        )

        rfq._record_event(
            RfqRegistredDomainEvent(
                rfq_id=rfq.rfq_id,
                revision=rfq.revision,
                product=rfq.product,
                assigned_trader=rfq.assigned_trader,
                registered_by=registered_by,
                occurred_at=occurred_at,
            )
        )

        return rfq

    @property
    def rfq_id(self) -> str:
        return self._rfq_id

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def product(self) -> InterestRateSwap:
        return self._product

    @property
    def assigned_trader(self) -> str:
        return self._assigned_trader

    @property
    def registered_by(self) -> str:
        return self._registered_by

    def change_product(
        self,
        product: InterestRateSwap,
    ) -> None:
        self._product = product
        self._revision = self.revision + 1
