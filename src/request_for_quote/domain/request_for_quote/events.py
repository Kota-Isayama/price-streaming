import dataclasses

from request_for_quote.domain.product.swap import InterestRateSwap
from request_for_quote.domain.shared.aware_datetime import AwareDateTime
from request_for_quote.domain.shared.domain_event import DomainEvent


@dataclasses.dataclass(frozen=True)
class RfqRegistredDomainEvent(DomainEvent):
    rfq_id: str
    revision: int
    product: InterestRateSwap

    assigned_trader: str
    registered_by: str
    
    occurred_at: AwareDateTime

