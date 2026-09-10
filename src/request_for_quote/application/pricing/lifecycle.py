from dataclasses import dataclass


@dataclass(frozen=True)
class PricingActivated:
    request_id: str
    revision: int


@dataclass(frozen=True)
class PricingChanged:
    request_id: str
    revision: int


@dataclass(frozen=True)
class PricingStopped:
    request_id: str


RfqPricingLifecycleEvent = (
    PricingActivated
    | PricingChanged
    | PricingStopped
)