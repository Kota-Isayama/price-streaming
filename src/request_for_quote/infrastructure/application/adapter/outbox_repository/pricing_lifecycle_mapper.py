from request_for_quote.application.port.outbox_repository import OutboxEvent
from request_for_quote.application.pricing.lifecycle import PricingActivated, PricingChanged, PricingStopped, RfqPricingLifecycleEvent



def to_pricing_lifecycle_event(
    event: OutboxEvent,
) -> RfqPricingLifecycleEvent:
    payload = event.payload

    match event.event_type:
        case "rfq_pricing_activated":
            return PricingActivated(
                request_id=payload["rfq_id"],
                revision=int(payload["revision"]),
            )

        case "rfq_pricing_changed":
            return PricingChanged(
                request_id=payload["rfq_id"],
                revision=int(payload["revision"]),
            )

        case "rfq_pricing_stopped":
            return PricingStopped(
                request_id=payload["rfq_id"],
            )

        case _:
            raise ValueError(
                f"Unsupported outbox event: "
                f"{event.event_type}"
            )
        