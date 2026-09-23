from email import message


class KafkaEventRouter:
    def resolve_topic(
        self,
        event_type: str,
    ) -> str:
        match event_type:
            case "rfq.registered":
                return "rfq-domain-events"

            case "pricing.swap.activate":
                return "rfq-pricing-lifecycle"

            case _:
                raise ValueError(
                    "Unknown integration event: "
                    f"{event_type}"
                )
            