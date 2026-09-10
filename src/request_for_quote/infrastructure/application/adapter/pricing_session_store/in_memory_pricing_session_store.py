from request_for_quote.application.port.pricing_session_store import IPricingSessionStore
from request_for_quote.application.pricing.session import PricingSession


class InMemoryPricingSessionStore(IPricingSessionStore):
    def __init__(self):
        self._sessions: dict[str, PricingSession] = {}

    def get(self, request_id) -> PricingSession | None:
        return self._sessions.get(request_id, None)

    def save(self, session: PricingSession) -> None:
        self._sessions[session.request.request_id] = session

    def remove(
        self,
        request_id: str,
    ) -> None:
        self._sessions.pop(
            request_id,
            None,
        )

    def list_all(
        self,
    ) -> list[PricingSession]:
        return list(
            self._sessions.values()
        )
        