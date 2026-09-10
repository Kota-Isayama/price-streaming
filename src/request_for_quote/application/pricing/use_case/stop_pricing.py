from request_for_quote.application.port.pricing_session_store import IPricingSessionStore


class StopPricingUseCase:
    def __init__(
        self,
        session_store: IPricingSessionStore,
    ) -> None:
        self._session_store = session_store

    async def execute(
        self,
        request_id: str,
    ) -> None:
        session = self._session_store.get(request_id)

        if session is None:
            return

        self._session_store.remove(request_id)

        print(
            f"[STOP] "
            f"request={request_id}"
        )

        