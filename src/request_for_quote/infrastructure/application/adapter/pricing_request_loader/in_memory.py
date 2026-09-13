from request_for_quote.application.port.pricing_request_loader import IPricingRequestLoader, LoadedPricingRequest


class InMemoryPricingRequestLoader(IPricingRequestLoader):
    def __init__(
        self,
        requests: dict[tuple[str, int], LoadedPricingRequest],
    ) -> None:
        self._requests = requests

    async def load(
        self,
        request_id: str,
        revision: int,
    ) -> LoadedPricingRequest | None:
        key = (request_id, revision)

        return self._requests.get(key, None)

    def add(
        self,
        loaded_request: LoadedPricingRequest,
    ) -> None:
        request = loaded_request.request

        self._requests[(request.request_id, request.revision)] = loaded_request
        