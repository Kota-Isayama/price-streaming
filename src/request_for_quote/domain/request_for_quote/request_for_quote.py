from request_for_quote.domain.product import Product


class RequestForQuote:
    def __init__(
        self,
        request_id: str,
        revision: int,
        product: Product,
    ) -> None:
        self._request_id = request_id
        self._revision = revision
        self._product = product

    