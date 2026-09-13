import copy
from decimal import Decimal

from request_for_quote.domain.product import Product


class RequestForQuote:
    def __init__(
        self,
        rfq_id: str,
        revision: int,
        product: Product,
    ) -> None:
        self._rfq_id = rfq_id
        self._revision = revision
        self._product = product

    @property
    def rfq_id(self) -> str:
        return self._rfq_id

    @property
    def revision(self) -> str:
        return self._revision

    @property
    def product(self) -> Product:
        return self._product

    def change_product(
        self,
        product: Product,
    ) -> None:
        self._product = product
        self._revision = self.revision + 1
