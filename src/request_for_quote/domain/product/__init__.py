from typing import TypeAlias

from request_for_quote.domain.product.bond import Bond
from request_for_quote.domain.product.swap import InterestRateSwap


Product: TypeAlias = InterestRateSwap | Bond
