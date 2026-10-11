from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel


class TenorMessage(BaseModel):
    value: int

    unit: Literal[
        "D",
        "W",
        "M",
        "Y",
    ]


class SwapQuoteMessage(BaseModel):
    tenor: TenorMessage
    par_rate: Decimal


class SwapQuoteSnapshotMessage(
    BaseModel
):
    quote_set_id: str
    as_of: datetime

    quotes: tuple[
        SwapQuoteMessage,
        ...,
    ]
    