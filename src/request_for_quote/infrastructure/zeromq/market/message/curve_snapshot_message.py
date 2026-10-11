from datetime import (
    date,
    datetime,
)
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel


class CurveNodeMessage(BaseModel):
    pillar_date: date
    discount_factor: Decimal


class CurveSnapshotMessage(BaseModel):
    curve_id: str

    reference_date: date
    as_of: datetime

    nodes: tuple[
        CurveNodeMessage,
        ...,
    ]

    interpolation: Literal[
        "log_linear_discount"
    ] = "log_linear_discount"
    