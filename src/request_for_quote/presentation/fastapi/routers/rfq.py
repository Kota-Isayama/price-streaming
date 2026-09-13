from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, status
from pydantic import BaseModel

from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import PayReceive
from request_for_quote.presentation.fastapi.dependencies import ChangeRfqUseCaseDep, CreateRfqUseCaseDep, StopRfqPricingUseCaseDep


router = APIRouter(prefix="/rfq", tags=["rfqs"])


class CreateSwapRfqRequest(BaseModel):
    notional: Decimal
    effective_date: date
    maturity_date: date
    fixed_leg: PayReceive
    currency: Currency


class CreateRfqResponse(BaseModel):
    rfq_id: str
    revision: int


class ChangeSwapRfqRequest(BaseModel):
    notional: Decimal | None = None
    effective_date: date | None = None
    maturity_date: date | None = None
    fixed_leg: PayReceive | None = None
    currency: Currency | None = None


class ChangeRfqResponse(BaseModel):
    rfq_id: str
    revision: int


@router.post(
    "",
    response_model=CreateRfqResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_rfq(
    request: CreateSwapRfqRequest,
    use_case: CreateRfqUseCaseDep,
) -> CreateRfqResponse:
    result = await use_case.execute(
        notional=request.notional,
        effective_date=request.effective_date,
        maturity_date=request.maturity_date,
        fixed_leg=request.fixed_leg,
        currency=request.currency,
    )

    return CreateRfqResponse(
        rfq_id=result,
        revision=1,
    )

@router.post(
    "/{rfq_id}:stop-pricing",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def stp_rfq_pricing(
    rfq_id: str,
    use_case: StopRfqPricingUseCaseDep,
) -> None:
    await use_case.execute(rfq_id=rfq_id)


@router.patch(
    "/{rfq_id}",
    response_model=ChangeRfqResponse,
)
async def change_rfq(
    rfq_id: str,
    request: ChangeSwapRfqRequest,
    use_case: ChangeRfqUseCaseDep,
) -> ChangeRfqResponse:
    result = await use_case.execute(
        rfq_id=rfq_id,
        notional=request.notional,
        effective_date=request.effective_date,
        maturity_date=request.maturity_date,
        fixed_leg=request.fixed_leg,
        currency=request.currency,
    )

    return ChangeRfqResponse(
        rfq_id=rfq_id,
        revision=result,
    )
