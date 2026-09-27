# presentation/fastapi/main.py

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from request_for_quote.bootstrap.processes.api import bootstrap_api
from request_for_quote.bootstrap.settings.api import ApiSettings

from request_for_quote.presentation.fastapi.routers.rfq import (
    router as rfq_router,
)
from request_for_quote.presentation.fastapi.routers.pricing import (
    router as pricing_router,
)
from request_for_quote.presentation.fastapi.routers.notification import (
    router as notification_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = ApiSettings.load()

    async with bootstrap_api(settings) as container:
        app.state.container = container
        yield


app = FastAPI(
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rfq_router)
app.include_router(pricing_router)
app.include_router(notification_router)
