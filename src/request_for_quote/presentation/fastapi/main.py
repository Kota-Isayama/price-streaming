# presentation/fastapi/main.py

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from request_for_quote.infrastructure.postgres.base import Base
from request_for_quote.infrastructure.postgres.base import (
    create_engine,
    create_session_maker,
)
from request_for_quote.presentation.fastapi.container import (
    ApiContainer,
)
from request_for_quote.presentation.fastapi.routers.rfq import (
    router as rfq_router,
)
from request_for_quote.presentation.fastapi.routers.pricing import (
    router as pricing_router,
)
from request_for_quote.presentation.fastapi.routers.notification import (
    router as notification_router,
)

DATABASE_URL = (
    "postgresql+asyncpg://"
    "postgres:postgres@localhost:5432/request_for_quote"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = create_engine(DATABASE_URL)

    session_maker = create_session_maker(engine)

    # Alembicを使わないので開発中はここで作る
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all
        )

    app.state.container = ApiContainer(
        session_maker=session_maker,
    )

    try:
        yield
    finally:
        await engine.dispose()


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
