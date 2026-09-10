"""複数のInbound Adapterを起動するProcess Host/Conposition Root

入力は現在のところ以下の２系統が考えられる
┌──────────────────────────────────┐
│                                  │
│ ZeroMQ Market Data Consumer      │
│      ↓                           │
│ HandleMarketDataUpdateUseCase    │
│                                  │
│ RFQ Lifecycle Consumer           │
│      ↓                           │
│ ActivateRfqPricingUseCase        │
│ StopRfqPricingUseCase            │
│                                  │
└──────────────────────────────────┘

Market message #1
    ↓
HandleMarketDataUpdateUseCase.execute()

Market message #2
    ↓
HandleMarketDataUpdateUseCase.execute()

RfqActivated message
    ↓
ActivateRfqPricingUseCase.execute()

Market message #3
    ↓
HandleMarketDataUpdateUseCase.execute()
このように、実行時には、MQなどを通じて入力されるイベントに対して一つのUseCaseが実行される。

構造は通常のWebAPIと全く同様である。
                   外部世界

 HTTP ──────→ FastAPI Endpoint ──┐
                                 │
 CLI ───────→ CLI Handler ───────┤
                                 ▼
 ZeroMQ ─────→ Market Handler → Application
                                 ▲
 Kafka ──────→ RFQ Handler ──────┘
"""


import asyncio

from request_for_quote.application.port.pricing_lifecycle_subscriber import IPricingLifecycleSubscriber
from request_for_quote.application.pricing.lifecycle import PricingActivated, PricingChanged, PricingStopped
from request_for_quote.application.pricing.use_case.activate_pricing import ActivatePricingSessionUseCase
from request_for_quote.application.pricing.use_case.change_pricing import ChangePricingUseCase
from request_for_quote.application.pricing.use_case.handle_market_data_update import HandleMarketDataUpdateUseCase
from request_for_quote.application.pricing.use_case.stop_pricing import StopPricingUseCase
from request_for_quote.domain.market.market import MarketState
from request_for_quote.domain.pricing.request import SwapPricingRequest
from request_for_quote.domain.pricing.swap_pricer import JPY_OIS, SwapPricer
from request_for_quote.domain.product.shared import Currency
from request_for_quote.domain.product.swap import InterestRateSwap, PayReceive
from request_for_quote.infrastructure.application.adapter.market_data_subscriber.zeromq_market_data_subscriber import ZeroMqMarketDataSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_lifecycle_subscriber.kafka_pricing_lifecycle_subscriber import KafkaPricingLifecycleSubscriber
from request_for_quote.infrastructure.application.adapter.pricing_session_store.in_memory_pricing_session_store import InMemoryPricingSessionStore


async def consume_market_data(
    subscriber: ZeroMqMarketDataSubscriber,
    use_case: HandleMarketDataUpdateUseCase,
) -> None:
    async for update in subscriber.subscribe():
        await use_case.execute(
            market_data_id=update.market_data_id,
            value=update.value,
        )


async def consume_pricing_request_lifecycle(
    subscriber: IPricingLifecycleSubscriber,
    activate_use_case: ActivatePricingSessionUseCase,
    change_use_case: ChangePricingUseCase,
    stop_use_case: StopPricingUseCase,
) -> None:
    async for event in subscriber.subscribe():

        match event:
            case PricingActivated():
                await activate_use_case.execute(
                    ...,
                )

            case PricingChanged():
                change_use_case.execute(
                    request_id=event.request_id,
                    revision=event.revision,
                )

            case PricingStopped():
                await stop_use_case.execute(
                    request_id=event.request_id,
                )

        # UseCaseが例外なく完了してからack
        subscriber.ack()


async def main() -> None:
    #
    # Shared runtime state
    #
    market_state = MarketState()

    session_store = (
        InMemoryPricingSessionStore()
    )

    #
    # Domain services
    #
    pricer = SwapPricer()

    #
    # Application UseCases
    #
    activate_rfq_pricing = (
        ActivatePricingSessionUseCase(
            session_store=session_store,
            market_state=market_state,
            pricer=pricer,
        )
    )

    change_pricing = (
        ChangePricingUseCase(
            session_store=session_store,
            market_state=market_state,
            pricer=pricer,
        )
    )

    handle_market_data_update = (
        HandleMarketDataUpdateUseCase(
            session_store=session_store,
            market_state=market_state,
            pricer=pricer,
        )
    )

    stop_rfq_pricing = (
        StopPricingUseCase(
            session_store=session_store,
        )
    )

    #
    # Infrastructure Adapter
    #
    market_data_subscriber = (
        ZeroMqMarketDataSubscriber(
            endpoint="tcp://127.0.0.1:5555",
            topics={"JPY-OIS"},
        )
    )

    pricing_lifecycle_subscriber = KafkaPricingLifecycleSubscriber(
        topic="pricing-lifecycle",
        bootstrap_servers="localhost:9092",
        group_id="pricing-workers",
    )

    async with asyncio.TaskGroup() as task_group:
        task_group.create_task(
            consume_market_data(
                subscriber=market_data_subscriber,
                use_case=handle_market_data_update,
            )
        )

        task_group.create_task(
            consume_pricing_request_lifecycle(
                subscriber=pricing_lifecycle_subscriber,
                activate_use_case=activate_rfq_pricing,
                change_use_case=change_pricing,
                stop_use_case=stop_rfq_pricing,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())

    