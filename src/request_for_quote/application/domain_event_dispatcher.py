from typing import Any, Awaitable, Callable

from request_for_quote.domain.shared.domain_event import DomainEvent


DomainEventHandler = Callable[..., Awaitable[None]]


class DomainEventDispatcher:
    def __init__(
        self,
        handlers: dict[type[DomainEvent], list[DomainEventHandler]],
    ) -> None:
        self._handlers = handlers

    async def dispatch(
        self,
        event: DomainEvent,
        *,
        uow: Any,  # TODO remove any...
    ) -> None:
        handlers = self._handlers.get(type(event))

        if handlers is None:
            print(
                "[DOMAIN EVENT] "
                f"No handler is registered for {type(event)}."
            )
            return

        for handler in handlers:
            await handler(event, uow=uow)
