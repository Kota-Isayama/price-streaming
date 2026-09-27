from typing import Any, Self

from request_for_quote.bootstrap.settings.database import DatabaseSettings
from request_for_quote.bootstrap.settings.base import YamlSettings, yaml_settings_config
from request_for_quote.bootstrap.settings.rfq_events import RfqEventsPublisherSettings


class OutboxWorkerSettings(YamlSettings):
    database: DatabaseSettings

    rfq_events: RfqEventsPublisherSettings

    model_config = yaml_settings_config(
        "config/outbox-worker.yaml"
    )

    @classmethod
    def load(cls) -> Self:
        empty: dict[str, Any] = {}
        return cls(**empty)