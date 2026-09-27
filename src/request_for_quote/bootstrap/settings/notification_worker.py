from typing import Any, Self

from request_for_quote.bootstrap.settings.database import DatabaseSettings
from request_for_quote.bootstrap.settings.base import YamlSettings, yaml_settings_config
from request_for_quote.bootstrap.settings.realtime_updates import RedisNotificationUpdatesPublisherSettings
from request_for_quote.bootstrap.settings.rfq_events import RfqEventsSubscriberSettings


class NotificationWorkerSettings(YamlSettings):
    database: DatabaseSettings

    rfq_events: RfqEventsSubscriberSettings

    notification_updates: RedisNotificationUpdatesPublisherSettings

    model_config = yaml_settings_config("config/notifiction-worker.yaml")

    @classmethod
    def load(cls) -> Self:
        empty: dict[str, Any] = {}
        return cls(**empty)
    