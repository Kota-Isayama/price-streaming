from typing import Any, Self

from request_for_quote.bootstrap.settings.base import (
    YamlSettings,
    yaml_settings_config,
)
from request_for_quote.bootstrap.settings.database import (
    DatabaseSettings,
)
from request_for_quote.bootstrap.settings.realtime_updates import (
    RedisNotificationUpdatesSubscriberSettings,
    RedisPricingUpdatesSubscriberSettings,
)

class ApiSettings(YamlSettings):
    database: DatabaseSettings

    notification_updates: (
        RedisNotificationUpdatesSubscriberSettings
    )

    pricing_updates: (
        RedisPricingUpdatesSubscriberSettings
    )

    model_config = yaml_settings_config(
        "config/api.yaml"
    )

    @classmethod
    def load(cls) -> Self:
        empty: dict[str, Any] = {}
        return cls(**empty)
