from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class MarketDataId:
    value: str


@dataclass(frozen=True)
class MarketDataValue:
    value: Decimal


@dataclass(frozen=True)
class MarketSnapshot:
    values: dict[MarketDataId, MarketDataValue]

    def get(self, market_data_id: MarketDataId) -> MarketDataValue:
        try:
            return self.values[market_data_id]
        except KeyError as exc:
            raise MarketDataNotFound(market_data_id) from exc


class MarketDataNotFound(Exception):
    def __init__(self, market_data_id: MarketDataId):
        super().__init__(
            f"Market data not found: {market_data_id.value}"
        )
        self.market_data_id = market_data_id


class MarketState:
    def __init__(self) -> None:
        self._values: dict[MarketDataId, MarketDataValue] = {}

    def update(
        self,
        market_data_id: MarketDataId,
        value: MarketDataValue,
    ) -> None:
        self._values[market_data_id] = value

    def snapshot(
        self,
        market_data_ids: set[MarketDataId],
    ) -> MarketSnapshot:
        values = {
            market_data_id: self._values[market_data_id]
            for market_data_id in market_data_ids
            if market_data_id in self._values
        }

        return MarketSnapshot(values=values)

    def contains_all(
            self,
            market_data_ids: set[MarketDataId],
        ) -> bool:
            return all(
                market_data_id in self._values
                for market_data_id in market_data_ids
            )