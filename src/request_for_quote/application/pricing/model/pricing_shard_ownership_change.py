from dataclasses import dataclass
from typing import TypeAlias

from request_for_quote.application.pricing.model.pricing_shard import (
    PricingShardId,
)


@dataclass(frozen=True, slots=True)
class PricingShardOwnershipTransitionId:
    """
    Ownership変更1回分の識別子。

    Kafka callback側が、

        「Application側でこのrelease/restoreが終わるまで待つ」

    ために使う。
    """

    value: str


@dataclass(frozen=True, slots=True)
class PricingShardAcquired:
    """
    このWorkerがshardのownerになった。
    """

    transition_id: PricingShardOwnershipTransitionId
    shard_id: PricingShardId


@dataclass(frozen=True, slots=True)
class PricingShardReleased:
    """
    このWorkerがshardのownerではなくなる。
    """

    transition_id: PricingShardOwnershipTransitionId
    shard_id: PricingShardId


PricingShardOwnershipChange: TypeAlias = (
    PricingShardAcquired
    | PricingShardReleased
)