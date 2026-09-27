import abc

from request_for_quote.application.pricing.model.pricing_shard import (
    PricingShardId,
)


class IPricingShardResolver(abc.ABC):
    """
    Pricing SessionがどのShardに属するかを解決する。

    restore/releaseするときに使う。

    重要:
    Brokerへpublishするときのroutingルールと
    必ず一致している必要がある。
    """

    @abc.abstractmethod
    def resolve(
        self,
        request_id: str,
    ) -> PricingShardId:
        raise NotImplementedError
    