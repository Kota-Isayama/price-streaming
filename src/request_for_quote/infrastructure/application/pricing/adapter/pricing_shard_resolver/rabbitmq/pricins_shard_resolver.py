import hashlib

from request_for_quote.application.pricing.model.pricing_shard import PricingShardId
from request_for_quote.application.pricing.port.pricing_shard_resolver import IPricingShardResolver


class StablePricingShardResolver(IPricingShardResolver):
    """
    request_id -> PricingShardIdを決定する。
    
    Publisher側とPricing Worker側で必ず同じ実装を使う。

    Python build-in hash()は使わない。プロセスごとに結果が変わり得るため。
    """
    def __init__(self, shard_count: int) -> None:
        if shard_count <= 0:
            raise ValueError(
                "shard_count must be positive"
            )
        
        self._shard_count = shard_count

    def resolve(self, request_id: str) -> PricingShardId:
        digest = hashlib.sha256(
            request_id.encode("utf-8")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=True,
        )

        return PricingShardId(
            value=value % self._shard_count
        )
    