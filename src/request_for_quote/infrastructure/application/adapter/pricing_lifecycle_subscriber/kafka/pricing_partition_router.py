from aiokafka.partitioner import DefaultPartitioner

class KafkaPricingPartitionRouter:
    def __init__(self) -> None:
        self._partitioner = DefaultPartitioner()

    def partition_for(
        self,
        request_id: str,
        partitions: set[int],
    ) -> int:
        if not partitions:
            raise ValueError("partitions must not be empty")

        all_partitions = sorted(partitions)

        return self._partitioner(
            request_id.encode(),
            all_partitions,
            all_partitions,
        )
    