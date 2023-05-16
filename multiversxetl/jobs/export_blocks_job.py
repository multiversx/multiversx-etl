from typing import Any, Dict, Iterable, Protocol, cast

from blockchainetl_common.executors.batch_work_executor import \
    BatchWorkExecutor
from blockchainetl_common.jobs.base_job import BaseJob
from blockchainetl_common.utils import validate_range  # type: ignore

from multiversxetl.domain.hyperblock import MultiversXHyperblock
from multiversxetl.mappers import (MultiversXHyperblockMapper,
                                   MultiversXTransactionMapper)


class IItemExporter(Protocol):
    def open(self): ...
    def export_item(self, item: Any): ...
    def close(self): ...


class IBatchWorkExecutor(Protocol):
    def execute(self, items: Any, callback: Any, total_items: int): ...
    def shutdown(self): ...


class INetworkProvider(Protocol):
    def get_hyperblocks_by_nonces(self, nonces: Iterable[int]) -> Iterable[Dict[str, Any]]: ...


class ExportBlocksJob(BaseJob):
    def __init__(
            self,
            provider: Any,
            start_block: int,
            end_block: int,
            batch_size: int,
            max_workers: int,
            item_exporter: IItemExporter,
            export_blocks: bool = True,
            export_transactions: bool = True):
        validate_range(start_block, end_block)

        self.provider = provider
        self.start_block = start_block
        self.end_block = end_block

        self.batch_work_executor: IBatchWorkExecutor = cast(IBatchWorkExecutor, BatchWorkExecutor(batch_size, max_workers))
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions

        self.hyperblock_mapper = MultiversXHyperblockMapper()
        self.transaction_mapper = MultiversXTransactionMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, hyperblock_nonces: Iterable[int]):
        items = self.provider.get_hyperblocks_by_nonces(hyperblock_nonces)
        hyperblocks = [self.hyperblock_mapper.from_json_dict(item) for item in items]

        for hyperblock in hyperblocks:
            self._export_block(hyperblock)

    def _export_block(self, block: MultiversXHyperblock):
        if self.export_blocks:
            json_dict = self.hyperblock_mapper.to_json_dict(block)
            self.item_exporter.export_item(json_dict)

        if self.export_transactions:
            for transaction in block.transactions:
                json_dict = self.transaction_mapper.to_json_dict(transaction)
                self.item_exporter.export_item(json_dict)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
