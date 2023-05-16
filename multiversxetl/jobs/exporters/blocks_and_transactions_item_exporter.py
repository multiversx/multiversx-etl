from typing import Any

from blockchainetl_common.jobs.exporters.composite_item_exporter import \
    CompositeItemExporter

BLOCK_FIELDS_TO_EXPORT = [
    "hash",
    "previous_block_hash",
    "round",
    "nonce",
    "epoch",
    "timestamp",
    "num_transactions"
]

TRANSACTION_FIELDS_TO_EXPORT = [
    "hash",
    "previous_transaction_hash",
    "original_transaction_hash",
    "type",
    "processing_type_on_source",
    "processing_type_on_destination",
    "nonce",
    "round",
    "epoch",
    "timestamp",
    "sender",
    "receiver",
    "value",
    "gas_limit",
    "gas_price",
    "data",
    "version",
    "options",
    "chain_id",
    "signature",
    "status",
    "initially_paid_fee",
    "source_shard",
    "destination_shard",
    "miniblock_type",
    "miniblock_hash",
    "operation",
    "function",
    "call_type"
]


def blocks_and_transactions_item_exporter(blocks_output: Any = None, transactions_output: Any = None) -> CompositeItemExporter:
    return CompositeItemExporter(
        filename_mapping={
            "block": blocks_output,
            "transaction": transactions_output,
        },
        field_mapping={
            "block": BLOCK_FIELDS_TO_EXPORT,
            "transaction": TRANSACTION_FIELDS_TO_EXPORT,
        }
    )
