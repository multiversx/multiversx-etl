from typing import Any, Dict

from multiversxetl.domain import MultiversXTransaction


class MultiversXTransactionMapper:
    def __init__(self):
        pass

    def from_json_dict(self, json_dict: Dict[str, Any]) -> MultiversXTransaction:
        transaction = MultiversXTransaction()

        transaction.hash = json_dict.get("hash", "")
        transaction.previous_transaction_hash = json_dict.get("previous_transaction_hash", "")
        transaction.original_transaction_hash = json_dict.get("original_transaction_hash", "")

        transaction.type = json_dict.get("type", "")
        transaction.processing_type_on_source = json_dict.get("processing_type_on_source", "")
        transaction.processing_type_on_destination = json_dict.get("processing_type_on_destination", "")

        transaction.nonce = json_dict.get("nonce", 0)
        transaction.round = json_dict.get("round", 0)
        transaction.epoch = json_dict.get("epoch", 0)
        transaction.timestamp = json_dict.get("timestamp", 0)

        transaction.sender = json_dict.get("sender", "")
        transaction.receiver = json_dict.get("receiver", "")
        transaction.value = json_dict.get("value", 0)
        transaction.gas_limit = json_dict.get("gas_limit", 0)
        transaction.gas_price = json_dict.get("gas_price", 0)
        transaction.data = json_dict.get("data", "")
        transaction.version = json_dict.get("version", 0)
        transaction.options = json_dict.get("options", "")
        transaction.chain_id = json_dict.get("chain_id", 0)
        transaction.signature = json_dict.get("signature", "")

        transaction.status = json_dict.get("status", "")
        transaction.initially_paid_fee = json_dict.get("initially_paid_fee", 0)

        transaction.source_shard = json_dict.get("source_shard", 0)
        transaction.destination_shard = json_dict.get("destination_shard", 0)
        transaction.miniblock_type = json_dict.get("miniblock_type", "")
        transaction.miniblock_hash = json_dict.get("miniblock_hash", "")

        transaction.operation = json_dict.get("operation", "")
        transaction.function = json_dict.get("function", "")
        transaction.call_type = json_dict.get("call_type", "")

        return transaction

    def to_json_dict(self, transaction: MultiversXTransaction) -> Dict[str, Any]:
        return {
            "hash": transaction.hash,
            "previous_transaction_hash": transaction.previous_transaction_hash,
            "original_transaction_hash": transaction.original_transaction_hash,

            "type": transaction.type,
            "processing_type_on_source": transaction.processing_type_on_source,
            "processing_type_on_destination": transaction.processing_type_on_destination,

            "nonce": transaction.nonce,
            "round": transaction.round,
            "epoch": transaction.epoch,
            "timestamp": transaction.timestamp,

            "sender": transaction.sender,
            "receiver": transaction.receiver,
            "value": transaction.value,
            "gas_limit": transaction.gas_limit,
            "gas_price": transaction.gas_price,
            "data": transaction.data,
            "version": transaction.version,
            "options": transaction.options,
            "chain_id": transaction.chain_id,
            "signature": transaction.signature,

            "status": transaction.status,
            "initially_paid_fee": transaction.initially_paid_fee,

            "source_shard": transaction.source_shard,
            "destination_shard": transaction.destination_shard,
            "miniblock_type": transaction.miniblock_type,
            "miniblock_hash": transaction.miniblock_hash,

            "operation": transaction.operation,
            "function": transaction.function,
            "call_type": transaction.call_type
        }
